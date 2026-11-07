#define USING_LOG_PREFIX SERVER
#include "ob_ai_column_task_worker.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_smart_var.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace observer {

static const char *safe_memmem(const char *haystack, int64_t haystack_len,
                               const char *needle, int64_t needle_len)
{
  if (haystack_len < needle_len || needle_len <= 0) return nullptr;
  for (int64_t i = 0; i <= haystack_len - needle_len; ++i) {
    if (0 == memcmp(haystack + i, needle, needle_len)) {
      return haystack + i;
    }
  }
  return nullptr;
}

static const char *safe_strchr(const char *haystack, int64_t haystack_len, char c)
{
  for (int64_t i = 0; i < haystack_len; ++i) {
    if (haystack[i] == c) return haystack + i;
  }
  return nullptr;
}

static int64_t json_escape_for_http(const char *src, int64_t src_len,
                                    char *dst, int64_t dst_size)
{
  int64_t pos = 0;
  for (int64_t i = 0; i < src_len && pos < dst_size - 6; ++i) {
    unsigned char c = (unsigned char)src[i];
    switch (c) {
      case '"':  dst[pos++] = '\\'; dst[pos++] = '"'; break;
      case '\\': dst[pos++] = '\\'; dst[pos++] = '\\'; break;
      case '\n': dst[pos++] = '\\'; dst[pos++] = 'n'; break;
      case '\r': dst[pos++] = '\\'; dst[pos++] = 'r'; break;
      case '\t': dst[pos++] = '\\'; dst[pos++] = 't'; break;
      default:
        if (c < 0x20) {
          pos += snprintf(dst + pos, dst_size - pos, "\\u%04x", c);
        } else {
          dst[pos++] = (char)c;
        }
        break;
    }
  }
  return pos;
}

int ObAIColumnTaskWorker::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "sql_proxy is null", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    inited_ = true;
    OB_LOG(INFO, "ai column task worker inited");
  }
  return ret;
}

int ObAIColumnTaskWorker::claim_tasks(int64_t &affected)
{
  int ret = OB_SUCCESS;
  affected = 0;
  const char *claim_sql =
    "UPDATE oceanbase.__all_ai_column_task SET state=2 "
    "WHERE state=1 AND eligible_ts <= now(6) LIMIT 10";
  if (OB_FAIL(sql_proxy_->write(claim_sql, affected))) {
    OB_LOG(WARN, "claim ai tasks failed", K(ret));
  }
  return ret;
}

int ObAIColumnTaskWorker::process_tasks()
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    const char *select_sql =
      "SELECT tenant_id, table_id, column_id, rowkey_predicate, request_id, retry_count "
      "FROM oceanbase.__all_ai_column_task WHERE state=2";
    if (OB_FAIL(sql_proxy_->read(res, select_sql))) {
      OB_LOG(WARN, "select ai tasks failed", K(ret));
    } else if (OB_ISNULL(res.get_result())) {
    } else {
      while (OB_SUCC(res.get_result()->next())) {
        int64_t tenant_id = 0, table_id = 0, column_id = 0;
        int64_t request_id = 0, retry_count = 0;
        ObString rowkey_predicate;
        if (OB_FAIL(res.get_result()->get_int(static_cast<int64_t>(0), tenant_id))
            || OB_FAIL(res.get_result()->get_int(static_cast<int64_t>(1), table_id))
            || OB_FAIL(res.get_result()->get_int(static_cast<int64_t>(2), column_id))
            || OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(3), rowkey_predicate))
            || OB_FAIL(res.get_result()->get_int(static_cast<int64_t>(4), request_id))
            || OB_FAIL(res.get_result()->get_int(static_cast<int64_t>(5), retry_count))) {
          OB_LOG(WARN, "read task cols failed", K(ret));
        } else {
          int tmp_ret = process_one_task(tenant_id, table_id, column_id,
                                         rowkey_predicate, request_id, retry_count);
          if (OB_SUCCESS != tmp_ret) {
            OB_LOG(WARN, "process one task failed", K(tmp_ret),
                   K(tenant_id), K(table_id), K(column_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObAIColumnTaskWorker::process_one_task(
    int64_t tenant_id, int64_t table_id, int64_t column_id,
    const ObString &rowkey_predicate, int64_t request_id, int64_t retry_count)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc("ai_worker");
  char sql_buf[16384];

  // 0. 查表名 + 库名（动态）
  ObString table_name, database_name;
  {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT t.table_name, d.database_name FROM oceanbase.__all_table t "
               "JOIN oceanbase.__all_database d ON t.database_id = d.database_id "
               "WHERE t.table_id=%ld", table_id);
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "query table_name failed", K(ret), K(table_id));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_table, tmp_db;
        res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_table);
        res.get_result()->get_varchar(static_cast<int64_t>(1), tmp_db);
        if (OB_FAIL(ob_write_string(alloc, tmp_table, table_name))) {
          OB_LOG(WARN, "copy table_name failed", K(ret));
        } else if (OB_FAIL(ob_write_string(alloc, tmp_db, database_name))) {
          OB_LOG(WARN, "copy database_name failed", K(ret));
        }
      }
    }
  }

  // 1. 查 __all_ai_column 拿 model_name + normalized_expr + max_retries
  ObString model_name, normalized_expr;
  int64_t max_retries = 2;
  {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT ai_model_name, normalized_expr, max_retries "
               "FROM oceanbase.__all_ai_column WHERE table_id=%ld AND column_id=%ld",
               table_id, column_id);
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "query ai_column meta failed", K(ret), K(sql_buf));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_model, tmp_expr;
        if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_model))
            || OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(1), tmp_expr))
            || OB_FAIL(res.get_result()->get_int(static_cast<int64_t>(2), max_retries))) {
          OB_LOG(WARN, "read ai_column meta cols failed", K(ret));
        } else {
          ob_write_string(alloc, tmp_model, model_name);
          ob_write_string(alloc, tmp_expr, normalized_expr);
        }
      }
    }
  }

  // 2. 查 docs 拿依赖列 content
  ObString content;
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT content FROM %.*s.%.*s WHERE %.*s",
               database_name.length(), database_name.ptr(),
               table_name.length(), table_name.ptr(),
               rowkey_predicate.length(), rowkey_predicate.ptr());
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "query docs content failed", K(ret), K(sql_buf));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_content;
        res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_content);
        ob_write_string(alloc, tmp_content, content);
      }
    }
  }

  // 3. 拆分提示词和依赖表达式
  ObString prompt;
  ObString dep_expr;
  ObString user_text;
  if (OB_SUCC(ret)) {
    const char *sep = safe_memmem(normalized_expr.ptr(), normalized_expr.length(), "|||", 3);
    if (NULL == sep) {
      prompt = normalized_expr;
    } else {
      const int64_t prompt_len = sep - normalized_expr.ptr();
      prompt.assign_ptr(normalized_expr.ptr(), static_cast<int32_t>(prompt_len));
      dep_expr.assign_ptr(sep + 3, static_cast<int32_t>(normalized_expr.length() - prompt_len - 3));
    }
    user_text = content;
  }
  if (OB_SUCC(ret) && !dep_expr.empty()) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT %.*s FROM %.*s.%.*s WHERE %.*s",
               dep_expr.length(), dep_expr.ptr(),
               database_name.length(), database_name.ptr(),
               table_name.length(), table_name.ptr(),
               rowkey_predicate.length(), rowkey_predicate.ptr());
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "query ai column dependency expression failed", K(ret), K(sql_buf));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_user_text;
        if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_user_text))) {
        } else if (OB_FAIL(ob_write_string(alloc, tmp_user_text, user_text))) {
        }
      }
    }
  }

  // 4. 迟到保护：写回前再查 request_id
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT request_id FROM oceanbase.__all_ai_column_task "
               "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
               "AND rowkey_predicate='%.*s'",
               tenant_id, table_id, column_id,
               rowkey_predicate.length(), rowkey_predicate.ptr());
      int64_t cur_req = -1;
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "query cur request_id failed", K(ret));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
        OB_LOG(INFO, "task gone, skip", K(ret));
      } else {
        res.get_result()->get_int(static_cast<int64_t>(0), cur_req);
        if (cur_req != request_id) {
          OB_LOG(INFO, "late result discarded", K(cur_req), K(request_id));
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
    }
  }

  // 5. 调真实模型（Mock HTTP）
  ObString ai_result;
  if (OB_SUCC(ret)) {
    ObString ep_url, ep_req_model;
    {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        snprintf(sql_buf, sizeof(sql_buf),
                 "SELECT url, request_model_name "
                 "FROM oceanbase.__all_ai_model_endpoint WHERE ai_model_name='%.*s'",
                 model_name.length(), model_name.ptr());
        if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
          OB_LOG(WARN, "query endpoint failed", K(ret));
        } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
          ret = OB_ENTRY_NOT_EXIST;
          OB_LOG(WARN, "endpoint not found", K(ret), K(model_name));
        } else {
          ObString tmp_url, tmp_model;
          res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_url);
          res.get_result()->get_varchar(static_cast<int64_t>(1), tmp_model);
          ob_write_string(alloc, tmp_url, ep_url);
          ob_write_string(alloc, tmp_model, ep_req_model);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObAIFuncClient client;
      ObArray<ObString> headers;
      headers.push_back(ObString::make_string("Content-Type"));
      headers.push_back(ObString::make_string("application/json"));
      headers.push_back(ObString::make_string("Authorization"));
      headers.push_back(ObString::make_string("Bearer p0-key"));
      if (OB_FAIL(client.init(alloc, ep_url, headers))) {
        OB_LOG(WARN, "client init failed", K(ret));
      } else {
        // 判断端点类型：embeddings 还是 chat/completions
        bool is_embedding_ep = (nullptr != safe_memmem(ep_url.ptr(), ep_url.length(), "/embeddings", 11));
        char model_esc[512];
        int64_t model_esc_len = json_escape_for_http(ep_req_model.ptr(), ep_req_model.length(),
                                                     model_esc, sizeof(model_esc));
        char body_buf[16384];
        int64_t body_len = 0;
        if (is_embedding_ep) {
          // embeddings: {"model":"...","input":"<content>"}
          char content_esc[8192];
          int64_t content_esc_len = json_escape_for_http(content.ptr(), content.length(),
                                                         content_esc, sizeof(content_esc));
          body_len = snprintf(body_buf, sizeof(body_buf),
              "{\"model\":\"%.*s\",\"input\":\"%.*s\"}",
              (int)model_esc_len, model_esc,
              (int)content_esc_len, content_esc);
        } else {
          // chat/completions: system carries the normalized prompt, user carries evaluated data.
          char prompt_esc[8192];
          char user_esc[8192];
          int64_t prompt_esc_len = json_escape_for_http(prompt.ptr(), prompt.length(),
                                                        prompt_esc, sizeof(prompt_esc));
          int64_t user_esc_len = json_escape_for_http(user_text.ptr(), user_text.length(),
                                                      user_esc, sizeof(user_esc));
          body_len = snprintf(body_buf, sizeof(body_buf),
              "{\"model\":\"%.*s\",\"messages\":[{\"role\":\"system\",\"content\":\"%.*s\"},"
              "{\"role\":\"user\",\"content\":\"%.*s\"}]}",
              (int)model_esc_len, model_esc,
              (int)prompt_esc_len, prompt_esc,
              (int)user_esc_len, user_esc);
        }
        if (body_len <= 0 || body_len >= (int64_t)sizeof(body_buf)) {
          ret = OB_SIZE_OVERFLOW;
          OB_LOG(WARN, "body too long", K(ret), K(body_len));
        }
        ObString body_str(body_len, body_buf);
        ObJsonObject *req_obj = nullptr;
        ObJsonObject *resp_obj = nullptr;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(alloc, body_str, req_obj))) {
          OB_LOG(WARN, "parse request json failed", K(ret));
        } else if (OB_FAIL(client.send_post(alloc, ep_url, headers, req_obj, resp_obj))) {
          OB_LOG(WARN, "send_post failed", K(ret));
        } else if (OB_ISNULL(resp_obj)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ObString resp_str;
          if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(alloc, resp_obj, resp_str))) {
          } else if (is_embedding_ep) {
            // 解析 data[0].embedding 数组 -> [v1,v2,v3,v4]
            const char *emb_pos = safe_memmem(resp_str.ptr(), resp_str.length(), "\"embedding\"", 11);
            const char *lb = nullptr;
            const char *rb = nullptr;
            if (nullptr != emb_pos) {
              int64_t off = emb_pos - resp_str.ptr();
              lb = safe_strchr(resp_str.ptr() + off, resp_str.length() - off, '[');
              if (nullptr != lb) {
                int64_t lb_off = lb - resp_str.ptr();
                rb = safe_strchr(resp_str.ptr() + lb_off, resp_str.length() - lb_off, ']');
              }
            }
            if (nullptr == lb || nullptr == rb) {
              ret = OB_ERR_UNEXPECTED;
              OB_LOG(WARN, "parse embedding array failed", K(ret), K(resp_str));
            } else {
              int64_t vec_len = rb - lb + 1;
              char vec_buf[512];
              if (vec_len >= (int64_t)sizeof(vec_buf)) {
                ret = OB_SIZE_OVERFLOW;
              } else {
                memcpy(vec_buf, lb, vec_len);
                vec_buf[vec_len] = 0;
                ob_write_string(alloc, ObString(vec_len, vec_buf), ai_result);
              }
            }
          } else {
            ObIJsonBase *result_base = nullptr;
            ObOpenAIUtils::ObOpenAIComplete openai_parser;
            if (OB_FAIL(openai_parser.parse_output(
                alloc, resp_obj, result_base))) {
            } else if (OB_ISNULL(result_base)
                       || ObJsonNodeType::J_STRING != result_base->json_type()) {
              ret = OB_ERR_UNEXPECTED;
              OB_LOG(WARN, "completion content is not a JSON string", K(ret), K(resp_str));
            } else {
              const ObJsonString *content_str =
                  static_cast<const ObJsonString *>(result_base);
              if (OB_FAIL(ob_write_string(alloc, content_str->get_str(), ai_result))) {
              }
            }
          }
        }
      }
    }
  }

  // 6. 写回真实结果（SQL 转义：\ 和 ' 都双写）
  if (OB_SUCC(ret) && !ai_result.empty()) {
    char sql_escaped[16384];
    int64_t esc_pos = 0;
    for (int64_t i = 0; i < ai_result.length() && esc_pos < (int64_t)sizeof(sql_escaped) - 2; ++i) {
      char c = ai_result.ptr()[i];
      if ('\\' == c) {
        sql_escaped[esc_pos++] = '\\';
        sql_escaped[esc_pos++] = '\\';
      } else if ('\'' == c) {
        sql_escaped[esc_pos++] = '\'';
        sql_escaped[esc_pos++] = '\'';
      } else {
        sql_escaped[esc_pos++] = c;
      }
    }
    sql_escaped[esc_pos] = 0;
    // 按 column_id 查列名
    ObString col_name;
    {
      SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
        snprintf(sql_buf, sizeof(sql_buf),
                 "SELECT column_name FROM oceanbase.__all_column "
                 "WHERE table_id=%ld AND column_id=%ld", table_id, column_id);
        if (OB_SUCC(sql_proxy_->read(res2, sql_buf))
            && OB_NOT_NULL(res2.get_result())
            && OB_SUCCESS == res2.get_result()->next()) {
          ObString tmp_col;
          res2.get_result()->get_varchar(static_cast<int64_t>(0), tmp_col);
          ob_write_string(alloc, tmp_col, col_name);
        }
      }
    }
    if (col_name.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      OB_LOG(WARN, "column_name not found", K(ret), K(table_id), K(column_id));
    } else {
      snprintf(sql_buf, sizeof(sql_buf),
               "UPDATE %.*s.%.*s SET %.*s='%s' WHERE %.*s",
               database_name.length(), database_name.ptr(),
               table_name.length(), table_name.ptr(),
               col_name.length(), col_name.ptr(),
               sql_escaped, rowkey_predicate.length(), rowkey_predicate.ptr());
    }
    int64_t affected = 0;
    if (OB_FAIL(sql_proxy_->write(sql_buf, affected))) {
      OB_LOG(WARN, "writeback failed", K(ret), K(sql_buf));
    } else if (0 == affected) {
      // 行已被 DELETE，放弃本次结果，清理任务
      OB_LOG(INFO, "row gone, discard result", K(table_id), K(column_id));
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  // 7. 成功删任务 / 失败重试
  if (OB_SUCC(ret)) {
    snprintf(sql_buf, sizeof(sql_buf),
             "DELETE FROM oceanbase.__all_ai_column_task "
             "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
             "AND rowkey_predicate='%.*s' AND request_id=%ld",
             tenant_id, table_id, column_id,
             rowkey_predicate.length(), rowkey_predicate.ptr(), request_id);
    int64_t affected = 0;
    sql_proxy_->write(sql_buf, affected);
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    int64_t new_retry = retry_count + 1;
    int64_t new_state = (new_retry > max_retries) ? 3 : 1;
    snprintf(sql_buf, sizeof(sql_buf),
             "UPDATE oceanbase.__all_ai_column_task SET retry_count=%ld, state=%ld "
             "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
             "AND rowkey_predicate='%.*s'",
             new_retry, new_state, tenant_id, table_id, column_id,
             rowkey_predicate.length(), rowkey_predicate.ptr());
    int64_t affected = 0;
    sql_proxy_->write(sql_buf, affected);
    OB_LOG(WARN, "task retry scheduled", K(ret), K(new_retry), K(new_state));
  }

  return ret;
}

void ObAIColumnTaskWorker::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
  } else {
    int64_t affected = 0;
    if (OB_FAIL(claim_tasks(affected))) {
    } else if (affected == 0) {
    } else {
      OB_LOG(INFO, "ai task worker claimed", K(affected));
      if (OB_FAIL(process_tasks())) {
        OB_LOG(WARN, "process ai tasks failed", K(ret));
      }
    }
  }
}

} // namespace observer
} // namespace oceanbase
