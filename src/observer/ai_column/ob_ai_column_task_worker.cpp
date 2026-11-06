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
    "UPDATE ai_column_p0_t1.task_shadow SET state=2 "
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
      "FROM ai_column_p0_t1.task_shadow WHERE state=2";
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
  char sql_buf[2048];

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
               "SELECT content FROM ai_column_p0_t1.docs WHERE %.*s",
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

  // 3. 拼 prompt
  if (OB_SUCC(ret)) {
    int64_t prompt_len = snprintf(sql_buf, sizeof(sql_buf), "%.*s\n%.*s",
                                  normalized_expr.length(), normalized_expr.ptr(),
                                  content.length(), content.ptr());
    if (prompt_len <= 0 || prompt_len >= (int64_t)sizeof(sql_buf)) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "prompt too long", K(ret), K(prompt_len));
    }
  }

  // 4. 迟到保护：写回前再查 request_id
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT request_id FROM ai_column_p0_t1.task_shadow "
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
        ObString body_str;
        body_str = ObString::make_string("{\"model\":\"mock-complete\",\"messages\":[{\"role\":\"user\",\"content\":\"test\"}]}");
        ObJsonObject *req_obj = nullptr;
        ObJsonObject *resp_obj = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(alloc, body_str, req_obj))) {
          OB_LOG(WARN, "parse request json failed", K(ret));
        } else if (OB_FAIL(client.send_post(alloc, ep_url, headers, req_obj, resp_obj))) {
          OB_LOG(WARN, "send_post failed", K(ret));
        } else if (OB_ISNULL(resp_obj)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          // 直接取响应 JSON 的字符串
          ObString resp_str;
          if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(alloc, resp_obj, resp_str))) {
          } else {
            ob_write_string(alloc, resp_str, ai_result);
          }
        }
      }
    }
  }

  // 6. 写回真实结果（单引号用 ASCII 39，避免转义）
  if (OB_SUCC(ret) && !ai_result.empty()) {
    char escaped[8192];
    int64_t esc_pos = 0;
    for (int64_t i = 0; i < ai_result.length() && esc_pos < (int64_t)sizeof(escaped) - 2; ++i) {
      char c = ai_result.ptr()[i];
      escaped[esc_pos++] = c;
      if (39 == c) {
        escaped[esc_pos++] = c;
      }
    }
    escaped[esc_pos] = 0;
    snprintf(sql_buf, sizeof(sql_buf),
             "UPDATE ai_column_p0_t1.docs SET tags='%s' WHERE %.*s",
             escaped, rowkey_predicate.length(), rowkey_predicate.ptr());
    int64_t affected = 0;
    if (OB_FAIL(sql_proxy_->write(sql_buf, affected))) {
      OB_LOG(WARN, "writeback failed", K(ret), K(sql_buf));
    }
  }

  // 7. 成功删任务 / 失败重试
  if (OB_SUCC(ret)) {
    snprintf(sql_buf, sizeof(sql_buf),
             "DELETE FROM ai_column_p0_t1.task_shadow "
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
             "UPDATE ai_column_p0_t1.task_shadow SET retry_count=%ld, state=%ld "
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
