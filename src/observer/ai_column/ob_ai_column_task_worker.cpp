#define USING_LOG_PREFIX SERVER
#include "ob_ai_column_task_worker.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
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

static int safe_string_length(const ObString &str)
{
  return NULL == str.ptr() || str.empty() ? 0 : static_cast<int>(str.length());
}

static const char *safe_string_ptr(const ObString &str)
{
  return NULL == str.ptr() || str.empty() ? "" : str.ptr();
}

struct ObAIColumnTaskSnapshot
{
  ObAIColumnTaskSnapshot()
    : tenant_id_(0), table_id_(0), column_id_(0), request_id_(0),
      retry_count_(0), completed_(false), input_ready_(false),
      published_(false), cleaned_(false), rowkey_predicate_(), user_text_(),
      result_str_(), column_name_(), state_column_name_()
  {}
  int64_t tenant_id_;
  int64_t table_id_;
  int64_t column_id_;
  int64_t request_id_;
  int64_t retry_count_;
  bool completed_;
  bool input_ready_;
  bool published_;
  bool cleaned_;
  ObString rowkey_predicate_;
  ObString user_text_;
  ObString result_str_;
  ObString column_name_;
  ObString state_column_name_;
};

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
    "WHERE state IN (1, 3) AND eligible_ts <= now(6) LIMIT 10";
  if (OB_FAIL(sql_proxy_->write(claim_sql, affected))) {
    OB_LOG(WARN, "claim ai tasks failed", K(ret));
  }
  return ret;
}

static bool same_ai_task_group(const ObAIColumnTaskSnapshot &left,
                               const ObAIColumnTaskSnapshot &right)
{
  return left.tenant_id_ == right.tenant_id_
      && left.table_id_ == right.table_id_
      && left.request_id_ == right.request_id_
      && left.rowkey_predicate_.length() == right.rowkey_predicate_.length()
      && (0 == left.rowkey_predicate_.length()
          || 0 == memcmp(left.rowkey_predicate_.ptr(),
                         right.rowkey_predicate_.ptr(),
                         left.rowkey_predicate_.length()));
}

static int append_sql_string_literal(ObSqlString &sql, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
      const char c = value.ptr()[i];
      if ('\\' == c || '\'' == c) {
        const char escaped[2] = {c, c};
        ret = sql.append(escaped, 2);
      } else {
        ret = sql.append(&c, 1);
      }
    }
    if (OB_SUCC(ret)) {
      ret = sql.append("'");
    }
  }
  return ret;
}

static int publish_group_results(
    ObMySQLTransaction *transaction,
    ObArray<ObAIColumnTaskSnapshot> &tasks,
    const int64_t seed,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObAIColumnTaskSnapshot &first = tasks.at(seed);
  bool group_completed = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    if (same_ai_task_group(first, tasks.at(i)) && !tasks.at(i).completed_) {
      group_completed = false;
    }
  }
  if (!group_completed) {
    // A failed member keeps the whole request group unpublished.
  } else {
    char sql_buf[4096];
    ObString table_name;
    ObString database_name;
    {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        snprintf(sql_buf, sizeof(sql_buf),
                 "SELECT t.table_name, d.database_name FROM oceanbase.__all_table t "
                 "JOIN oceanbase.__all_database d ON t.database_id=d.database_id "
                 "WHERE t.table_id=%ld", first.table_id_);
        if (OB_FAIL(transaction->read(res, sql_buf))) {
          OB_LOG(WARN, "publish group query table failed", K(ret),
                 K(first.table_id_));
        } else if (OB_ISNULL(res.get_result())
                   || OB_SUCCESS != res.get_result()->next()) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          ObString tmp_table;
          ObString tmp_database;
          if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_table))
              || OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(1), tmp_database))) {
          } else if (OB_FAIL(ob_write_string(allocator, tmp_table, table_name))
                     || OB_FAIL(ob_write_string(allocator, tmp_database,
                                                database_name))) {
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObSqlString set_clause;
      bool first_set = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
        ObAIColumnTaskSnapshot &task = tasks.at(i);
        if (!same_ai_task_group(first, task)) {
          continue;
        }
        if (!first_set && OB_FAIL(set_clause.append(", "))) {
        } else if (OB_FAIL(set_clause.append_fmt(
                       "`%.*s`=", task.column_name_.length(),
                       task.column_name_.ptr()))) {
        } else if (OB_FAIL(append_sql_string_literal(
                       set_clause, task.result_str_))) {
        } else if (OB_FAIL(set_clause.append_fmt(
                       ", `%.*s`=0", task.state_column_name_.length(),
                       task.state_column_name_.ptr()))) {
        } else {
          first_set = false;
        }
      }

      if (OB_SUCC(ret) && first_set) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "AI task group has no publishable columns", K(ret),
               K(first.request_id_));
      }

      ObSqlString publish_sql;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(publish_sql.append_fmt(
                     "UPDATE `%.*s`.`%.*s` SET ",
                     database_name.length(), database_name.ptr(),
                     table_name.length(), table_name.ptr()))) {
      } else if (OB_FAIL(publish_sql.append(set_clause.string()))) {
      } else if (OB_FAIL(publish_sql.append_fmt(
                     " WHERE %.*s AND EXISTS ("
                     "SELECT 1 FROM oceanbase.__all_ai_column_task "
                     "WHERE tenant_id=%ld AND table_id=%ld "
                     "AND rowkey_predicate=",
                     first.rowkey_predicate_.length(),
                     first.rowkey_predicate_.ptr(),
                     first.tenant_id_, first.table_id_))) {
      } else if (OB_FAIL(append_sql_string_literal(
                     publish_sql, first.rowkey_predicate_))) {
      } else if (OB_FAIL(publish_sql.append_fmt(
                     " AND request_id=%ld)", first.request_id_))) {
      } else {
        int64_t affected = 0;
        if (OB_FAIL(transaction->write(publish_sql.ptr(), affected))) {
          OB_LOG(WARN, "publish AI task group failed", K(ret),
                 K(publish_sql));
        } else if (0 == affected) {
          OB_LOG(INFO, "AI task group publish discarded as stale",
                 K(first.table_id_), K(first.request_id_));
        } else {
          OB_LOG(INFO, "AI task group published", K(first.table_id_),
                 K(first.request_id_), K(affected));
        }
      }
    }
  }
  return ret;
}

static int cleanup_published_group(
    ObMySQLTransaction *transaction,
    ObArray<ObAIColumnTaskSnapshot> &tasks,
    const int64_t seed)
{
  int ret = OB_SUCCESS;
  ObAIColumnTaskSnapshot &first = tasks.at(seed);
  char cleanup_buf[4096];
  ObSqlString rowkey_literal;
  if (OB_FAIL(append_sql_string_literal(
          rowkey_literal, first.rowkey_predicate_))) {
  } else {
    snprintf(cleanup_buf, sizeof(cleanup_buf),
             "DELETE FROM oceanbase.__all_ai_column_task "
             "WHERE tenant_id=%ld AND table_id=%ld "
             "AND rowkey_predicate=%s AND request_id=%ld",
             first.tenant_id_, first.table_id_, rowkey_literal.ptr(),
             first.request_id_);
    int64_t affected = 0;
    if (OB_FAIL(transaction->write(cleanup_buf, affected))) {
      OB_LOG(WARN, "cleanup published AI task group failed", K(ret),
             K(cleanup_buf));
    } else {
      OB_LOG(INFO, "cleanup published AI task group",
             K(first.table_id_), K(first.request_id_), K(affected));
    }
  }
  return ret;
}

int ObAIColumnTaskWorker::process_tasks()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator task_alloc("ai_task_snap");
  ObArray<ObAIColumnTaskSnapshot> tasks;
  OB_LOG(INFO, "process ai tasks begin");

  // Phase 1: snapshot all claimed tasks before executing model calls or writes.
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    const char *select_sql =
      "SELECT tenant_id, table_id, column_id, rowkey_predicate, request_id, retry_count "
      "FROM oceanbase.__all_ai_column_task WHERE state=2";
    if (OB_FAIL(sql_proxy_->read(res, select_sql))) {
      OB_LOG(WARN, "select ai tasks failed", K(ret));
    } else if (OB_ISNULL(res.get_result())) {
      ret = OB_ERR_NULL_VALUE;
      OB_LOG(WARN, "ai task result is null", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        int next_ret = res.get_result()->next();
        if (OB_ITER_END == next_ret) {
          // EOF is the normal terminal condition for a result-set scan.
          break;
        } else if (OB_SUCCESS != next_ret) {
          ret = next_ret;
          OB_LOG(WARN, "iterate ai task result failed", K(ret), K(tasks.count()));
          break;
        }
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
          ObString rowkey_copy;
          if (rowkey_predicate.empty() || OB_ISNULL(rowkey_predicate.ptr())) {
            ret = OB_INVALID_ARGUMENT;
            OB_LOG(ERROR, "invalid ai task rowkey", K(ret), K(tenant_id),
                   K(table_id), K(column_id), K(request_id));
          } else if (OB_FAIL(ob_write_string(task_alloc, rowkey_predicate, rowkey_copy))) {
            OB_LOG(WARN, "copy ai task rowkey failed", K(ret));
          } else {
            ObAIColumnTaskSnapshot task;
            task.tenant_id_ = tenant_id;
            task.table_id_ = table_id;
            task.column_id_ = column_id;
            task.request_id_ = request_id;
            task.retry_count_ = retry_count;
            task.rowkey_predicate_ = rowkey_copy;
            if (OB_FAIL(tasks.push_back(task))) {
              OB_LOG(WARN, "append ai task snapshot failed", K(ret), K(tasks.count()));
            }
          }
        }
      }
    }
  }

  // Phase 2: freeze every model input before the first model call.
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    ObAIColumnTaskSnapshot &task = tasks.at(i);
    const int prep_ret = prepare_task_input(
        task.table_id_, task.column_id_, task.rowkey_predicate_,
        task_alloc, task.user_text_);
    if (OB_SUCCESS != prep_ret) {
      OB_LOG(WARN, "prepare ai task input failed", K(prep_ret),
             K(task.table_id_), K(task.column_id_), K(task.request_id_));
    } else {
      task.input_ready_ = true;
    }
  }

  // Phase 3: execute all model calls and collect results without publishing.
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    ObAIColumnTaskSnapshot &task = tasks.at(i);
    fprintf(stderr,
            "[FIX] process task idx=%ld/%ld table=%ld col=%ld request=%ld input_ready=%d\n",
            i, tasks.count(), task.table_id_, task.column_id_,
            task.request_id_, task.input_ready_);
    fflush(stderr);
    const int tmp_ret = execute_one_task(
        task.tenant_id_, task.table_id_, task.column_id_,
        task.rowkey_predicate_, task.request_id_, task.retry_count_,
        task.user_text_, task.input_ready_, task_alloc,
        task.result_str_, task.column_name_, task.state_column_name_);
    task.completed_ = (OB_SUCCESS == tmp_ret);
    if (OB_SUCCESS != tmp_ret) {
      OB_LOG(WARN, "execute one task failed", K(tmp_ret),
             K(task.table_id_), K(task.column_id_), K(task.request_id_));
    } else {
      OB_LOG(INFO, "execute one task finished", K(task.table_id_),
             K(task.column_id_), K(task.request_id_));
    }
  }

  // Phase 4: publish results and remove the task group in one transaction.
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    ObAIColumnTaskSnapshot &task = tasks.at(i);
    if (!task.completed_ || task.published_ || task.cleaned_) {
      continue;
    }
    ObMySQLTransaction transaction;
    int group_ret = transaction.start(sql_proxy_);
    if (OB_SUCCESS != group_ret) {
      ret = group_ret;
      OB_LOG(WARN, "start AI task group transaction failed", K(ret),
             K(task.table_id_), K(task.request_id_));
    } else {
      group_ret = publish_group_results(&transaction, tasks, i, task_alloc);
      if (OB_SUCCESS == group_ret) {
        group_ret = cleanup_published_group(&transaction, tasks, i);
      }
      int end_ret = transaction.end(OB_SUCCESS == group_ret);
      if (OB_SUCCESS != group_ret) {
        ret = group_ret;
        OB_LOG(WARN, "publish and cleanup AI task group failed", K(ret),
               K(task.table_id_), K(task.request_id_));
      } else if (OB_SUCCESS != end_ret) {
        ret = end_ret;
        OB_LOG(WARN, "commit AI task group transaction failed", K(ret),
               K(task.table_id_), K(task.request_id_));
      } else {
        for (int64_t j = 0; j < tasks.count(); ++j) {
          if (same_ai_task_group(task, tasks.at(j))) {
            tasks.at(j).published_ = true;
            tasks.at(j).cleaned_ = true;
          }
        }
      }
    }
  }

  OB_LOG(INFO, "process ai tasks finished", K(ret), K(tasks.count()));
  return ret;
}

static FILE *debug_fp = fopen("/tmp/retry_fix.log", "a");

int ObAIColumnTaskWorker::prepare_task_input(
    int64_t table_id, int64_t column_id,
    const ObString &rowkey_predicate, ObIAllocator &allocator,
    ObString &user_text)
{
  int ret = OB_SUCCESS;
  char sql_buf[16384];
  ObString table_name;
  ObString database_name;
  ObString normalized_expr;
  ObString content;

  {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT t.table_name, d.database_name FROM oceanbase.__all_table t "
               "JOIN oceanbase.__all_database d ON t.database_id = d.database_id "
               "WHERE t.table_id=%ld", table_id);
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "prepare task query table failed", K(ret), K(table_id));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_table;
        ObString tmp_database;
        if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_table))
            || OB_FAIL(res.get_result()->get_varchar(1, tmp_database))
            || OB_FAIL(ob_write_string(allocator, tmp_table, table_name))
            || OB_FAIL(ob_write_string(allocator, tmp_database, database_name))) {
          OB_LOG(WARN, "prepare task copy table failed", K(ret), K(table_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT normalized_expr FROM oceanbase.__all_ai_column "
               "WHERE table_id=%ld AND column_id=%ld", table_id, column_id);
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "prepare task query metadata failed", K(ret),
               K(table_id), K(column_id));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_expr;
        if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_expr))
            || OB_FAIL(ob_write_string(allocator, tmp_expr, normalized_expr))) {
          OB_LOG(WARN, "prepare task copy metadata failed", K(ret),
                 K(table_id), K(column_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT content FROM %.*s.%.*s WHERE %.*s",
               database_name.length(), database_name.ptr(),
               table_name.length(), table_name.ptr(),
               rowkey_predicate.length(), rowkey_predicate.ptr());
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "prepare task query content failed", K(ret), K(sql_buf));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_content;
        if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_content))
            || OB_FAIL(ob_write_string(allocator, tmp_content, content))) {
          OB_LOG(WARN, "prepare task copy content failed", K(ret),
                 K(table_id), K(column_id));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, content, user_text))) {
    OB_LOG(WARN, "prepare task copy user text failed", K(ret),
           K(table_id), K(column_id));
  }
  if (OB_SUCC(ret)) {
    const char *sep = safe_memmem(normalized_expr.ptr(), normalized_expr.length(), "|||", 3);
    if (NULL != sep) {
      const int64_t prompt_len = sep - normalized_expr.ptr();
      ObString dep_expr;
      dep_expr.assign_ptr(sep + 3,
                          static_cast<int32_t>(normalized_expr.length() - prompt_len - 3));
      if (!dep_expr.empty()) {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          snprintf(sql_buf, sizeof(sql_buf),
                   "SELECT %.*s FROM %.*s.%.*s WHERE %.*s",
                   dep_expr.length(), dep_expr.ptr(),
                   database_name.length(), database_name.ptr(),
                   table_name.length(), table_name.ptr(),
                   rowkey_predicate.length(), rowkey_predicate.ptr());
          if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
            OB_LOG(WARN, "prepare task evaluate dependency failed",
                   K(ret), K(sql_buf));
          } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
            ret = OB_ENTRY_NOT_EXIST;
          } else {
            ObString tmp_user_text;
            if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_user_text))
                || OB_FAIL(ob_write_string(allocator, tmp_user_text, user_text))) {
              OB_LOG(WARN, "prepare task copy evaluated input failed", K(ret),
                     K(table_id), K(column_id));
            }
          }
        }
      }
    }
  }
  OB_LOG(INFO, "prepare ai task input finished", K(ret), K(table_id),
         K(column_id), K(user_text));
  return ret;
}

int ObAIColumnTaskWorker::execute_one_task(
    int64_t tenant_id, int64_t table_id, int64_t column_id,
    const ObString &rowkey_predicate, int64_t request_id, int64_t retry_count,
    const ObString &prepared_user_text, bool use_prepared_input,
    ObIAllocator &result_allocator, ObString &result_str,
    ObString &column_name, ObString &out_state_column_name)
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

  // 1. 查 __all_ai_column 拿 model_name + normalized_expr + max_retries + state column
  ObString model_name, normalized_expr, state_column_name;
  int64_t max_retries = 2;
  {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      snprintf(sql_buf, sizeof(sql_buf),
               "SELECT ac.ai_model_name, ac.normalized_expr, ac.max_retries, sc.column_name "
               "FROM oceanbase.__all_ai_column ac "
               "JOIN oceanbase.__all_column sc ON sc.table_id=ac.table_id "
               "AND sc.column_id=ac.state_column_id "
               "WHERE ac.table_id=%ld AND ac.column_id=%ld",
               table_id, column_id);
      if (OB_FAIL(sql_proxy_->read(res, sql_buf))) {
        OB_LOG(WARN, "query ai_column meta failed", K(ret), K(sql_buf));
      } else if (OB_ISNULL(res.get_result()) || OB_SUCCESS != res.get_result()->next()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ObString tmp_model, tmp_expr, tmp_state_column;
        if (OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(0), tmp_model))
            || OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(1), tmp_expr))
            || OB_FAIL(res.get_result()->get_int(static_cast<int64_t>(2), max_retries))
            || OB_FAIL(res.get_result()->get_varchar(static_cast<int64_t>(3), tmp_state_column))) {
          OB_LOG(WARN, "read ai_column meta cols failed", K(ret));
        } else if (OB_FAIL(ob_write_string(alloc, tmp_model, model_name))) {
          OB_LOG(WARN, "copy ai model name failed", K(ret));
        } else if (OB_FAIL(ob_write_string(alloc, tmp_expr, normalized_expr))) {
          OB_LOG(WARN, "copy ai normalized expr failed", K(ret));
        } else if (OB_FAIL(ob_write_string(alloc, tmp_state_column, state_column_name))) {
          OB_LOG(WARN, "copy ai column state name failed", K(ret));
        }
      }
    }
  }

  // 2. 查 docs 拿依赖列 content
  ObString content;
  if (OB_SUCC(ret) && !use_prepared_input) {
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
    user_text = use_prepared_input ? prepared_user_text : content;
  }
  if (OB_SUCC(ret) && !use_prepared_input && !dep_expr.empty()) {
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

  // 4. 调真实模型（Mock HTTP）。The result is guarded atomically at writeback.
  ObString ai_result;
  if (OB_SUCC(ret)) {
    fprintf(debug_fp, "[FIX] model call begin table=%ld col=%ld\n", table_id, column_id);
    fflush(stderr);
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
      headers.push_back(ObString::make_string("Content-Type: application/json"));
      headers.push_back(ObString::make_string("Authorization: Bearer p0-key"));
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
          // embeddings: use the same frozen input as chat/completions.
          char content_esc[8192];
          int64_t content_esc_len = json_escape_for_http(user_text.ptr(), user_text.length(),
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
  if (OB_SUCC(ret)) {
    fprintf(debug_fp, "[FIX] model call end ret=%d\n", ret);
    fflush(stderr);
  } else {
    fprintf(debug_fp, "[FIX] model call failed ret=%d\n", ret);
    fflush(stderr);
  }

  // 6. Collect the result for grouped atomic publication.
  if (OB_SUCC(ret)) {
    ObString col_name;
    if (ai_result.empty()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "AI model returned empty result", K(ret),
             K(table_id), K(column_id));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
        snprintf(sql_buf, sizeof(sql_buf),
                 "SELECT column_name FROM oceanbase.__all_column "
                 "WHERE table_id=%ld AND column_id=%ld", table_id, column_id);
        if (OB_FAIL(sql_proxy_->read(res2, sql_buf))) {
          OB_LOG(WARN, "query AI result column name failed", K(ret),
                 K(table_id), K(column_id));
        } else if (OB_ISNULL(res2.get_result())
                   || OB_SUCCESS != res2.get_result()->next()) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          ObString tmp_col;
          if (OB_FAIL(res2.get_result()->get_varchar(static_cast<int64_t>(0), tmp_col))
              || OB_FAIL(ob_write_string(alloc, tmp_col, col_name))) {
            OB_LOG(WARN, "copy AI result column name failed", K(ret),
                   K(table_id), K(column_id));
          }
        }
      }
    }
    if (OB_SUCC(ret)
        && (OB_FAIL(ob_write_string(result_allocator, ai_result, result_str))
            || OB_FAIL(ob_write_string(result_allocator, col_name, column_name))
            || OB_FAIL(ob_write_string(result_allocator, state_column_name,
                                       out_state_column_name)))) {
      OB_LOG(WARN, "copy AI task result for publication failed", K(ret),
             K(table_id), K(column_id));
    }
  }

  // 7. 成功结果等待组发布 / 失败重试
  if (OB_SUCC(ret)) {
    // Successful results stay state=2 until process_tasks() publishes the group.
  } else {
    int64_t new_retry = retry_count + 1;
    OB_LOG(WARN, "worker non-success branch", K(ret), K(new_retry), K(max_retries));
    if (new_retry > max_retries) {
      if (state_column_name.empty() || OB_ISNULL(state_column_name.ptr())
          || rowkey_predicate.empty() || OB_ISNULL(rowkey_predicate.ptr())
          || database_name.empty() || OB_ISNULL(database_name.ptr())
          || table_name.empty() || OB_ISNULL(table_name.ptr())) {
        fprintf(debug_fp, "[FIX] terminal invalid key cleanup\n");
        fflush(stderr);
        OB_LOG(ERROR, "invalid ai terminal failure key", K(ret),
               K(state_column_name), K(rowkey_predicate),
               K(tenant_id), K(table_id), K(column_id), K(request_id));
        snprintf(sql_buf, sizeof(sql_buf),
                 "DELETE FROM oceanbase.__all_ai_column_task "
                 "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
                 "AND request_id=%ld",
                 tenant_id, table_id, column_id, request_id);
        int64_t cleanup_affected = 0;
        sql_proxy_->write(sql_buf, cleanup_affected);
        ret = OB_SUCCESS;
      } else {
        // Terminal failure: persist FAILED for strict read, then remove the completed task.
        const int state_col_len = safe_string_length(state_column_name);
        const char *state_col_ptr = safe_string_ptr(state_column_name);
        const int rowkey_len = safe_string_length(rowkey_predicate);
        const char *rowkey_ptr = safe_string_ptr(rowkey_predicate);
        fprintf(debug_fp, "[FIX] terminal state update begin\n");
        fflush(stderr);
        snprintf(sql_buf, sizeof(sql_buf),
                 "UPDATE %.*s.%.*s SET %.*s=3 WHERE %.*s",
                 database_name.length(), database_name.ptr(),
                 table_name.length(), table_name.ptr(),
                 state_col_len, state_col_ptr,
                 rowkey_len, rowkey_ptr);
        int64_t state_affected = 0;
        int state_ret = sql_proxy_->write(sql_buf, state_affected);
        fprintf(debug_fp, "[FIX] terminal state write ret=%d affected=%ld\n",
                state_ret, state_affected);
        fflush(stderr);
        if (OB_SUCCESS != state_ret) {
          OB_LOG(WARN, "persist ai column failed state failed", K(state_ret), K(sql_buf));
        }
        if (OB_SUCCESS == state_ret && 0 == state_affected) {
          // affected_rows may be zero when the state is already FAILED. Verify the
          // persisted value instead of issuing an unsafe REPLACE of the user row.
          char state_check_buf[4096];
          snprintf(state_check_buf, sizeof(state_check_buf),
                   "SELECT %.*s FROM %.*s.%.*s WHERE %.*s",
                   state_col_len, state_col_ptr,
                   database_name.length(), database_name.ptr(),
                   table_name.length(), table_name.ptr(),
                   rowkey_len, rowkey_ptr);
          SMART_VAR(ObMySQLProxy::MySQLResult, state_res) {
            int64_t persisted_state = -1;
            int check_ret = sql_proxy_->read(state_res, state_check_buf);
            if (OB_SUCCESS != check_ret) {
              state_ret = check_ret;
            } else if (OB_ISNULL(state_res.get_result())
                       || OB_SUCCESS != state_res.get_result()->next()) {
              state_ret = OB_ENTRY_NOT_EXIST;
            } else if (OB_SUCCESS != (state_ret =
                       state_res.get_result()->get_int(static_cast<int64_t>(0), persisted_state))) {
            } else if (3 != persisted_state) {
              state_ret = OB_ERR_UNEXPECTED;
            } else {
              state_affected = 1;
              state_ret = OB_SUCCESS;
            }
          }
        }
        OB_LOG(WARN, "[FIX] terminal fail", K(ret), K(state_ret), K(state_affected),
               K(state_column_name), K(rowkey_predicate), K(sql_buf));
        fprintf(debug_fp, "[FIX] terminal fail ret=%d state_ret=%d affected=%ld\n",
                ret, state_ret, state_affected);
        fflush(stderr);
        if (OB_SUCCESS == state_ret) {
          snprintf(sql_buf, sizeof(sql_buf),
                   "DELETE FROM oceanbase.__all_ai_column_task "
                   "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
                   "AND rowkey_predicate='%.*s' AND request_id=%ld",
                   tenant_id, table_id, column_id,
                   rowkey_len, rowkey_ptr, request_id);
          int64_t task_affected = 0;
          int task_ret = sql_proxy_->write(sql_buf, task_affected);
          if (OB_SUCCESS != task_ret) {
            OB_LOG(WARN, "delete terminal ai task failed", K(task_ret), K(sql_buf));
          }
        } else {
          // Leave the task retryable if FAILED state cannot be persisted.
          snprintf(sql_buf, sizeof(sql_buf),
                   "UPDATE oceanbase.__all_ai_column_task SET retry_count=%ld, state=1 "
                   "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
                   "AND rowkey_predicate='%.*s' AND request_id=%ld",
                   new_retry, tenant_id, table_id, column_id,
                   rowkey_len, rowkey_ptr, request_id);
          int64_t affected = 0;
          sql_proxy_->write(sql_buf, affected);
        }
      }
    } else {
      const int rowkey_len = safe_string_length(rowkey_predicate);
      const char *rowkey_ptr = safe_string_ptr(rowkey_predicate);
      snprintf(sql_buf, sizeof(sql_buf),
               "UPDATE oceanbase.__all_ai_column_task SET retry_count=%ld, state=3, eligible_ts=now(6) + INTERVAL 1 SECOND "
               "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
               "AND rowkey_predicate='%.*s' AND request_id=%ld",
               new_retry, tenant_id, table_id, column_id,
               rowkey_len, rowkey_ptr, request_id);
      int64_t affected = 0;
      sql_proxy_->write(sql_buf, affected);
    }
    OB_LOG(WARN, "task retry handled", K(ret), K(new_retry), K(max_retries));
  }

  return ret;
}

void ObAIColumnTaskWorker::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    { FILE *fp = fopen("/tmp/worker_tick.log", "a"); if (fp) { fprintf(fp, "tick inited=0 skip\n"); fclose(fp); } }
  } else {
    int64_t affected = 0;
    int claim_ret = claim_tasks(affected);
    { FILE *fp = fopen("/tmp/worker_tick.log", "a"); if (fp) { fprintf(fp, "tick claim_ret=%d affected=%ld\n", claim_ret, affected); fclose(fp); } }
    if (OB_FAIL(claim_ret)) {
      ret = claim_ret;
    } else if (affected == 0) {
    } else {
      int process_ret = process_tasks();
      { FILE *fp = fopen("/tmp/worker_tick.log", "a"); if (fp) { fprintf(fp, "tick process_ret=%d\n", process_ret); fclose(fp); } }
      if (OB_FAIL(process_ret)) {
        ret = process_ret;
      }
    }
  }
}

} // namespace observer
} // namespace oceanbase
