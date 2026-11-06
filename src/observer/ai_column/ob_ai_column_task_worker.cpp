#define USING_LOG_PREFIX SERVER
#include "ob_ai_column_task_worker.h"
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
          // 第一阶段：模拟模型返回，写 tags 可见列
          // __state_tags 隐藏列暂不写，用 task_shadow 存在性表示 PENDING
          char upd_sql[1024];
          snprintf(upd_sql, sizeof(upd_sql),
                   "UPDATE ai_column_p0_t1.docs SET tags='{}' WHERE %.*s",
                   rowkey_predicate.length(), rowkey_predicate.ptr());
          int64_t dummy = 0;
          int tmp_ret = sql_proxy_->write(upd_sql, dummy);
          if (OB_SUCCESS != tmp_ret) {
            OB_LOG(WARN, "update docs state failed", K(tmp_ret), K(upd_sql));
          }
          char del_sql[1024];
          snprintf(del_sql, sizeof(del_sql),
                   "DELETE FROM ai_column_p0_t1.task_shadow "
                   "WHERE tenant_id=%ld AND table_id=%ld AND column_id=%ld "
                   "AND rowkey_predicate='%.*s'",
                   tenant_id, table_id, column_id,
                   rowkey_predicate.length(), rowkey_predicate.ptr());
          tmp_ret = sql_proxy_->write(del_sql, dummy);
          if (OB_SUCCESS != tmp_ret) {
            OB_LOG(WARN, "delete ai task failed", K(tmp_ret), K(del_sql));
          }
        }
      }
    }
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
