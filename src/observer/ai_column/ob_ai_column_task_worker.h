#ifndef OCEANBASE_OBSERVER_AI_COLUMN_OB_AI_COLUMN_TASK_WORKER_H_
#define OCEANBASE_OBSERVER_AI_COLUMN_OB_AI_COLUMN_TASK_WORKER_H_

#include "lib/task/ob_timer_service.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace observer {

class ObAIColumnTaskWorker : public common::ObTimerTask
{
public:
  ObAIColumnTaskWorker() : inited_(false), sql_proxy_(nullptr) {}
  virtual ~ObAIColumnTaskWorker() {}
  int init(common::ObMySQLProxy *sql_proxy);
  virtual void runTimerTask() override;
private:
  int claim_tasks(int64_t &affected);
  int process_tasks();
  bool inited_;
  common::ObMySQLProxy *sql_proxy_;
};

} // namespace observer
} // namespace oceanbase

#endif
