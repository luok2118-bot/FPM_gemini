# 任务队列消费者并发数，默认 1
QUEUE_WORKER_CONCURRENCY = 1

# 依赖评估周期（秒），每 N 秒扫描 Pending/Wait 并更新为 Runnable 或 Wait
QUEUE_EVAL_INTERVAL_SEC = 3

# 日志保留天数（默认 7 天）
LOG_RETENTION_DAYS = 30

# 任务队列历史记录保留天数（终态 Success/Failed/Stopped/Skipped 超期删除）
JOB_QUEUE_RETENTION_DAYS = 7

# runs 表保留天数（与 LOG_RETENTION_DAYS 一致，便于 runs 与日志对应）
RUN_RETENTION_DAYS = 30

# 输出保留天数（仅用于因子计算任务；行情更新任务始终只保留一个输出）
OUTPUT_RETENTION_DAYS = 3
