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

# 交易日历 CSV 路径（默认为项目根目录下的 trading_calendar.csv）
TRADING_CALENDAR_CSV = "trading_calendar.csv"

# 交易日历自动拉取配置（参考 raw_data_hy 的日历获取逻辑）
# 是否在文件缺失时自动从数据源拉取
TRADING_CALENDAR_AUTO_FETCH = True
# 拉取的起始日期（包含），格式 YYYYMMDD
TRADING_CALENDAR_FETCH_START = "20140101"
# 拉取的结束日期偏移（相对今天往后若干天，默认一年）
TRADING_CALENDAR_FETCH_END_DAYS_AHEAD = 365
# 钉钉机器人（可选，未配置则不发送）
DINGTALK_ACCESS_TOKEN = "7cf00961af6929d78d10b5ff2a30a5489d7865a1d8f6eb293f437fe341cb9099"
DINGTALK_SECRET = "SEC7d70e78057a461deadd41370d561d72669e97803e0c4517dd8ad6539ee707a4c"
