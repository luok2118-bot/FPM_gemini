import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from loguru import logger


class TradingCalendar:
    """
    简单的交易日历封装：
    - 从 CSV 加载交易日（过滤 SecuMarket==83 且 IfTradingDay==1）
    - 提供 O(1) 的交易日查询
    - 缺失/越界时可回退到工作日判断（可配置）
    """

    def __init__(
        self,
        csv_path: Path,
        fallback_weekday: bool = True,
    ):
        self.csv_path = Path(csv_path)
        self.fallback_weekday = fallback_weekday
        self._dates = set()  # type: set[datetime.date]
        self._min_date: Optional[datetime.date] = None
        self._max_date: Optional[datetime.date] = None
        self._loaded = False
        self._load()

    def _load(self) -> None:
        if not self.csv_path.exists():
            logger.warning(
                "trading calendar file not found, path=%s; fallback to weekday logic",
                self.csv_path,
            )
            self._loaded = False
            return
        try:
            df = pd.read_csv(self.csv_path)
        except Exception as e:  # pragma: no cover - 防御性日志
            logger.exception(
                "failed to read trading calendar csv, fallback to weekday logic",
                path=self.csv_path,
            )
            self._loaded = False
            return

        # 过滤与 raw_data_hy 一致的条件
        if "SecuMarket" in df.columns:
            df = df[df["SecuMarket"] == 83]
        if "IfTradingDay" in df.columns:
            df = df[df["IfTradingDay"] == 1]
        if "TradingDate" not in df.columns:
            logger.warning(
                "trading calendar missing column TradingDate, path=%s; fallback to weekday logic",
                self.csv_path,
            )
            self._loaded = False
            return

        dates: list[datetime.date] = []
        for raw in df["TradingDate"]:
            if pd.isna(raw):
                continue
            s = str(raw)
            s = s[:10]  # 兼容 “YYYY-MM-DD 00:00:00”
            try:
                d = datetime.datetime.strptime(s, "%Y-%m-%d").date()
            except ValueError:
                # 再尝试纯数字格式
                try:
                    d = datetime.datetime.strptime(s, "%Y%m%d").date()
                except ValueError:
                    logger.debug("skip invalid trading date: %s", raw)
                    continue
            dates.append(d)

        if not dates:
            logger.warning(
                "trading calendar loaded but no valid dates, path=%s; fallback to weekday logic",
                self.csv_path,
            )
            self._loaded = False
            return

        self._dates = set(dates)
        self._min_date = min(self._dates)
        self._max_date = max(self._dates)
        self._loaded = True
        logger.info(
            "trading calendar loaded: %d dates, range %s ~ %s",
            len(self._dates),
            self._min_date,
            self._max_date,
        )

    def is_trading_day(self, day: datetime.date) -> bool:
        """
        判断是否交易日：
        - 在日历范围内：直接查 set
        - 不在范围或未加载：按 fallback_weekday 选择回退到 weekday() 逻辑或直接视为非交易日
        """
        if not isinstance(day, datetime.date):
            raise TypeError("day must be datetime.date")

        if self._loaded:
            if self._min_date and self._max_date and (day < self._min_date or day > self._max_date):
                logger.warning(
                    "date %s outside calendar range %s ~ %s; using fallback=%s",
                    day,
                    self._min_date,
                    self._max_date,
                    self.fallback_weekday,
                )
                return day.weekday() < 5 if self.fallback_weekday else False
            return day in self._dates

        # 未加载或加载失败
        return day.weekday() < 5 if self.fallback_weekday else False

    def get_trading_days(self, start: datetime.date, end: datetime.date) -> List[datetime.date]:
        if start > end:
            start, end = end, start
        if self._loaded:
            return sorted(d for d in self._dates if start <= d <= end)
        # fallback：仅在加载失败时使用 weekday 逻辑
        days: List[datetime.date] = []
        cur = start
        while cur <= end:
            if cur.weekday() < 5:
                days.append(cur)
            cur += datetime.timedelta(days=1)
        return days


# 单例实例，供主程序直接引用
_instance: Optional[TradingCalendar] = None


def _fetch_trading_calendar_online(
    csv_path: Path,
    start_date: str,
    end_date: str,
    secu_market: int = 83,
) -> bool:
    """
    参考 raw_data_hy.py 使用 ngshare 拉取交易日历，写入 csv_path。
    返回 True 表示写入成功，False 表示失败。
    """
    try:
        import ngshare as ng  # type: ignore
    except Exception as e:
        logger.warning("ngshare not available, skip auto-fetch trading calendar: %s", e)
        return False

    table = "QT_TradingDayNew"
    field_list = ["TradingDate", "IfTradingDay", "SecuMarket"]
    body = {
        "table": table,
        "field_list": field_list,
        "alterField": "TradingDate",
        "startDate": start_date,
        "endDate": end_date,
    }

    try:
        df = ng.get_fromDate(body)
    except Exception:
        logger.exception("failed to fetch trading calendar from ngshare")
        return False

    if df is None or df.empty:
        logger.warning("fetched empty trading calendar from ngshare")
        return False

    # 与 raw_data_hy 一致的过滤
    if "SecuMarket" in df.columns:
        df = df[df["SecuMarket"] == secu_market]
    if "IfTradingDay" in df.columns:
        df = df[df["IfTradingDay"] == 1]

    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)
        logger.info(
            "trading calendar fetched from ngshare and saved, rows=%d, path=%s",
            len(df),
            csv_path,
        )
        return True
    except Exception:
        logger.exception("failed to save trading calendar csv, path=%s", csv_path)
        return False


def init_calendar(csv_path: Path, fallback_weekday: bool = True) -> TradingCalendar:
    global _instance
    _instance = TradingCalendar(csv_path, fallback_weekday=fallback_weekday)
    return _instance


def is_trading_day(day: datetime.date) -> bool:
    global _instance
    if _instance is None:
        raise RuntimeError("Trading calendar not initialized; call init_calendar first.")
    return _instance.is_trading_day(day)


def get_trading_days(start: datetime.date, end: datetime.date) -> List[datetime.date]:
    global _instance
    if _instance is None:
        raise RuntimeError("Trading calendar not initialized; call init_calendar first.")
    return _instance.get_trading_days(start, end)


def ensure_calendar(
    csv_path: Path,
    fallback_weekday: bool = True,
    auto_fetch_online: bool = False,
    fetch_start_date: str = "20140101",
    fetch_end_date: Optional[str] = None,
    secu_market: int = 83,
    force_online_refresh: bool = False,
) -> TradingCalendar:
    """
    初始化/刷新全局交易日历：
    - 若 csv 不存在且允许自动拉取，或 force_online_refresh=True，则尝试在线获取后再加载；
    - 否则直接加载本地 csv（缺失时会回退到 weekday）。
    """
    csv_path = Path(csv_path)
    if auto_fetch_online and (force_online_refresh or not csv_path.exists()):
        if not fetch_end_date:
            # 默认取今天起往后一年，格式 YYYYMMDD
            fetch_end_date = (datetime.date.today() + datetime.timedelta(days=365)).strftime("%Y%m%d")
        _fetch_trading_calendar_online(
            csv_path=csv_path,
            start_date=fetch_start_date,
            end_date=fetch_end_date,
            secu_market=secu_market,
        )
    return init_calendar(csv_path, fallback_weekday=fallback_weekday)
