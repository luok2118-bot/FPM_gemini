#encoding:utf-8
import requests
import json
import base64
import urllib.parse
import hmac
import hashlib
import time
from typing import Optional
from loguru import logger

try:
    from config import DINGTALK_ACCESS_TOKEN, DINGTALK_SECRET
except ImportError:
    DINGTALK_ACCESS_TOKEN = ""
    DINGTALK_SECRET = ""


def _send_raw(content: str, mobile=None, is_all=None) -> bool:
    """内部实现：发送钉钉文本消息。未配置 token/secret 时静默跳过。"""
    token = (DINGTALK_ACCESS_TOKEN or "").strip()
    secret = (DINGTALK_SECRET or "").strip()
    if not token or not secret:
        return False
    body = {
        "msgtype": "text",
        "text": {"content": content},
        "at": {"atMobiles": mobile or [], "isAtAll": is_all or False}
    }
    timestamp = str(round(time.time() * 1000))
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    url = f'https://oapi.dingtalk.com/robot/send?access_token={token}&timestamp={timestamp}&sign={sign}'
    try:
        headers = {
            "Content-Type": "application/json",
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
        }
        response = requests.post(url, data=json.dumps(body), headers=headers, timeout=10)
        response_json = response.json()
        return response_json.get('errcode') == 0
    except Exception as e:
        logger.opt(exception=True).warning("dingtalk send failed: {}", e)
        return False


_STATUS_ICON = {"Success": "🟢", "Failed": "🔴", "Stopped": "🟡", "Unknown": "⚪"}


def notify_startup() -> bool:
    """系统启动钉钉通知，言简意赅。"""
    from datetime import datetime
    content = f"🔵 【FPM】因子平台已启动 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    try:
        return _send_raw(content)
    except Exception:
        logger.opt(exception=True).debug("notify_startup failed")
        return False


def notify_task_result(
    task_name: str,
    status: str,
    run_date=None,
    message: Optional[str] = None,
    duration_ms: Optional[int] = None,
    end_time=None,
) -> bool:
    """任务结果钉钉通知，言简意赅。status: Success/Failed/Stopped"""
    from datetime import datetime
    ts = end_time if end_time else datetime.now()
    date_str = ts.strftime("%Y%m%d %H:%M:%S")
    icon = _STATUS_ICON.get(status, "⚪")
    parts = [f"{icon} 【FPM】{task_name} | {status} | {date_str}"]
    if status == "Success" and duration_ms is not None:
        sec = duration_ms / 1000
        parts.append(f"{sec:.0f}s" if sec >= 1 else f"{duration_ms}ms")
    elif status == "Failed" and message:
        msg_short = (message[:80] + "…") if len(message) > 80 else message
        parts.append(msg_short)
    content = " | ".join(parts)
    try:
        return _send_raw(content)
    except Exception:
        logger.opt(exception=True).debug("notify_task_result failed")
        return False


def notify_factor_summary(task_name: str, summary_line: str, run_date=None) -> bool:
    """因子计算任务总结钉钉通知。"""
    run_str = run_date.strftime("%Y-%m-%d") if run_date else "-"
    content = f"【FPM】因子计算总结 | {task_name} | {run_str} | {summary_line.strip()}"
    try:
        return _send_raw(content)
    except Exception:
        logger.opt(exception=True).debug("notify_factor_summary failed")
        return False


def notify_factor_summary_missing(task_name: str, run_date=None) -> bool:
    """因子计算任务成功但未找到 [FPM] 总结行时发送告警。"""
    run_str = run_date.strftime("%Y-%m-%d") if run_date else "-"
    content = f"【FPM】告警 | 因子计算任务 {task_name} 已成功完成，但未在日志中发现 [FPM] 总结行，请检查脚本是否正常输出。| 运行日期={run_str}"
    try:
        return _send_raw(content)
    except Exception:
        logger.opt(exception=True).debug("notify_factor_summary_missing failed")
        return False

