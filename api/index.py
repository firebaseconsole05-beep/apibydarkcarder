"""
darkcarderapi — Shopify CC Check API (Vercel)
Watermark: darkcarderapi
"""
import os
import sys
import asyncio
import concurrent.futures
import functools
import logging
import time
import threading
from typing import Optional, Tuple

# ensure api/ is on path for checkout_engine
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

try:
    from checkout_engine import run_checkout_for_card, normalize_proxy, parse_card_entry
except Exception as e:
    run_checkout_for_card = None
    normalize_proxy = None
    parse_card_entry = None
    _IMPORT_ERR = str(e)
else:
    _IMPORT_ERR = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("darkcarderapi")

WATERMARK = "darkcarderapi"
THREAD_WORKERS = int(os.environ.get("CHECKER_THREADS", "20"))
MAX_RETRIES = int(os.environ.get("CHECKER_RETRIES", "1"))

_pool = concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_WORKERS, thread_name_prefix="chk")
_active = 0
_lock = threading.Lock()

app = FastAPI(title="darkcarderapi", version="1.0.0", docs_url="/docs", redoc_url=None)


def _stamp(d: dict) -> dict:
    d["dev"] = WATERMARK
    d["watermark"] = WATERMARK
    d["powered_by"] = "darkcarderapi"
    return d


class CheckBody(BaseModel):
    card: Optional[str] = None
    cc: Optional[str] = None
    shop_url: Optional[str] = None
    site: Optional[str] = None
    url: Optional[str] = None
    proxy: Optional[str] = None
    low: bool = True


def _norm_proxy(raw: str) -> str:
    """Optional proxy. Empty string = direct (no proxy)."""
    if not raw or not str(raw).strip():
        return ""
    if normalize_proxy:
        try:
            return normalize_proxy(raw)
        except Exception:
            pass
    p = str(raw).strip()
    if p.startswith("http://") or p.startswith("https://") or p.startswith("socks"):
        return p
    parts = p.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    if len(parts) == 2:
        return f"http://{p}"
    return f"http://{p}"


def _run_sync(shop_url: str, card: str, proxy: str, low: bool) -> dict:
    if run_checkout_for_card is None:
        return {
            "Response": "ERROR",
            "Status": False,
            "Gateway": "Shopify",
            "Price": "-",
            "cc": card,
            "error": f"engine import failed: {_IMPORT_ERR}",
            "retryable": True,
        }
    last = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            result = run_checkout_for_card(
                shop_url=shop_url,
                card_entry=card,
                proxy_url=proxy,
                low=low,
            )
            # CheckResult dataclass / object
            if isinstance(result, dict):
                data = result
            else:
                st = getattr(result, "status", None)
                st_name = st.name if hasattr(st, "name") else str(st or "")
                err_obj = getattr(result, "error", None)
                data = {
                    "status": st_name,
                    "status_code": getattr(result, "status_code", "") or st_name,
                    "amount": getattr(result, "amount", "") or "-",
                    "error": str(err_obj) if err_obj else "",
                    "retryable": bool(getattr(result, "retryable", False)),
                    "receipt_url": getattr(result, "receipt_url", "") or "",
                    "gateway": "Shopify Payments",
                }

            status = str(data.get("status") or "").upper()
            status_code = str(data.get("status_code") or status)
            amount = str(data.get("amount") or "-")
            err = str(data.get("error") or "")
            retryable = bool(data.get("retryable", False))

            charged = status in ("CHARGED", "SUCCESS") or "ORDER_PLACED" in status_code.upper() or "CHARGED" in status_code.upper()
            approved = status in ("APPROVED", "LIVE") or any(
                x in status_code.upper() for x in ("OTP", "3DS", "INSUFFICIENT")
            )
            declined = status in ("DECLINED", "DEAD") or "DECLINED" in status_code.upper()

            if charged:
                resp_txt, st_bool = "ORDER_PLACED", True
                gate_status = "Live"
            elif approved:
                resp_txt, st_bool = status_code or "APPROVED", True
                gate_status = "Live"
            elif declined:
                resp_txt, st_bool = status_code or "CARD_DECLINED", False
                gate_status = False
            else:
                resp_txt, st_bool = status_code or status or "ERROR", False
                gate_status = False
                if retryable and attempt < MAX_RETRIES:
                    time.sleep(0.3)
                    continue

            # Classify proxy failures from engine error text
            elow = (err or "").lower()
            if any(x in elow for x in ("402", "connect tunnel", "curl: (7)", "tunnel failed", "proxy error")):
                return {
                    "Response": "PROXY_ERROR",
                    "RawResponse": status_code or "PROXY_ERROR",
                    "Status": False,
                    "Gateway": "Shopify Payments",
                    "Price": "-",
                    "cc": card,
                    "Site": shop_url,
                    "error": "Proxy dead/402 — use fresh proxy or call without proxy",
                    "retryable": True,
                    "receipt_url": "",
                }
            return {
                "Response": resp_txt,
                "RawResponse": status_code,
                "Status": gate_status if isinstance(gate_status, str) else st_bool,
                "Gateway": data.get("gateway") or data.get("Gate") or "Shopify Payments",
                "Price": amount,
                "cc": card,
                "Site": shop_url,
                "error": err,
                "retryable": retryable and not (charged or approved or declined),
                "receipt_url": data.get("receipt_url") or "",
            }
        except Exception as e:
            last = str(e)[:300]
            low = last.lower()
            if attempt < MAX_RETRIES:
                time.sleep(0.2)
                continue
            # Clear proxy-dead messages
            if any(x in low for x in ("402", "connect tunnel", "proxy", "curl: (7)", "curl: (56)", "tunnel failed")):
                return {
                    "Response": "PROXY_ERROR",
                    "Status": False,
                    "Gateway": "Shopify",
                    "Price": "-",
                    "cc": card,
                    "error": "Proxy dead/402 — use fresh proxy or omit proxy",
                    "retryable": True,
                }
            return {
                "Response": "ERROR",
                "Status": False,
                "Gateway": "Shopify",
                "Price": "-",
                "cc": card,
                "error": last,
                "retryable": True,
            }
    return {
        "Response": "ERROR",
        "Status": False,
        "Gateway": "Shopify",
        "Price": "-",
        "cc": card,
        "error": last or "unknown",
        "retryable": True,
    }


async def _run_check(shop_url: str, card: str, proxy: str, low: bool) -> dict:
    global _active
    with _lock:
        _active += 1
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _pool, functools.partial(_run_sync, shop_url, card, proxy, low)
        )
        return _stamp(result)
    finally:
        with _lock:
            _active -= 1


@app.get("/")
async def root():
    return _stamp({
        "ok": True,
        "service": "darkcarderapi",
        "endpoints": ["/shopify", "/check", "/health", "/status"],
    })


@app.get("/health")
@app.get("/status")
async def health():
    with _lock:
        active = _active
    return _stamp({
        "status": "online",
        "ok": True,
        "threads": THREAD_WORKERS,
        "active_checks": active,
        "engine": "ok" if run_checkout_for_card else f"fail:{_IMPORT_ERR}",
    })


@app.get("/shopify")
async def shopify_get(
    cc: str = Query(None, description="card number|mm|yy|cvv"),
    card: str = Query(None),
    site: str = Query(None, description="shopify store url"),
    url: str = Query(None),
    proxy: str = Query(None),
    low: str = Query("true"),
):
    """Bot-compatible: /shopify?cc=...&site=...&proxy=..."""
    card_val = (cc or card or "").strip()
    site_val = (site or url or "").strip()
    if not card_val:
        return JSONResponse(_stamp({"Response": "ERROR", "Status": False, "error": "cc required", "Gateway": "Shopify", "Price": "-"}))
    if not site_val:
        return JSONResponse(_stamp({"Response": "ERROR", "Status": False, "error": "site required", "Gateway": "Shopify", "Price": "-", "cc": card_val}))
    if not site_val.startswith("http"):
        site_val = "https://" + site_val
    proxy_val = _norm_proxy(proxy or "")
    low_mode = str(low).lower() in ("1", "true", "yes")
    return await _run_check(site_val, card_val, proxy_val, low_mode)


@app.get("/check")
async def check_get(
    card: str = Query(...),
    url: str = Query(...),
    proxy: str = Query(...),
    low: str = Query("true"),
):
    site_val = url if url.startswith("http") else "https://" + url
    try:
        proxy_val = _norm_proxy(proxy)
    except Exception as e:
        return JSONResponse(_stamp({"Response": "ERROR", "Status": False, "error": str(e), "cc": card, "Gateway": "Shopify", "Price": "-"}))
    return await _run_check(site_val, card.strip(), proxy_val, str(low).lower() in ("1", "true", "yes"))


@app.post("/check")
@app.post("/shopify")
async def check_post(body: CheckBody):
    card_val = (body.cc or body.card or "").strip()
    site_val = (body.site or body.shop_url or body.url or "").strip()
    if not site_val.startswith("http"):
        site_val = "https://" + site_val
    try:
        proxy_val = _norm_proxy(body.proxy or "")
    except Exception as e:
        return JSONResponse(_stamp({"Response": "ERROR", "Status": False, "error": str(e), "cc": card_val, "Gateway": "Shopify", "Price": "-"}))
    return await _run_check(site_val, card_val, proxy_val, body.low)


# Vercel / ASGI entrypoints
# Vercel Python looks for `app` (ASGI) or `handler`
try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except Exception as _e:
    handler = app  # fallback: some runtimes accept raw ASGI app
