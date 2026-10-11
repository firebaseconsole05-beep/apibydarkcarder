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
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

_IMPORT_ERR = None
run_checkout_for_card = None
normalize_proxy = None
parse_card_entry = None

try:
    from checkout_engine import run_checkout_for_card, normalize_proxy, parse_card_entry
except ImportError as e:
    _IMPORT_ERR = f"ImportError: {e}"
except Exception as e:
    _IMPORT_ERR = f"Exception: {e}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("darkcarderapi")

WATERMARK = "darkcarderapi"
THREAD_WORKERS = int(os.environ.get("CHECKER_THREADS", "10"))
MAX_RETRIES = int(os.environ.get("CHECKER_RETRIES", "1"))

_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=THREAD_WORKERS,
    thread_name_prefix="chk"
)
_active = 0
_lock = threading.Lock()

app = FastAPI(
    title="darkcarderapi",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,
)


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
    if not raw or not str(raw).strip():
        return ""
    if normalize_proxy:
        try:
            return normalize_proxy(raw)
        except Exception:
            pass
    p = str(raw).strip()
    if p.startswith(("http://", "https://", "socks4://", "socks5://")):
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
        return _stamp({
            "Response": "ENGINE_MISSING",
            "Status": False,
            "Gateway": "Shopify",
            "Price": "-",
            "cc": card,
            "Site": shop_url,
            "error": f"checkout_engine not loaded — {_IMPORT_ERR}",
            "retryable": False,
        })

    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            result = run_checkout_for_card(
                shop_url=shop_url,
                card_entry=card,
                proxy_url=proxy,
                low=low,
            )

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

            charged = (
                status in ("CHARGED", "SUCCESS")
                or "ORDER_PLACED" in status_code.upper()
                or "CHARGED" in status_code.upper()
            )
            approved = status in ("APPROVED", "LIVE") or any(
                x in status_code.upper() for x in ("OTP", "3DS", "INSUFFICIENT")
            )
            declined = status in ("DECLINED", "DEAD") or "DECLINED" in status_code.upper()

            if charged:
                resp_txt, st_bool, gate_status = "ORDER_PLACED", True, "Live"
            elif approved:
                resp_txt, st_bool, gate_status = status_code or "APPROVED", True, "Live"
            elif declined:
                resp_txt, st_bool, gate_status = status_code or "CARD_DECLINED", False, False
            else:
                resp_txt, st_bool, gate_status = status_code or status or "ERROR", False, False
                if retryable and attempt < MAX_RETRIES:
                    time.sleep(0.3)
                    continue

            elow = (err or "").lower()
            if any(x in elow for x in ("402", "connect tunnel", "curl: (7)", "tunnel failed", "proxy error")):
                return _stamp({
                    "Response": "PROXY_ERROR",
                    "RawResponse": status_code or "PROXY_ERROR",
                    "Status": False,
                    "Gateway": "Shopify Payments",
                    "Price": "-",
                    "cc": card,
                    "Site": shop_url,
                    "error": "Proxy dead/402 — use fresh proxy or omit proxy",
                    "retryable": True,
                    "receipt_url": "",
                })

            return _stamp({
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
            })

        except Exception as e:
            last_err = str(e)[:300]
            elow = last_err.lower()
            if attempt < MAX_RETRIES:
                time.sleep(0.2)
                continue
            if any(x in elow for x in ("402", "connect tunnel", "proxy", "curl: (7)", "curl: (56)", "tunnel failed")):
                return _stamp({
                    "Response": "PROXY_ERROR",
                    "Status": False,
                    "Gateway": "Shopify",
                    "Price": "-",
                    "cc": card,
                    "Site": shop_url,
                    "error": "Proxy dead/402 — use fresh proxy or omit proxy",
                    "retryable": True,
                })
            return _stamp({
                "Response": "ERROR",
                "Status": False,
                "Gateway": "Shopify",
                "Price": "-",
                "cc": card,
                "Site": shop_url,
                "error": last_err,
                "retryable": True,
            })

    return _stamp({
        "Response": "ERROR",
        "Status": False,
        "Gateway": "Shopify",
        "Price": "-",
        "cc": card,
        "Site": shop_url,
        "error": last_err or "unknown",
        "retryable": True,
    })


async def _run_check(shop_url: str, card: str, proxy: str, low: bool) -> dict:
    global _active
    with _lock:
        _active += 1
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _pool,
            functools.partial(_run_sync, shop_url, card, proxy, low)
        )
        return result
    finally:
        with _lock:
            _active -= 1


@app.get("/")
async def root():
    return _stamp({
        "ok": True,
        "service": "darkcarderapi",
        "engine": "ok" if run_checkout_for_card else f"fail:{_IMPORT_ERR}",
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
    cc: str = Query(None, description="card|mm|yy|cvv"),
    card: str = Query(None),
    site: str = Query(None, description="shopify store url"),
    url: str = Query(None),
    proxy: str = Query(None),
    low: str = Query("true"),
):
    card_val = (cc or card or "").strip()
    site_val = (site or url or "").strip()
    if not card_val:
        return JSONResponse(status_code=400, content=_stamp({
            "Response": "ERROR", "Status": False,
            "error": "cc/card required", "Gateway": "Shopify", "Price": "-",
        }))
    if not site_val:
        return JSONResponse(status_code=400, content=_stamp({
            "Response": "ERROR", "Status": False,
            "error": "site/url required", "Gateway": "Shopify",
            "Price": "-", "cc": card_val,
        }))
    if not site_val.startswith("http"):
        site_val = "https://" + site_val
    proxy_val = _norm_proxy(proxy or "")
    low_mode = str(low).lower() in ("1", "true", "yes")
    result = await _run_check(site_val, card_val, proxy_val, low_mode)
    return JSONResponse(content=result)


@app.get("/check")
async def check_get(
    card: str = Query(...),
    url: str = Query(...),
    proxy: str = Query(None),
    low: str = Query("true"),
):
    site_val = url if url.startswith("http") else "https://" + url
    proxy_val = _norm_proxy(proxy or "")
    result = await _run_check(
        site_val,
        card.strip(),
        proxy_val,
        str(low).lower() in ("1", "true", "yes"),
    )
    return JSONResponse(content=result)


@app.post("/check")
@app.post("/shopify")
async def check_post(body: CheckBody):
    card_val = (body.cc or body.card or "").strip()
    site_val = (body.site or body.shop_url or body.url or "").strip()
    if not card_val:
        return JSONResponse(status_code=400, content=_stamp({
            "Response": "ERROR", "Status": False,
            "error": "card required", "Gateway": "Shopify", "Price": "-",
        }))
    if not site_val:
        return JSONResponse(status_code=400, content=_stamp({
            "Response": "ERROR", "Status": False,
            "error": "site required", "Gateway": "Shopify", "Price": "-",
        }))
    if not site_val.startswith("http"):
        site_val = "https://" + site_val
    proxy_val = _norm_proxy(body.proxy or "")
    result = await _run_check(site_val, card_val, proxy_val, body.low)
    return JSONResponse(content=result)


# Vercel ASGI entrypoint
try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except ImportError:
    handler = app
