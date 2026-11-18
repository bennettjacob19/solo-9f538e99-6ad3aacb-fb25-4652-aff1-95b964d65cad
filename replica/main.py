"""业务副本（old / new 共用镜像，用 REPLICA_ID 区分）。

- GET  /api/work          模拟业务请求，可按 delay_ms 慢响应
- GET  /healthz           健康检查（可控制为失效）
- POST /control/health    设置健康状态（模拟健康变化）
- POST /control/delay     设置默认响应延迟（模拟慢响应）
- GET  /state             自身观测：在途、已收、已服务、收到的请求ID（供恰好一次核对）
"""
import asyncio
import os
import time
from collections import deque

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

REPLICA_ID = os.environ.get("REPLICA_ID", "replica")

app = FastAPI(title=f"replica-{REPLICA_ID}")

healthy = True
default_delay_ms = int(os.environ.get("DEFAULT_DELAY_MS", "100"))
in_flight = 0
received = 0
served = 0
received_ids: deque = deque(maxlen=5000)


@app.get("/api/work")
async def work(request: Request, delay_ms: int | None = None):
    global in_flight, received, served
    d = delay_ms if delay_ms is not None else default_delay_ms
    rid = request.headers.get("x-request-id", "-")
    received += 1
    received_ids.append(rid)
    in_flight += 1
    started = time.time()
    try:
        await asyncio.sleep(max(d, 0) / 1000)
        served += 1
        return {
            "replica": REPLICA_ID,
            "request_id": rid,
            "delay_ms": d,
            "started": round(started, 3),
            "ended": round(time.time(), 3),
        }
    finally:
        in_flight -= 1


@app.get("/healthz")
async def healthz():
    if healthy:
        return {"replica": REPLICA_ID, "healthy": True}
    return JSONResponse({"replica": REPLICA_ID, "healthy": False}, status_code=503)


@app.post("/control/health")
async def set_health(healthy: bool):
    globals()["healthy"] = healthy
    return {"replica": REPLICA_ID, "healthy": healthy}


@app.post("/control/delay")
async def set_delay(delay_ms: int):
    globals()["default_delay_ms"] = delay_ms
    return {"replica": REPLICA_ID, "default_delay_ms": delay_ms}


@app.get("/state")
async def get_state():
    return {
        "replica": REPLICA_ID,
        "healthy": healthy,
        "default_delay_ms": default_delay_ms,
        "in_flight": in_flight,
        "received": received,
        "served": served,
        "received_ids": list(received_ids),
    }
