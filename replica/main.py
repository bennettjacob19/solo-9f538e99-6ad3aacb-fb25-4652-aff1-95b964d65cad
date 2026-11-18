"""业务副本（old / new 共用镜像，用 REPLICA_ID 区分）。

- GET  /api/work          模拟业务请求，可按 delay_ms 慢响应
- GET  /healthz           健康检查（可控制为失效）
- POST /control/health    设置健康状态（模拟健康变化）
- POST /control/delay     设置默认响应延迟（模拟慢响应）
- GET  /state             自身观测：在途、已收、已服务、收到的请求ID（供恰好一次核对）
- GET  /control/requests?ids=1,2,3  按 X-Request-Id 批量回报请求当前状态
- GET  /control/boot-id   本进程启动标识（重启后必然变化，网关据此区分“新进程空闲”
                          与“原请求已结束”）

请求状态（按请求 ID 记录，供网关在“上游已超时、副本是否真正处理完”的核对中使用）：
- in-flight：已受理，仍在处理；
- served：处理完成；
- unknown：本进程从未见过该 ID（例如进程重启后旧状态丢失，或 ID 已被滚动淘汰）。
"""
import asyncio
import os
import time
import uuid
from collections import deque

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

REPLICA_ID = os.environ.get("REPLICA_ID", "replica")
# 进程级启动标识：重启后必然变化。网关在超时排水核对中用它判断“原请求所在进程”
# 是否还是当前进程——新进程空闲绝不等于原请求已结束。
BOOT_ID = uuid.uuid4().hex[:12]

app = FastAPI(title=f"replica-{REPLICA_ID}")

healthy = True
default_delay_ms = int(os.environ.get("DEFAULT_DELAY_MS", "100"))
in_flight = 0
received = 0
served = 0
received_ids: deque = deque(maxlen=5000)
# 请求 ID -> 状态（in-flight / served）。有界保留，超出后淘汰最旧记录，
# 被淘汰的 ID 在查询接口回报 unknown。
request_states: dict[str, str] = {}
REQUEST_STATE_MAX = 5000


@app.get("/api/work")
async def work(request: Request, delay_ms: int | None = None):
    global in_flight, received, served
    d = delay_ms if delay_ms is not None else default_delay_ms
    rid = request.headers.get("x-request-id", "-")
    received += 1
    received_ids.append(rid)
    in_flight += 1
    if len(request_states) >= REQUEST_STATE_MAX:
        request_states.pop(next(iter(request_states)))
    request_states[rid] = "in-flight"
    started = time.time()
    try:
        await asyncio.sleep(max(d, 0) / 1000)
        served += 1
        request_states[rid] = "served"
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


@app.get("/control/boot-id")
async def boot_id():
    return {"replica": REPLICA_ID, "boot_id": BOOT_ID}


@app.get("/control/requests")
async def request_statuses(ids: str = ""):
    """按请求 ID 批量回报本进程观测到的状态。

    每个 ID 回报：in-flight（仍在处理）/ served（已处理完）/ unknown（本进程
    没有该请求的记录：重启后状态丢失或记录已淘汰——调用方必须按“未确认”处理）。
    """
    wanted = [x.strip() for x in ids.split(",") if x.strip()]
    return {
        "replica": REPLICA_ID,
        "boot_id": BOOT_ID,
        "states": {rid: request_states.get(rid, "unknown") for rid in wanted},
    }


@app.get("/state")
async def get_state():
    return {
        "replica": REPLICA_ID,
        "boot_id": BOOT_ID,
        "healthy": healthy,
        "default_delay_ms": default_delay_ms,
        "in_flight": in_flight,
        "received": received,
        "served": served,
        "received_ids": list(received_ids),
    }
