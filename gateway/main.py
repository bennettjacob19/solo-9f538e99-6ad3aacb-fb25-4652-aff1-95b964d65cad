"""流量切换网关（cutover gateway）。

职责：
- 把 /api/* 反向代理到当前活动副本（old 或 new）；
- 健康检查两个副本；新副本未就绪时拒绝切换，旧副本继续接单；
- 切换后新请求只进新副本；旧副本在途请求真正结束前不显示“可停机”；
- 客户端超时/断连不等于上游结束：上游任务被 shield 保护，计数只在上游
  请求真正结束时扣减；
- 宣告完成前新副本失效 -> 自动回滚，旧副本重新接单；
- 计数恰好一次：受理时在锁内 +1，每个上游任务的 done 回调里 -1（恰好一次）。
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, Response

OLD_URL = os.environ.get("OLD_URL", "http://127.0.0.1:8001").rstrip("/")
NEW_URL = os.environ.get("NEW_URL", "http://127.0.0.1:8002").rstrip("/")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.0"))
UPSTREAM_TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "120"))
REPLICAS = {"old": OLD_URL, "new": NEW_URL}
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [gateway] %(levelname)s %(message)s")
log = logging.getLogger("gateway")


def ts() -> str:
    return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"


class CutoverState:
    """全部字段只在事件循环线程内访问。

    不变式（恰好一次计数）：
    - 受理：在 lock 内读取 active 并 in_flight[active] += 1，同时创建上游任务，
      中间无任何 await，切换/回滚不可能插进来 -> 每个请求只被计到恰好一个副本；
    - 完成：每个受理的请求对应恰好一个上游任务，其 done 回调恰好执行一次，
      且只在上游真正结束（拿到响应/出错/任务被取消）时执行 -> 不漏计不重计；
    - 客户端断连只取消“等待响应”的处理器协程，上游任务被 shield 保护继续运行，
      计数不变，直到上游真正结束。
    """

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.active = "old"                # 当前活动副本：old | new
        self.declared_complete = False     # 是否已宣告完成（宣告后不再回滚）
        self.in_flight = {"old": 0, "new": 0}
        self.health = {"old": None, "new": None}   # None=未知
        self.admitted = {"old": 0, "new": 0}
        self.completed = {"old": 0, "new": 0}
        self.failed = {"old": 0, "new": 0}
        self.next_id = 0
        self.generation = 0                # 每次切换/回滚 +1，便于观察
        self.events: deque = deque(maxlen=300)
        self.requests: deque = deque(maxlen=300)


state = CutoverState()
client: httpx.AsyncClient | None = None
# 强引用持有上游任务，防止被 GC（asyncio 对未引用的任务会告警甚至提前回收）
background_tasks: set[asyncio.Task] = set()


def log_event(msg: str) -> None:
    state.events.appendleft({"ts": ts(), "msg": msg})
    log.info(msg)


async def check_health(name: str) -> bool:
    try:
        r = await client.get(REPLICAS[name] + "/healthz", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


async def health_poller() -> None:
    """周期健康检查；发现 new 在宣告完成前失效 -> 自动回滚到 old。"""
    await asyncio.sleep(0.2)
    while True:
        results = {name: await check_health(name) for name in ("old", "new")}
        async with state.lock:
            for name, ok in results.items():
                prev = state.health[name]
                state.health[name] = ok
                if prev != ok:
                    log_event(f"副本 {name} 健康状态 -> {'健康' if ok else '失效'}")
            if state.active == "new" and not state.declared_complete and results["new"] is False:
                state.active = "old"
                state.generation += 1
                log_event("自动回滚：新副本在宣告完成前失效，流量切回旧副本，旧副本重新接单")
        await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = httpx.AsyncClient()
    poller = asyncio.create_task(health_poller())
    log_event(f"网关启动：活动副本=old (old={OLD_URL}, new={NEW_URL})")
    yield
    poller.cancel()
    await client.aclose()


app = FastAPI(title="cutover-gateway", lifespan=lifespan)


# ---------------------------------------------------------------- 反向代理

@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    body = await request.body()

    # ---- 受理（恰好一次）：锁内选定副本、计数 +1、创建上游任务，中间无 await ----
    async with state.lock:
        target = state.active
        state.next_id += 1
        rid = state.next_id
        state.in_flight[target] += 1
        state.admitted[target] += 1
        rec = {
            "id": rid, "ts": ts(), "replica": target, "path": f"/api/{path}",
            "status": "in-flight", "http_status": None, "latency_ms": None,
            "client_gone": False, "gen": state.generation,
        }
        state.requests.appendleft(rec)

        url = f"{REPLICAS[target]}/api/{path}"
        if request.url.query:
            url += "?" + request.url.query

        async def call_upstream():
            r = await client.request(
                request.method, url, content=body,
                headers={"X-Request-Id": str(rid)}, timeout=UPSTREAM_TIMEOUT,
            )
            return r.status_code, r.content, r.headers.get("content-type", "application/json")

        task = asyncio.create_task(call_upstream(), name=f"req-{rid}")
        background_tasks.add(task)
        t0 = time.monotonic()

        def on_done(t: asyncio.Task, target: str = target, rec: dict = rec, t0: float = t0) -> None:
            # 每个任务恰好执行一次；只在事件循环内同步执行，天然原子
            state.in_flight[target] -= 1
            rec["latency_ms"] = round((time.monotonic() - t0) * 1000, 1)
            if t.cancelled():
                rec["status"] = "cancelled"
                state.failed[target] += 1
                return
            exc = t.exception()
            if exc is not None:
                rec["status"] = "upstream-error"
                rec["error"] = f"{type(exc).__name__}: {exc}"
                state.failed[target] += 1
            else:
                code, _, _ = t.result()
                rec["status"] = "done"
                rec["http_status"] = code
                state.completed[target] += 1

        task.add_done_callback(on_done)
        task.add_done_callback(background_tasks.discard)

    # ---- 等待上游；客户端断连时上游任务继续运行（shield），计数不受影响 ----
    try:
        status_code, content, ctype = await asyncio.shield(task)
    except asyncio.CancelledError:
        rec["client_gone"] = True
        log_event(f"请求 #{rid} 客户端断连/超时；其在 {target} 上的上游请求继续执行直至真正结束")
        raise
    except Exception as exc:  # 上游连接失败等
        return JSONResponse(
            {"error": f"upstream '{target}' unavailable", "detail": str(exc), "request_id": rid},
            status_code=502,
        )
    return Response(content=content, status_code=status_code, media_type=ctype.split(";")[0])


# ---------------------------------------------------------------- 运维操作

@app.post("/ops/switch")
async def switch_to_new():
    """发起切换。新副本未就绪 -> 拒绝，旧副本继续接单。"""
    ok = await check_health("new")   # 切换前实时探活，不依赖周期任务
    async with state.lock:
        state.health["new"] = ok
        if not ok:
            log_event("切换被拒绝：新副本未就绪，旧副本继续接单")
            return JSONResponse({"ok": False, "reason": "新副本未就绪，已拒绝切换，旧副本继续接单"}, status_code=409)
        if state.active == "new":
            return {"ok": True, "msg": "已在新副本上"}
        if state.declared_complete:
            return JSONResponse({"ok": False, "reason": "已宣告完成，无需再切换"}, status_code=409)
        state.active = "new"
        state.generation += 1
        log_event("切换：新请求只进新副本（旧副本在途请求继续排水）")
        return {"ok": True, "msg": "已切换到新副本"}


@app.post("/ops/rollback")
async def rollback_manual():
    """手动回滚（仅宣告完成前可用）。"""
    async with state.lock:
        if state.active == "old":
            return JSONResponse({"ok": False, "reason": "当前就在旧副本上"}, status_code=409)
        if state.declared_complete:
            return JSONResponse({"ok": False, "reason": "已宣告完成，回滚已禁用"}, status_code=409)
        state.active = "old"
        state.generation += 1
        log_event("手动回滚：流量切回旧副本")
        return {"ok": True, "msg": "已回滚到旧副本"}


@app.post("/ops/declare-complete")
async def declare_complete():
    """宣告完成：此后禁用回滚。旧副本可停机仍需等在途清零。"""
    ok = await check_health("new")
    async with state.lock:
        state.health["new"] = ok
        if state.active != "new":
            return JSONResponse({"ok": False, "reason": "尚未切换到新副本"}, status_code=409)
        if not ok:
            log_event("宣告完成被拒绝：新副本当前失效")
            return JSONResponse({"ok": False, "reason": "新副本当前失效，不能宣告完成"}, status_code=409)
        if state.declared_complete:
            return {"ok": True, "msg": "已宣告过完成"}
        state.declared_complete = True
        log_event("宣告完成：切换定案，回滚禁用；旧副本在途清零后即可停机")
        return {"ok": True, "msg": "已宣告完成"}


@app.post("/ops/reset")
async def reset_demo():
    """演示用：在无在途请求时复位到初始状态（活动=old，未宣告）。"""
    async with state.lock:
        if state.in_flight["old"] or state.in_flight["new"]:
            return JSONResponse({"ok": False, "reason": "仍有在途请求，不能复位"}, status_code=409)
        state.active = "old"
        state.declared_complete = False
        state.generation += 1
        state.events.clear()      # 复位给演示一个干净的日志视图；计数器保持累计
        state.requests.clear()
        log_event("复位：回到初始状态（活动副本=old）")
        return {"ok": True}


@app.get("/ops/status")
async def get_status():
    async with state.lock:
        old_drained = state.in_flight["old"] == 0
        return {
            "active": state.active,
            "declared_complete": state.declared_complete,
            "in_flight": dict(state.in_flight),
            "health": dict(state.health),
            "admitted": dict(state.admitted),
            "completed": dict(state.completed),
            "failed": dict(state.failed),
            "old_drained": old_drained,
            # 可停机 = 已宣告完成（不会再回滚，旧副本不再需要兜底）且旧副本在途真正清零
            "safe_to_stop_old": state.declared_complete and old_drained,
            "generation": state.generation,
            "replicas": dict(REPLICAS),
            "events": list(state.events)[:60],
            "requests": list(state.requests)[:80],
        }


# 通过网关代理副本控制接口（compose 中副本不对外暴露端口）

@app.post("/ops/replica/{name}/health")
async def set_replica_health(name: str, healthy: bool):
    if name not in REPLICAS:
        return JSONResponse({"ok": False, "reason": "unknown replica"}, status_code=404)
    try:
        r = await client.post(f"{REPLICAS[name]}/control/health?healthy={str(healthy).lower()}", timeout=3.0)
        return JSONResponse(r.json(), status_code=r.status_code)
    except Exception as exc:
        return JSONResponse({"ok": False, "reason": f"replica unreachable: {exc}"}, status_code=502)


@app.post("/ops/replica/{name}/delay")
async def set_replica_delay(name: str, delay_ms: int):
    if name not in REPLICAS:
        return JSONResponse({"ok": False, "reason": "unknown replica"}, status_code=404)
    try:
        r = await client.post(f"{REPLICAS[name]}/control/delay?delay_ms={delay_ms}", timeout=3.0)
        return JSONResponse(r.json(), status_code=r.status_code)
    except Exception as exc:
        return JSONResponse({"ok": False, "reason": f"replica unreachable: {exc}"}, status_code=502)


@app.get("/ops/replica/{name}/state")
async def replica_state(name: str):
    if name not in REPLICAS:
        return JSONResponse({"ok": False, "reason": "unknown replica"}, status_code=404)
    try:
        r = await client.get(f"{REPLICAS[name]}/state", timeout=3.0)
        return JSONResponse(r.json(), status_code=r.status_code)
    except Exception as exc:
        return JSONResponse({"ok": False, "reason": f"replica unreachable: {exc}"}, status_code=502)


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
