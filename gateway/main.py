"""流量切换网关（cutover gateway）。

职责：
- 把 /api/* 反向代理到当前活动副本（old 或 new）；
- 健康检查两个副本；新副本未就绪时拒绝切换，旧副本继续接单；
- 切换后新请求只进新副本；旧副本在途请求真正结束前不显示“可停机”；
- 客户端超时/断连不等于上游结束：上游任务被 shield 保护，计数只在上游
  请求真正结束时扣减；
- **网关自身 UPSTREAM_TIMEOUT 也不等于上游已结束**：超时后客户端立刻收到
  502，但该请求转入“待副本确认”，持续阻止旧副本停机，直到向副本核对到
  该请求真正处理完成（或人工确认）。副本暂不可查 / 重启后查无此请求时
  一律保持“未确认”，绝不能把新进程的空闲误判为原请求已结束；
- 宣告完成前新副本失效 -> 自动回滚，旧副本重新接单；
- 计数恰好一次：受理时在锁内 +1，每个上游任务的 done 回调里状态恰好
  流转一次（完成/失败/待确认三选一）。
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
# 超时请求转入“待确认”后，向副本核对请求真实状态的周期
CONFIRM_POLL_INTERVAL = float(os.environ.get("CONFIRM_POLL_INTERVAL", "1.0"))
REPLICAS = {"old": OLD_URL, "new": NEW_URL}
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [gateway] %(levelname)s %(message)s")
log = logging.getLogger("gateway")


def ts() -> str:
    return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"


class CutoverState:
    """全部字段只在事件循环线程内访问。

    不变式（恰好一次计数，按副本分别成立）：
        admitted == completed + failed + in_flight + unconfirmed

    - 受理：在 lock 内读取 active 并 in_flight[active] += 1，同时创建上游任务，
      中间无任何 await，切换/回滚不可能插进来 -> 每个请求只被计到恰好一个副本；
    - 完成/失败：每个受理的请求对应恰好一个上游任务，其 done 回调恰好执行一次，
      且只在上游调用真正出结果时执行 -> 不漏计不重计；
    - 网关超时（httpx.TimeoutException）：客户端立刻拿到 502，但上游可能仍在
      处理。该请求从 in_flight 转入 unconfirmed（待副本确认），计数不丢、不计
      完成也不计失败，持续阻止旧副本停机；
    - 客户端断连只取消“等待响应”的处理器协程，上游任务被 shield 保护继续运行，
      计数不变，直到上游真正结束或网关超时转入待确认。
    """

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.active = "old"                # 当前活动副本：old | new
        self.declared_complete = False     # 是否已宣告完成（宣告后不再回滚）
        self.in_flight = {"old": 0, "new": 0}
        # 网关已超时、等待向副本核对真实结局的请求数（仍占住停机判断）
        self.unconfirmed = {"old": 0, "new": 0}
        self.health = {"old": None, "new": None}   # None=未知
        self.admitted = {"old": 0, "new": 0}
        self.completed = {"old": 0, "new": 0}
        self.failed = {"old": 0, "new": 0}
        self.next_id = 0
        self.generation = 0                # 每次切换/回滚 +1，便于观察
        # 最近一次成功核对到的各副本进程启动标识（重启后变化）
        self.replica_boot_ids = {"old": None, "new": None}
        # 待确认请求：rid -> {"id","replica","rec","boot_id","restarted","reason"}
        self.pending: dict[int, dict] = {}
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


async def _query_replica_requests(name: str, ids: list[int]) -> dict | None:
    """向副本批量查询请求状态。不可达/出错返回 None（调用方按“暂不可查”处理）。"""
    try:
        r = await client.get(
            REPLICAS[name] + "/control/requests",
            params={"ids": ",".join(str(i) for i in ids)},
            timeout=2.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("核对副本 %s 请求状态失败：%s", name, exc)
        return None


async def _query_replica_snapshot(name: str) -> dict | None:
    """查询副本自身观测快照（含进程在途数）。不可达返回 None。"""
    try:
        r = await client.get(REPLICAS[name] + "/state", timeout=2.0)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("查询副本 %s 快照失败：%s", name, exc)
        return None


async def confirmation_reconciler() -> None:
    """周期核对“网关已超时、结局未确认”的请求。

    核对规则（保守原则：拿不到“原请求确已结束”的证据就一直阻止停机）：
    - 副本进程 boot_id 与超时时刻记录的不一致 -> 副本已重启，原请求记录丢失，
      永久保持未确认（标记 restarted），只能人工确认；
    - 同进程回报 served -> 原请求确已处理完成，转为完成计数并解除阻止；
    - 同进程回报 in-flight -> 原请求仍在处理，继续阻止；
    - 同进程回报 unknown（记录被有界淘汰等）-> 结合副本进程在途数判断：同进程
      已无在途则确认结束，否则保持未确认继续阻止；
    - 副本暂不可查 -> 保持未确认，下轮再查。
    """
    await asyncio.sleep(0.3)
    while True:
        await asyncio.sleep(CONFIRM_POLL_INTERVAL)
        # 锁内取一致快照，HTTP 查询在锁外做（避免阻塞受理/状态接口）
        async with state.lock:
            by_replica = {"old": [], "new": []}
            for rid, p in state.pending.items():
                if not p["restarted"]:
                    by_replica[p["replica"]].append(rid)
        for name, ids in by_replica.items():
            if not ids:
                continue
            data = await _query_replica_requests(name, ids)
            # 锁内只做分类，不做 HTTP 等待；unknown 项先收集，快照查询放到锁外
            unknown_ids: list[int] = []
            async with state.lock:
                if data is None:
                    # 副本暂不可查：全部保持未确认；仅在原因变化时记一条事件
                    for rid in ids:
                        p = state.pending.get(rid)
                        if p and p["reason"] != "副本暂不可查，未确认":
                            p["reason"] = "副本暂不可查，未确认"
                            p["rec"]["confirm_reason"] = p["reason"]
                            log_event(f"请求 #{rid} 待确认：副本 {name} 暂不可查，"
                                      f"继续阻止 {name} 停机")
                    continue
                boot = data.get("boot_id")
                if state.replica_boot_ids[name] != boot:
                    log_event(f"副本 {name} 进程标识变化（{state.replica_boot_ids[name]} -> {boot}）")
                    state.replica_boot_ids[name] = boot
                states = data.get("states", {})
                for rid in ids:
                    p = state.pending.get(rid)
                    if not p:
                        continue
                    if p["boot_id"] is None:
                        # 超时时刻还没拿到过进程标识：首次成功查询时落锚
                        p["boot_id"] = boot
                    if boot != p["boot_id"]:
                        p["restarted"] = True
                        p["reason"] = "副本已重启，原请求记录丢失，未确认（需人工确认）"
                        p["rec"]["status"] = "replica-restarted"
                        p["rec"]["confirm_reason"] = p["reason"]
                        log_event(f"请求 #{rid} 待确认：副本 {name} 已重启（boot_id 不一致），"
                                  f"新进程空闲不等于原请求结束，持续阻止停机")
                        continue
                    st = states.get(str(rid), "unknown")
                    if st == "served":
                        _confirm_served_locked(rid, p, name)
                    elif st == "in-flight":
                        if p["reason"] != "副本确认请求仍在处理":
                            p["reason"] = "副本确认请求仍在处理"
                            p["rec"]["confirm_reason"] = p["reason"]
                            log_event(f"请求 #{rid} 待确认：副本 {name} 确认仍在处理，继续阻止停机")
                    else:  # unknown：同进程查无记录，需结合副本在途数进一步判断
                        unknown_ids.append(rid)
            if not unknown_ids:
                continue
            # 锁外查询副本快照：同进程无在途时，unknown 只可能是处理完后记录被淘汰
            snap = await _query_replica_snapshot(name)
            replica_busy = bool(snap and snap.get("in_flight"))
            async with state.lock:
                for rid in unknown_ids:
                    p = state.pending.get(rid)
                    if not p:
                        continue
                    if snap is not None and not replica_busy:
                        _confirm_served_locked(rid, p, name)
                    else:
                        reason = ("副本查无该请求记录且仍有在途请求，未确认"
                                  if replica_busy else "副本查无该请求记录，未确认")
                        if p["reason"] != reason:
                            p["reason"] = reason
                            p["rec"]["confirm_reason"] = reason
                            log_event(f"请求 #{rid} 待确认：副本 {name} 查无请求 #{rid} 记录，"
                                      f"按未确认处理，继续阻止停机")


def _confirm_served_locked(rid: int, p: dict, name: str) -> None:
    """副本确认原请求已处理完成：待确认 -> 完成，解除停机阻止。"""
    state.unconfirmed[name] -= 1
    state.completed[name] += 1
    p["rec"]["status"] = "confirmed-done"
    p["rec"]["confirm_reason"] = "副本确认原请求已处理完成"
    state.pending.pop(rid, None)
    log_event(f"请求 #{rid} 已由副本 {name} 确认处理完成，解除其对停机的阻止")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = httpx.AsyncClient()
    poller = asyncio.create_task(health_poller())
    reconciler = asyncio.create_task(confirmation_reconciler())
    log_event(f"网关启动：活动副本=old (old={OLD_URL}, new={NEW_URL}, "
              f"UPSTREAM_TIMEOUT={UPSTREAM_TIMEOUT}s)")
    yield
    poller.cancel()
    reconciler.cancel()
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
            "client_gone": False, "gen": state.generation, "confirm_reason": None,
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
            rec["latency_ms"] = round((time.monotonic() - t0) * 1000, 1)
            if t.cancelled():
                state.in_flight[target] -= 1
                rec["status"] = "cancelled"
                state.failed[target] += 1
                return
            exc = t.exception()
            if exc is not None:
                if isinstance(exc, httpx.TimeoutException):
                    # 网关超时：客户端将收到 502，但上游可能仍在处理。
                    # 不计完成/失败，转入“待副本确认”，继续阻止该副本停机。
                    state.in_flight[target] -= 1
                    state.unconfirmed[target] += 1
                    rec["status"] = "upstream-timeout"
                    rec["error"] = f"{type(exc).__name__}: {exc}"
                    state.pending[rec["id"]] = {
                        "id": rec["id"],
                        "replica": target,
                        "rec": rec,
                        "boot_id": state.replica_boot_ids[target],
                        "restarted": False,
                        "reason": "网关等待超时，等待副本确认请求结局",
                    }
                    rec["confirm_reason"] = state.pending[rec["id"]]["reason"]
                    log_event(f"请求 #{rid} 等待上游 {target} 超时（{UPSTREAM_TIMEOUT}s）："
                              f"客户端将收到 502；请求转为待确认，在副本确认处理结束前阻止停机")
                    return
                state.in_flight[target] -= 1
                rec["status"] = "upstream-error"
                rec["error"] = f"{type(exc).__name__}: {exc}"
                state.failed[target] += 1
            else:
                code, _, _ = t.result()
                state.in_flight[target] -= 1
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
        log_event(f"请求 #{rid} 客户端断连/超时；其在 {target} 上的上游请求继续执行直至真正结束或转入待确认")
        raise
    except httpx.TimeoutException as exc:
        # 客户端及时拿到错误；上游任务已在 done 回调中转入“待确认”，排水判断不受影响
        return JSONResponse(
            {
                "error": f"upstream '{target}' timeout after {UPSTREAM_TIMEOUT}s",
                "detail": "客户端请求到此结束；上游可能仍在处理，网关已将该请求转为待确认，"
                          "在副本确认处理结束前不会判定该副本可停机",
                "request_id": rid,
            },
            status_code=502,
        )
    except Exception as exc:  # 上游连接失败等：调用确实没有送达，按失败结束
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
        log_event("切换：新请求只进新副本（旧副本在途/待确认请求继续阻止停机）")
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
    """宣告完成：此后禁用回滚。旧副本可停机仍需等在途与待确认全部清零。"""
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
        log_event("宣告完成：切换定案，回滚禁用；旧副本在途/待确认清零后即可停机")
        return {"ok": True, "msg": "已宣告完成"}


@app.post("/ops/reset")
async def reset_demo():
    """演示用：在无在途、无待确认请求时复位到初始状态（活动=old，未宣告）。"""
    async with state.lock:
        if state.in_flight["old"] or state.in_flight["new"]:
            return JSONResponse({"ok": False, "reason": "仍有在途请求，不能复位"}, status_code=409)
        if state.pending:
            return JSONResponse({"ok": False, "reason": "仍有待确认请求，不能复位"}, status_code=409)
        state.active = "old"
        state.declared_complete = False
        state.generation += 1
        state.events.clear()      # 复位给演示一个干净的日志视图；计数器保持累计
        state.requests.clear()
        log_event("复位：回到初始状态（活动副本=old）")
        return {"ok": True}


def _old_blockers_locked() -> list[dict]:
    """旧副本停机受阻明细：在途请求 + 待确认请求。"""
    blockers = []
    for rec in state.requests:
        if rec["replica"] == "old" and rec["status"] == "in-flight":
            blockers.append({
                "id": rec["id"], "kind": "in-flight",
                "reason": "请求仍在旧副本处理中", "client_gone": rec["client_gone"],
            })
    for p in state.pending.values():
        if p["replica"] == "old":
            blockers.append({
                "id": p["id"], "kind": "unconfirmed",
                "reason": p["reason"], "client_gone": p["rec"]["client_gone"],
                "restarted": p["restarted"],
            })
    return blockers


@app.get("/ops/status")
async def get_status():
    async with state.lock:
        old_drained = state.in_flight["old"] == 0
        old_pending = [p for p in state.pending.values() if p["replica"] == "old"]
        blockers = _old_blockers_locked()
        return {
            "active": state.active,
            "declared_complete": state.declared_complete,
            "in_flight": dict(state.in_flight),
            "unconfirmed": dict(state.unconfirmed),
            "health": dict(state.health),
            "admitted": dict(state.admitted),
            "completed": dict(state.completed),
            "failed": dict(state.failed),
            "old_drained": old_drained,
            "replica_boot_ids": dict(state.replica_boot_ids),
            # 可停机 = 已宣告完成（不会再回滚，旧副本不再需要兜底）
            # 且旧副本既没有在途请求、也没有结局未确认的超时请求
            "safe_to_stop_old": (
                state.declared_complete
                and state.in_flight["old"] == 0
                and not old_pending
            ),
            "old_blockers": blockers,
            "pending": [
                {
                    "id": p["id"], "replica": p["replica"], "reason": p["reason"],
                    "restarted": p["restarted"], "boot_id": p["boot_id"],
                    "client_gone": p["rec"]["client_gone"],
                }
                for p in state.pending.values()
            ],
            "generation": state.generation,
            "replicas": dict(REPLICAS),
            "events": list(state.events)[:60],
            "requests": list(state.requests)[:80],
        }


@app.post("/ops/request/{rid}/ack")
async def acknowledge_request(rid: int):
    """人工确认一个待确认请求的结局（例如副本已重启、现场已人工核对）。

    确认后该请求不再阻止停机；计数上归入失败（网关侧未确认到正常完成），
    事件与请求记录保留完整原因，便于审计。
    """
    async with state.lock:
        p = state.pending.get(rid)
        if not p:
            return JSONResponse(
                {"ok": False, "reason": f"请求 #{rid} 不在待确认列表中"}, status_code=404)
        name = p["replica"]
        state.unconfirmed[name] -= 1
        state.failed[name] += 1
        p["rec"]["status"] = "manually-acknowledged"
        p["rec"]["confirm_reason"] = "人工确认（" + p["reason"] + "）"
        state.pending.pop(rid)
        log_event(f"请求 #{rid} 经人工确认结案（原状态：{p['reason']}），解除对 {name} 的停机阻止")
        return {"ok": True, "msg": f"请求 #{rid} 已人工确认，不再阻止 {name} 停机"}


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
