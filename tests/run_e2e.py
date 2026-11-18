#!/usr/bin/env python3
"""端到端自测：真实启动 网关 + 新旧副本 三个进程，验证切换/排水/断连/回滚/并发交错。

运行：python3 tests/run_e2e.py
退出码 0 = 全部通过。
"""
import asyncio
import os
import random
import subprocess
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GW, OLD, NEW = "http://127.0.0.1:18000", "http://127.0.0.1:18001", "http://127.0.0.1:18002"

PROCS = []


def spawn(name, app_dir, port, env):
    logf = open(f"/tmp/e2e_{name}.log", "w")
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=app_dir, env={**os.environ, **env}, stdout=logf, stderr=subprocess.STDOUT)
    PROCS.append(p)
    return p


def kill_proc(p):
    p.terminate()
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
    PROCS.remove(p)


async def wait_url(c, url, want=200, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if (await c.get(url)).status_code == want:
                return
        except Exception:
            pass
        await asyncio.sleep(0.2)
    raise RuntimeError(f"服务未就绪: {url}（见 /tmp/e2e_*.log）")


def cleanup():
    for p in PROCS:
        p.terminate()
    for p in PROCS:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


async def wait_ready(c: httpx.AsyncClient):
    deadline = time.monotonic() + 25
    for url in (OLD + "/state", NEW + "/state", GW + "/ops/status"):
        while True:
            try:
                r = await c.get(url)
                if r.status_code == 200:
                    break
            except Exception:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError(f"服务未就绪: {url}（见 /tmp/e2e_*.log）")
            await asyncio.sleep(0.2)


async def status(c):
    return (await c.get(GW + "/ops/status")).json()


async def reset_all(c):
    """等排水/待确认清零 -> 网关复位 -> 两副本恢复健康/默认延迟 -> 等 poller 刷新。"""
    for _ in range(150):
        s = await status(c)
        if (s["in_flight"]["old"] == 0 and s["in_flight"]["new"] == 0
                and s.get("unconfirmed", {"old": 0, "new": 0})["old"] == 0
                and s.get("unconfirmed", {"old": 0, "new": 0})["new"] == 0):
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError("复位前在途/待确认请求未清零")
    # 兜底：仍残留的待确认请求（如副本重启无法自动核对）人工确认结案
    s = await status(c)
    for p in s.get("pending", []):
        r = await c.post(GW + f"/ops/request/{p['id']}/ack")
        assert r.status_code == 200, f"人工确认失败: {r.text}"
    r = await c.post(GW + "/ops/reset")
    assert r.status_code == 200, f"reset 失败: {r.text}"
    for name in ("old", "new"):
        await c.post(GW + f"/ops/replica/{name}/health?healthy=true")
        await c.post(GW + f"/ops/replica/{name}/delay?delay_ms=50")
    await asyncio.sleep(0.8)


# ---------------------------------------------------------------- 场景

async def s1_basic_routing(c):
    """S1 初始状态：请求只进旧副本；操作网页可访问。"""
    await reset_all(c)
    r = await c.get(GW + "/")
    assert r.status_code == 200 and "流量切换控制台" in r.text, "操作网页应可访问"
    r = await c.get(GW + "/api/work")
    assert r.status_code == 200 and r.json()["replica"] == "old", f"应路由到 old: {r.text}"
    s = await status(c)
    assert s["active"] == "old" and s["admitted"]["old"] >= 1


async def s2_switch_refused_when_new_down(c):
    """S2 新副本未就绪：拒绝切换，旧副本继续接单。"""
    await reset_all(c)
    await c.post(GW + "/ops/replica/new/health?healthy=false")
    await asyncio.sleep(0.7)
    r = await c.post(GW + "/ops/switch")
    assert r.status_code == 409, f"新副本失效时切换应被拒绝: {r.status_code}"
    r = await c.get(GW + "/api/work")
    assert r.json()["replica"] == "old", "拒绝切换后旧副本应继续接单"
    s = await status(c)
    assert s["active"] == "old" and s["health"]["new"] is False


async def s3_switch_when_ready(c):
    """S3 新副本就绪后切换成功，新请求只进新副本。"""
    await reset_all(c)
    r = await c.post(GW + "/ops/switch")
    assert r.status_code == 200, f"就绪后切换应成功: {r.text}"
    for _ in range(3):
        r = await c.get(GW + "/api/work")
        assert r.json()["replica"] == "new", "切换后新请求应只进新副本"
    s = await status(c)
    assert s["active"] == "new" and s["safe_to_stop_old"] is False  # 未宣告完成


async def s4_drain_then_safe_to_stop(c):
    """S4 旧副本慢请求真正结束前，即使已宣告完成也不显示可停机。"""
    await reset_all(c)
    await c.post(GW + "/ops/replica/old/delay?delay_ms=1500")
    slow = asyncio.create_task(c.get(GW + "/api/work"))   # 旧副本 1.5s 慢请求（低于网关 2s 超时）
    await asyncio.sleep(0.4)
    s = await status(c)
    assert s["in_flight"]["old"] == 1, "慢请求应在旧副本在途"

    r = await c.post(GW + "/ops/switch")
    assert r.status_code == 200
    r = await c.get(GW + "/api/work?delay_ms=10")
    assert r.json()["replica"] == "new", "切换后新请求只进新副本"

    r = await c.post(GW + "/ops/declare-complete")
    assert r.status_code == 200, f"宣告完成应成功: {r.text}"
    s = await status(c)
    assert s["declared_complete"] is True
    assert s["safe_to_stop_old"] is False, "旧副本在途未清零，不得显示可停机"

    resp = await slow
    assert resp.status_code == 200 and resp.json()["replica"] == "old"
    await asyncio.sleep(0.3)
    s = await status(c)
    assert s["in_flight"]["old"] == 0 and s["safe_to_stop_old"] is True, \
        "旧副本在途真正清零后才可显示可停机"


async def s5_auto_rollback(c):
    """S5 宣告完成前新副本失效：自动回滚，旧副本重新接单。"""
    await reset_all(c)
    r = await c.post(GW + "/ops/switch")
    assert r.status_code == 200
    await c.post(GW + "/ops/replica/new/health?healthy=false")
    await asyncio.sleep(1.2)   # poller 0.3s，足够发现
    s = await status(c)
    assert s["active"] == "old", f"新副本失效应自动回滚: active={s['active']}"
    assert any("自动回滚" in e["msg"] for e in s["events"]), "事件日志应记录自动回滚"
    r = await c.get(GW + "/api/work")
    assert r.json()["replica"] == "old", "回滚后旧副本应重新接单"


async def s6_declare_disables_rollback(c):
    """S6 宣告完成后新副本失效不再回滚。"""
    await reset_all(c)
    assert (await c.post(GW + "/ops/switch")).status_code == 200
    assert (await c.post(GW + "/ops/declare-complete")).status_code == 200
    await c.post(GW + "/ops/replica/new/health?healthy=false")
    await asyncio.sleep(1.2)
    s = await status(c)
    assert s["active"] == "new", "宣告完成后不得自动回滚"
    assert not any("自动回滚" in e["msg"] for e in s["events"]), "宣告完成后不应出现自动回滚事件"


async def s7_client_disconnect_not_upstream_done(c):
    """S7 客户端超时断连 ≠ 上游结束：在途计数保持到上游真正完成。"""
    await reset_all(c)
    await c.post(GW + "/ops/replica/old/delay?delay_ms=1500")
    base = await status(c)                     # 计数器是累计值，取基线算增量
    base_served = (await c.get(OLD + "/state")).json()["served"]
    t0 = time.monotonic()

    async def impatient_client():
        try:
            await c.get(GW + "/api/work", timeout=0.5)   # 客户端 0.5s 超时断开
        except httpx.ReadTimeout:
            return "timeout"
        return "completed"

    res = await impatient_client()
    assert res == "timeout", "客户端应在 0.5s 超时"
    await asyncio.sleep(max(0, 1.0 - (time.monotonic() - t0)))

    s = await status(c)
    assert s["in_flight"]["old"] == 1, "客户端断连后上游未结束，在途必须仍为 1"
    assert s["completed"]["old"] == base["completed"]["old"], "客户端断连不得计为上游已完成"
    assert s["unconfirmed"]["old"] == 0, "网关自身未超时，不应进入待确认"
    gone = [r for r in s["requests"] if r.get("client_gone")]
    print(f"    [info] 网关标记 client_gone 的记录数: {len(gone)}（取决于 uvicorn 是否取消处理器，两种行为下计数均正确）")

    rold = (await c.get(OLD + "/state")).json()
    assert rold["in_flight"] == 1, "副本侧该请求应仍在执行"

    await asyncio.sleep(max(0, 1.9 - (time.monotonic() - t0)))
    s = await status(c)
    assert s["in_flight"]["old"] == 0, "上游真正结束后在途才允许清零"
    assert s["completed"]["old"] == base["completed"]["old"] + 1, "上游真正结束后才计入完成"
    rold = (await c.get(OLD + "/state")).json()
    assert rold["served"] == base_served + 1, "副本应真正完成了该请求"


async def s8_concurrent_interleaving(c):
    """S8 并发请求 × 健康抖动 × 切换/回滚交错：不重计、不漏计、恰好一次送达。"""
    await reset_all(c)
    base = await status(c)                     # 累计计数器的基线
    base_recv = {
        "old": (await c.get(OLD + "/state")).json()["received"],
        "new": (await c.get(NEW + "/state")).json()["received"],
    }
    base_served = {
        "old": (await c.get(OLD + "/state")).json()["served"],
        "new": (await c.get(NEW + "/state")).json()["served"],
    }
    N = 40
    stop = asyncio.Event()
    violations = []

    async def flapper():
        while not stop.is_set():
            await c.post(GW + f"/ops/replica/new/health?healthy={random.choice(['true','false'])}")
            await asyncio.sleep(random.uniform(0.1, 0.35))

    async def switcher():
        while not stop.is_set():
            if random.random() < 0.5:
                await c.post(GW + "/ops/switch")
            else:
                await c.post(GW + "/ops/rollback")
            await asyncio.sleep(random.uniform(0.2, 0.5))

    async def sampler():
        while not stop.is_set():
            s = await status(c)
            for n in ("old", "new"):
                if s["in_flight"][n] < 0:
                    violations.append(f"{n} in_flight 为负: {s['in_flight'][n]}")
                if s["admitted"][n] != (s["completed"][n] + s["failed"][n]
                                        + s["in_flight"][n] + s["unconfirmed"][n]):
                    violations.append(f"{n} 计数不变式被破坏: {s}")
            await asyncio.sleep(0.08)

    async def one_request(i):
        await asyncio.sleep(random.uniform(0, 0.8))
        r = await c.get(GW + f"/api/work?delay_ms={random.randint(30, 250)}")
        return r.status_code

    tasks_bg = [asyncio.create_task(f()) for f in (flapper, switcher, sampler)]
    codes = await asyncio.gather(*(one_request(i) for i in range(N)))
    stop.set()
    await asyncio.gather(*tasks_bg)
    await c.post(GW + "/ops/replica/new/health?healthy=true")
    await asyncio.sleep(1.0)

    assert not violations, f"运行期间计数不变式被破坏: {violations[:3]}"
    assert all(code == 200 for code in codes), f"所有请求应成功: {codes}"

    s = await status(c)
    assert s["in_flight"] == {"old": 0, "new": 0}, f"结束后在途应为 0: {s['in_flight']}"
    delta = {n: {k: s[k][n] - base[k][n] for k in ("admitted", "completed", "failed")} for n in ("old", "new")}
    for n in ("old", "new"):
        assert delta[n]["failed"] == 0, f"{n} 不应有上游错误"
        assert delta[n]["admitted"] == delta[n]["completed"], f"{n} 受理必须等于完成（不漏计）: {delta[n]}"
    total = delta["old"]["admitted"] + delta["new"]["admitted"]
    assert total == N, f"受理总数应等于发出的请求数 {N}: {total}（不重计不漏计）"

    rold = (await c.get(OLD + "/state")).json()
    rnew = (await c.get(NEW + "/state")).json()
    recv_old = rold["received"] - base_recv["old"]
    recv_new = rnew["received"] - base_recv["new"]
    assert recv_old == delta["old"]["admitted"], f"old 实收 {recv_old} 必须等于网关受理 {delta['old']['admitted']}（恰好一次送达）"
    assert recv_new == delta["new"]["admitted"], f"new 实收 {recv_new} 必须等于网关受理 {delta['new']['admitted']}（恰好一次送达）"
    assert rold["served"] - base_served["old"] == recv_old, "old 实际完成数必须等于实收数"
    assert rnew["served"] - base_served["new"] == recv_new, "new 实际完成数必须等于实收数"
    ids = rold["received_ids"][-recv_old:] + rnew["received_ids"][-recv_new:]
    assert len(ids) == N and len(set(ids)) == N, "每个请求必须恰好送达一次（无重复无遗漏）"
    print(f"    [info] 交错期间切换代数 generation={s['generation']}, "
          f"old/new 各受理 {delta['old']['admitted']}/{delta['new']['admitted']}")


async def s9_declare_gating(c):
    """S9 宣告完成的门槛：未切换不能宣告；宣告可幂等重复。"""
    await reset_all(c)
    r = await c.post(GW + "/ops/declare-complete")
    assert r.status_code == 409, "未切换到新副本时不得宣告完成"
    assert (await c.post(GW + "/ops/switch")).status_code == 200
    assert (await c.post(GW + "/ops/declare-complete")).status_code == 200
    assert (await c.post(GW + "/ops/declare-complete")).status_code == 200, "重复宣告应幂等"
    s = await status(c)
    assert s["declared_complete"] is True and s["safe_to_stop_old"] is True


async def s10_handler_cancelled_midflight(c):
    """S10 处理器协程被取消（某些服务器在客户端断连时的行为）：
    shield 保护的上游任务必须继续到真正结束，计数只在那时扣减。"""
    os.environ["OLD_URL"] = OLD
    os.environ["NEW_URL"] = NEW
    sys.path.insert(0, os.path.join(ROOT, "gateway"))
    import main as gw          # 在测试进程内加载网关模块，直接驱动其处理器
    from starlette.requests import Request
    gw.client = httpx.AsyncClient()

    scope = {
        "type": "http", "http_version": "1.1", "method": "GET", "scheme": "http",
        "path": "/api/work", "raw_path": b"/api/work", "query_string": b"delay_ms=800",
        "headers": [], "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 18000),
    }
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    f0 = gw.state.in_flight["old"]
    c0 = gw.state.completed["old"]
    e0 = gw.state.failed["old"]
    handler = asyncio.create_task(gw.proxy("work", Request(scope, receive)))
    await asyncio.sleep(0.3)
    assert gw.state.in_flight["old"] == f0 + 1, "请求应已受理并在途"

    handler.cancel()           # 模拟服务器因客户端断连取消处理器协程
    await asyncio.sleep(0.2)
    assert handler.cancelled(), "处理器应已被取消"
    assert gw.state.in_flight["old"] == f0 + 1, "处理器被取消后上游未结束，在途必须仍为 1"
    assert gw.state.completed["old"] == c0 and gw.state.failed["old"] == e0, \
        "处理器被取消不得计为完成或失败"

    await asyncio.sleep(1.0)   # 上游 0.8s 请求真正结束
    assert gw.state.in_flight["old"] == f0, "上游真正结束后在途才清零"
    assert gw.state.completed["old"] == c0 + 1, "上游真正结束后才计入完成"
    assert gw.state.failed["old"] == e0
    rec = gw.state.requests[0]
    assert rec["client_gone"] is True and rec["status"] == "done", \
        f"记录应标记客户端已走且上游真正完成: {rec}"
    await gw.client.aclose()


async def s11_upstream_timeout_blocks_stop(c):
    """S11 网关 UPSTREAM_TIMEOUT（本测试 2s）：客户端及时收 502，但旧副本仍在处理；
    请求转待确认持续阻止停机，副本核对到 served 后自动解除。"""
    await reset_all(c)
    await c.post(GW + "/ops/replica/old/delay?delay_ms=3000")
    base = await status(c)
    base_served = (await c.get(OLD + "/state")).json()["served"]

    t0 = time.monotonic()
    r = await c.get(GW + "/api/work")     # 网关 2s 超时，副本实际要 3s
    dt = time.monotonic() - t0
    assert r.status_code == 502, f"网关超时客户端应收到 502: {r.status_code}"
    assert 1.8 < dt < 2.6, f"客户端应在网关超时时点及时收到错误，实际 {dt:.2f}s"
    body = r.json()
    assert "timeout" in body["error"] and body["request_id"]
    rid = body["request_id"]

    s = await status(c)
    assert s["in_flight"]["old"] == 0, "超时后网关侧调用已结束，in_flight 应清零"
    assert s["unconfirmed"]["old"] == 1, "超时请求必须转为待确认"
    assert s["failed"]["old"] == base["failed"]["old"], "超时不得直接计为失败"
    assert s["completed"]["old"] == base["completed"]["old"], "超时不得计为完成"
    assert any(p["id"] == rid for p in s["pending"]), "待确认列表应含该请求"
    rold = (await c.get(OLD + "/state")).json()
    assert rold["in_flight"] == 1 and rold["served"] == base_served, "副本侧请求应仍在处理"

    # 切换 + 宣告完成：待确认未解除前绝不可停机，网页受阻明细可见
    assert (await c.post(GW + "/ops/switch")).status_code == 200
    assert (await c.post(GW + "/ops/declare-complete")).status_code == 200
    s = await status(c)
    assert s["safe_to_stop_old"] is False, "存在超时未确认请求时不得显示可停机"
    blockers = [b for b in s["old_blockers"] if b["id"] == rid]
    assert blockers and blockers[0]["kind"] == "unconfirmed", "网页应能查看停机受阻原因"

    # 等核对器向副本确认（CONFIRM_POLL_INTERVAL=0.2）；副本 3s 才真正处理完
    await asyncio.sleep(max(0, 3.6 - (time.monotonic() - t0)))
    s = await status(c)
    assert s["unconfirmed"]["old"] == 0 and not s["pending"], "副本确认 served 后应自动解除待确认"
    assert s["completed"]["old"] == base["completed"]["old"] + 1, "确认后才计入完成"
    assert s["safe_to_stop_old"] is True, "在途与待确认均清零后才可停机"
    rold = (await c.get(OLD + "/state")).json()
    assert rold["served"] == base_served + 1, "副本应真正处理完成了该请求"
    rec = next(r for r in s["requests"] if r["id"] == rid)
    # 超时拿不到上游 HTTP 状态（http_status=None），502 是网关合成给客户端的（前面已断言）
    assert rec["status"] == "confirmed-done" and rec["http_status"] is None, \
        f"记录应状态为副本确认完成且保留无上游状态码的事实: {rec}"


async def s12_unreachable_and_restart_unconfirmed(c):
    """S12 待确认期间副本暂不可查 / 重启后查无原请求：一律保持未确认，
    新进程空闲不得误判为原请求已结束；人工确认后才解除阻止。"""
    await reset_all(c)
    await c.post(GW + "/ops/replica/old/delay?delay_ms=3000")
    base = await status(c)

    r = await c.get(GW + "/api/work")
    assert r.status_code == 502
    rid = r.json()["request_id"]

    # --- 12a 副本进程宕机（不可查）：保持未确认，不自动结案 ---
    old_proc = next(p for p in PROCS if any("18001" in a for a in p.args))
    kill_proc(old_proc)
    await asyncio.sleep(1.2)     # 等若干轮核对，全部查询失败
    s = await status(c)
    assert s["unconfirmed"]["old"] == 1, "副本暂不可查时必须保持未确认"
    p = next(p for p in s["pending"] if p["id"] == rid)
    assert "暂不可查" in p["reason"], f"应标明暂不可查: {p['reason']}"

    # --- 12b 副本以新进程重启（boot_id 变化），查无原请求：仍未确认 ---
    old2 = spawn("old2", os.path.join(ROOT, "replica"), 18001,
                 {"REPLICA_ID": "old", "DEFAULT_DELAY_MS": "50"})
    await wait_url(c, OLD + "/state")
    new_boot = (await c.get(OLD + "/state")).json()["boot_id"]
    await asyncio.sleep(1.0)     # 核对器发现 boot_id 不一致并标记
    s = await status(c)
    assert s["unconfirmed"]["old"] == 1, "新进程空闲不等于原请求结束，必须仍未确认"
    p = next(p for p in s["pending"] if p["id"] == rid)
    assert p["restarted"] is True and "重启" in p["reason"], f"应标记重启未确认: {p}"
    assert s["safe_to_stop_old"] is False
    rec = next(r for r in s["requests"] if r["id"] == rid)
    assert rec["status"] == "replica-restarted"
    # 新进程上跑一个新请求，验证不会被误关联到旧请求
    r = await c.get(GW + "/api/work?delay_ms=10")
    assert r.status_code == 200
    s = await status(c)
    assert any(p["id"] == rid for p in s["pending"]), "新进程的空闲/新请求不得结案旧请求"

    # --- 12c 人工确认后才解除阻止，计数归入失败并保留审计原因 ---
    r = await c.post(GW + f"/ops/request/{rid}/ack")
    assert r.status_code == 200, r.text
    s = await status(c)
    assert s["unconfirmed"]["old"] == 0
    assert s["failed"]["old"] == base["failed"]["old"] + 1, "人工确认归入失败计数"
    assert s["admitted"]["old"] == (s["completed"]["old"] + s["failed"]["old"]
                                    + s["in_flight"]["old"] + s["unconfirmed"]["old"])
    rec = next(r for r in s["requests"] if r["id"] == rid)
    assert rec["status"] == "manually-acknowledged"
    # 未知 ID 的人工确认应 404
    assert (await c.post(GW + "/ops/request/999999/ack")).status_code == 404


async def s13_timeout_interleaved_with_switch(c):
    """S13 超时请求与正常请求、切换/回滚交错：去向与计数准确，
    待确认始终挂在实际受理它的副本上，不变式全程成立。"""
    await reset_all(c)
    # 只让旧副本慢：落到 old 的请求会超时待确认，落到 new 的正常返回
    await c.post(GW + "/ops/replica/old/delay?delay_ms=3000")
    base = await status(c)
    base_recv = {
        "old": (await c.get(OLD + "/state")).json()["received"],
        "new": (await c.get(NEW + "/state")).json()["received"],
    }
    violations = []

    async def sampler():
        end = time.monotonic() + 6
        while time.monotonic() < end:
            s = await status(c)
            for n in ("old", "new"):
                if s["admitted"][n] != (s["completed"][n] + s["failed"][n]
                                        + s["in_flight"][n] + s["unconfirmed"][n]):
                    violations.append(f"{n} 不变式破坏: {s}")
            await asyncio.sleep(0.05)

    st = asyncio.create_task(sampler())
    timed_out, normal = [], []

    async def one(i):
        try:
            r = await c.get(GW + f"/api/work?delay_ms=3000")
        except httpx.HTTPStatusError:
            raise
        if r.status_code == 502:
            timed_out.append((i, r.json()["request_id"]))
        else:
            normal.append((i, r.json()["replica"]))

    reqs = [asyncio.create_task(one(i)) for i in range(8)]
    # 交错切换/回滚（new 始终健康，操作允许）
    await asyncio.sleep(0.3)
    await c.post(GW + "/ops/switch")
    await asyncio.sleep(0.4)
    await c.post(GW + "/ops/rollback")
    await asyncio.sleep(0.4)
    await c.post(GW + "/ops/switch")
    results = await asyncio.gather(*reqs, return_exceptions=True)
    assert all(not isinstance(x, BaseException) for x in results), f"不应有连接级异常: {results}"

    s = await status(c)
    # 所有 502 必然是 old 受理的慢请求；所有 200 按当时活动副本路由
    assert timed_out, "应至少有一个超时请求"
    assert all(any(r["id"] == rid and r["replica"] == "old" for r in s["requests"])
               for _, rid in timed_out), "超时请求必须挂在实际受理它的 old 上"
    assert all(rep == "new" for _, rep in normal), "切换后成功的请求应都在 new"
    assert s["unconfirmed"]["new"] == 0, "new 没有慢请求，不应有待确认"
    assert s["unconfirmed"]["old"] == len(timed_out), \
        f"old 待确认数 {s['unconfirmed']['old']} 应等于超时数 {len(timed_out)}"
    await st
    assert not violations, f"交错期间不变式被破坏: {violations[:2]}"

    # 等旧副本全部处理完并被核对
    await asyncio.sleep(3.5)
    s = await status(c)
    assert s["unconfirmed"]["old"] == 0 and s["in_flight"]["old"] == 0
    delta = {n: {k: s[k][n] - base[k][n] for k in ("admitted", "completed", "failed", "unconfirmed")}
             for n in ("old", "new")}
    assert delta["old"]["admitted"] == len(timed_out) and delta["old"]["failed"] == 0
    assert delta["old"]["completed"] == len(timed_out), "超时请求经副本核对后全部计入完成"
    assert delta["new"]["admitted"] == len(normal) and delta["new"]["completed"] == len(normal)
    assert delta["old"]["admitted"] + delta["new"]["admitted"] == 8
    rold = (await c.get(OLD + "/state")).json()
    rnew = (await c.get(NEW + "/state")).json()
    assert rold["received"] - base_recv["old"] == len(timed_out), "old 实收数应与超时数一致"
    assert rnew["received"] - base_recv["new"] == len(normal), "new 实收数应与正常请求数一致"


SCENARIOS = [
    ("S1 初始路由到旧副本", s1_basic_routing),
    ("S2 新副本未就绪拒绝切换、旧副本继续接单", s2_switch_refused_when_new_down),
    ("S3 就绪后切换、新请求只进新副本", s3_switch_when_ready),
    ("S4 旧副本在途真正清零前不显示可停机", s4_drain_then_safe_to_stop),
    ("S5 宣告完成前新副本失效自动回滚", s5_auto_rollback),
    ("S6 宣告完成后不再回滚", s6_declare_disables_rollback),
    ("S7 客户端断连不等于上游结束", s7_client_disconnect_not_upstream_done),
    ("S8 并发×健康抖动×切换交错下不重计不漏计", s8_concurrent_interleaving),
    ("S9 宣告完成门槛与幂等", s9_declare_gating),
    ("S10 处理器被取消时上游仍真正结束才扣减", s10_handler_cancelled_midflight),
    ("S11 网关超时后待确认持续阻止停机、副本确认后解除", s11_upstream_timeout_blocks_stop),
    ("S12 副本不可查/重启后保持未确认，人工确认才结案", s12_unreachable_and_restart_unconfirmed),
    ("S13 超时请求与正常请求、切换回滚交错下计数准确", s13_timeout_interleaved_with_switch),
]


async def main():
    random.seed(20261118)
    spawn("old", os.path.join(ROOT, "replica"), 18001,
          {"REPLICA_ID": "old", "DEFAULT_DELAY_MS": "50"})
    spawn("new", os.path.join(ROOT, "replica"), 18002,
          {"REPLICA_ID": "new", "DEFAULT_DELAY_MS": "50"})
    spawn("gw", os.path.join(ROOT, "gateway"), 18000,
          {"OLD_URL": OLD, "NEW_URL": NEW, "POLL_INTERVAL": "0.3",
           "UPSTREAM_TIMEOUT": "2", "CONFIRM_POLL_INTERVAL": "0.2"})

    failed = []
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            await wait_ready(c)
            print("服务已就绪，开始场景测试\n")
            for name, fn in SCENARIOS:
                t0 = time.monotonic()
                try:
                    await fn(c)
                    print(f"PASS  {name}  ({time.monotonic()-t0:.1f}s)")
                except Exception as e:
                    failed.append(name)
                    print(f"FAIL  {name}\n      {type(e).__name__}: {e}")
    finally:
        cleanup()

    print()
    if failed:
        print(f"共 {len(failed)}/{len(SCENARIOS)} 个场景失败，日志见 /tmp/e2e_*.log")
        return 1
    print(f"全部 {len(SCENARIOS)} 个场景通过")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
