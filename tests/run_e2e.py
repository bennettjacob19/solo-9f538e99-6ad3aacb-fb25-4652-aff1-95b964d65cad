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
    """等排水 -> 网关复位 -> 两副本恢复健康/默认延迟 -> 等 poller 刷新。"""
    for _ in range(100):
        s = await status(c)
        if s["in_flight"]["old"] == 0 and s["in_flight"]["new"] == 0:
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError("复位前在途请求未清零")
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
    await c.post(GW + "/ops/replica/old/delay?delay_ms=3000")
    slow = asyncio.create_task(c.get(GW + "/api/work"))   # 旧副本 3s 慢请求
    await asyncio.sleep(0.5)
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
    await c.post(GW + "/ops/replica/old/delay?delay_ms=4000")
    base = await status(c)                     # 计数器是累计值，取基线算增量
    base_served = (await c.get(OLD + "/state")).json()["served"]
    t0 = time.monotonic()

    async def impatient_client():
        try:
            await c.get(GW + "/api/work", timeout=0.8)   # 客户端 0.8s 超时断开
        except httpx.ReadTimeout:
            return "timeout"
        return "completed"

    res = await impatient_client()
    assert res == "timeout", "客户端应在 0.8s 超时"
    await asyncio.sleep(max(0, 1.6 - (time.monotonic() - t0)))

    s = await status(c)
    assert s["in_flight"]["old"] == 1, "客户端断连后上游未结束，在途必须仍为 1"
    assert s["completed"]["old"] == base["completed"]["old"], "客户端断连不得计为上游已完成"
    gone = [r for r in s["requests"] if r.get("client_gone")]
    print(f"    [info] 网关标记 client_gone 的记录数: {len(gone)}（取决于 uvicorn 是否取消处理器，两种行为下计数均正确）")

    rold = (await c.get(OLD + "/state")).json()
    assert rold["in_flight"] == 1, "副本侧该请求应仍在执行"

    await asyncio.sleep(max(0, 4.6 - (time.monotonic() - t0)))
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
                if s["admitted"][n] != s["completed"][n] + s["failed"][n] + s["in_flight"][n]:
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
        "path": "/api/work", "raw_path": b"/api/work", "query_string": b"delay_ms=2000",
        "headers": [], "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 18000),
    }
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    f0 = gw.state.in_flight["old"]
    c0 = gw.state.completed["old"]
    e0 = gw.state.failed["old"]
    handler = asyncio.create_task(gw.proxy("work", Request(scope, receive)))
    await asyncio.sleep(0.4)
    assert gw.state.in_flight["old"] == f0 + 1, "请求应已受理并在途"

    handler.cancel()           # 模拟服务器因客户端断连取消处理器协程
    await asyncio.sleep(0.2)
    assert handler.cancelled(), "处理器应已被取消"
    assert gw.state.in_flight["old"] == f0 + 1, "处理器被取消后上游未结束，在途必须仍为 1"
    assert gw.state.completed["old"] == c0 and gw.state.failed["old"] == e0, \
        "处理器被取消不得计为完成或失败"

    await asyncio.sleep(2.2)   # 上游 2s 请求真正结束
    assert gw.state.in_flight["old"] == f0, "上游真正结束后在途才清零"
    assert gw.state.completed["old"] == c0 + 1, "上游真正结束后才计入完成"
    assert gw.state.failed["old"] == e0
    rec = gw.state.requests[0]
    assert rec["client_gone"] is True and rec["status"] == "done", \
        f"记录应标记客户端已走且上游真正完成: {rec}"
    await gw.client.aclose()


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
]


async def main():
    random.seed(20261118)
    spawn("old", os.path.join(ROOT, "replica"), 18001,
          {"REPLICA_ID": "old", "DEFAULT_DELAY_MS": "50"})
    spawn("new", os.path.join(ROOT, "replica"), 18002,
          {"REPLICA_ID": "new", "DEFAULT_DELAY_MS": "50"})
    spawn("gw", os.path.join(ROOT, "gateway"), 18000,
          {"OLD_URL": OLD, "NEW_URL": NEW, "POLL_INTERVAL": "0.3"})

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
