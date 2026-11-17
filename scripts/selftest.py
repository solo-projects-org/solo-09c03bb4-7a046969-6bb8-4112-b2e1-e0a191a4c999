#!/usr/bin/env python3
"""端到端自测：需先启动三服务(默认编排端 http://localhost:8000)。
覆盖:正常入仓、源站慢(超时/迟到/不重入仓)、等名额、质控慢瓶颈迁移、
源站首次失败、质控失败阻断、NEG/MIX 钩子、多批交错不串记录。"""
import json
import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
FAILS = []


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))
    if not cond:
        FAILS.append(name)


def set_config(c, **kw):
    r = c.post(f"{BASE}/api/config", json=kw, timeout=10)
    r.raise_for_status()
    j = r.json()
    assert j["push"]["source"] == "ok" and j["push"]["qc"] == "ok", f"配置下发失败: {j['push']}"
    return j["config"]


def submit(c, ids, threshold=0, concurrency=0):
    r = c.post(f"{BASE}/api/batches",
               json={"sample_ids": ids, "qc_reject_threshold": threshold,
                     "sample_concurrency": concurrency}, timeout=10)
    r.raise_for_status()
    return r.json()["id"]


def wait_batch(c, bid, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        d = c.get(f"{BASE}/api/batches/{bid}", timeout=10).json()
        if d["batch"]["status"] != "running":
            return d
        time.sleep(0.4)
    raise TimeoutError(f"batch {bid} still running")


def spans_of(d, stage=None, status=None, side=None):
    out = d["spans"]
    if stage:
        out = [s for s in out if s["stage"] == stage]
    if status:
        out = [s for s in out if s["status"] == status]
    if side:
        out = [s for s in out if s["side"] == side]
    return out


def main():
    c = httpx.Client()
    # 0. 服务可达
    for url in (f"{BASE}/api/health",):
        check("编排端健康", c.get(url, timeout=5).json().get("ok") is True)

    # 1. 基线：6 样品全部入仓，关键链存在
    set_config(c, source_delay_ms=300, source_slots=3, source_fail_first=False,
               qc_nonneg_delay_ms=200, qc_nonneg_fail_first=False,
               qc_match_delay_ms=200, qc_match_fail_first=False,
               client_timeout_ms=2000, max_attempts=3)
    ids = [f"T1-{i}" for i in range(6)]
    d = wait_batch(c, submit(c, ids))
    check("基线批次完成", d["batch"]["status"] == "done")
    check("基线全部入仓", len(d["warehouse"]) == 6, f"{len(d['warehouse'])}/6")
    check("基线结论含关键链", bool(d["conclusion"] and d["conclusion"]["chain"]))
    check("基线无迟到/失败", not spans_of(d, status="late") and not spans_of(d, status="failed"))

    # 2. 源站慢(延迟>超时)：出现超时与迟到尝试，迟到不重复入仓
    set_config(c, source_delay_ms=2600, client_timeout_ms=900, max_attempts=3)
    ids = [f"T2-{i}" for i in range(3)]
    d = wait_batch(c, submit(c, ids), timeout=90)
    fetch = spans_of(d, stage="fetch", side="client")
    check("慢源批次仍完成", d["batch"]["status"] == "done")
    check("慢源产生多次尝试", len(fetch) > 3, f"{len(fetch)} 次")
    check("存在迟到/曾被超时放弃的尝试",
          any(s["status"] in ("late", "timeout") or "超时" in (s["note"] or "") for s in fetch))
    check("迟到不重复入仓", len(d["warehouse"]) == 3, f"{len(d['warehouse'])}/3")
    eff = [s for s in fetch if s["status"] == "effective"]
    check("每样品恰一个有效尝试", len(eff) == 3, f"{len(eff)}/3")

    # 3. 等名额：名额=1、8 样品，源站服务端上报 queue_ms>0
    set_config(c, source_delay_ms=300, source_slots=1, client_timeout_ms=5000)
    ids = [f"T3-{i}" for i in range(8)]
    d = wait_batch(c, submit(c, ids), timeout=90)
    srv = spans_of(d, stage="fetch", side="server")
    queued = [s for s in srv if (s.get("queue_ms") or 0) > 50]
    check("等名额批次完成", d["batch"]["status"] == "done")
    check("服务端记录到等名额(queue_ms>50ms)", len(queued) > 0, f"{len(queued)} 条")
    check("瓶颈判为等名额", d["conclusion"]["bottleneck"] == "等名额(源站槽位)",
          str(d["conclusion"]["bottleneck"]))
    set_config(c, source_slots=3)

    # 4. 调慢质控②：瓶颈迁移到质控②
    set_config(c, source_delay_ms=150, qc_match_delay_ms=1500, client_timeout_ms=3000)
    ids = [f"T4-{i}" for i in range(4)]
    d = wait_batch(c, submit(c, ids), timeout=60)
    check("调慢质控②后瓶颈迁移", d["conclusion"]["bottleneck"] == "质控②样品号对应",
          str(d["conclusion"]["bottleneck"]))
    set_config(c, qc_match_delay_ms=200)

    # 5. 源站首次失败：批次完成，存在失败尝试且有效尝试为 #2
    set_config(c, source_fail_first=True)
    ids = [f"T5-{i}" for i in range(3)]
    d = wait_batch(c, submit(c, ids), timeout=60)
    failed_fetch = spans_of(d, stage="fetch", status="failed")
    eff2 = [s for s in spans_of(d, stage="fetch", status="effective") if s["attempt"] == 2]
    check("源站首次失败批次仍完成", d["batch"]["status"] == "done")
    check("记录了失败的首次尝试", len(failed_fetch) >= 3, f"{len(failed_fetch)}")
    check("重试#2成为有效尝试", len(eff2) == 3, f"{len(eff2)}/3")
    set_config(c, source_fail_first=False)

    # 6. 质控①首次失败：批次失败，阻断定位在质控①，不伪造成功链
    set_config(c, qc_nonneg_fail_first=True)
    ids = [f"T6-{i}" for i in range(3)]
    d = wait_batch(c, submit(c, ids), timeout=60)
    check("质控失败批次标记失败", d["batch"]["status"] == "failed")
    check("失败批次无成功链", d["conclusion"]["chain"] is None)
    bl = d["conclusion"]["blocking"] or []
    check("阻断定位在质控①", any(b["stage"] == "qc_nonneg" for b in bl),
          json.dumps(bl, ensure_ascii=False)[:120])
    check("失败批次零入仓", len(d["warehouse"]) == 0)
    set_config(c, qc_nonneg_fail_first=False)

    # 7. 演示钩子：NEG 负浓度 / MIX 错位样品号
    d = wait_batch(c, submit(c, ["HOOK-NEG-1", "HOOK-MIX-1", "HOOK-OK-1"]), timeout=60)
    bl = {b["sample_id"]: b["stage"] for b in (d["conclusion"]["blocking"] or [])}
    check("NEG 阻断于质控①", bl.get("HOOK-NEG-1") == "qc_nonneg", str(bl))
    check("MIX 阻断于质控②", bl.get("HOOK-MIX-1") == "qc_match", str(bl))
    check("正常样品照常入仓", any(w["sample_id"] == "HOOK-OK-1" for w in d["warehouse"]))

    # 8. 多批交错：两批并发，记录不串
    b1 = submit(c, [f"X1-{i}" for i in range(5)])
    b2 = submit(c, [f"X2-{i}" for i in range(5)])
    d1, d2 = wait_batch(c, b1, timeout=60), wait_batch(c, b2, timeout=60)
    check("交错批次均完成", d1["batch"]["status"] == "done" and d2["batch"]["status"] == "done")
    ok1 = all(s["batch_id"] == b1 for s in d1["spans"]) and \
          all(w["batch_id"] == b1 for w in d1["warehouse"])
    ok2 = all(s["batch_id"] == b2 for s in d2["spans"]) and \
          all(w["batch_id"] == b2 for w in d2["warehouse"])
    check("交错批次记录不串", ok1 and ok2)
    pref1 = {w["sample_id"] for w in d1["warehouse"]}
    pref2 = {w["sample_id"] for w in d2["warehouse"]}
    check("入仓样品归属正确", all(x.startswith("X1-") for x in pref1)
          and all(x.startswith("X2-") for x in pref2))

    # 9. 明确拒绝阈值 + 同时处理数=1：达到阈值后停止派发，未启动样品不再启动
    set_config(c, source_delay_ms=150, qc_nonneg_delay_ms=150, qc_match_delay_ms=150,
               client_timeout_ms=3000, max_attempts=3)
    ids = ["TH-OK-1", "TH-NEG-1", "TH-OK-2", "TH-OK-3", "TH-OK-4"]
    bid = submit(c, ids, threshold=1, concurrency=1)
    d = wait_batch(c, bid, timeout=60)
    status = {s["sample_id"]: s["status"] for s in d["samples"]}
    sched = d["scheduling"]
    check("阈值=1 批次停止派发", sched["halted"] == 1, json.dumps(sched, ensure_ascii=False))
    check("明确拒绝计数=1", sched["reject_count"] == 1, str(sched["reject_count"]))
    check("未启动样品标为未启动", status.get("TH-OK-3") == "not_started"
          and status.get("TH-OK-4") == "not_started", str(status))
    check("拒绝样品标为明确拒绝", status.get("TH-NEG-1") == "rejected", str(status))
    check("阈值触发批次不完整成功", d["batch"]["status"] == "failed")
    check("未启动样品无任何取数调用",
          not [s for s in d["spans"] if s["sample_id"] == "TH-OK-3"])
    check("逐样品状态齐全", set(status) == set(ids), str(sorted(status)))
    check("结论含停止派发说明", "停止派发" in (d["conclusion"]["text"] or ""))

    # 10. 调用异常不计入拒绝：BAD 取数失败批次，阈值=1 也不因它停止派发
    ids = ["ERR-BAD-1", "ERR-OK-1", "ERR-OK-2", "ERR-OK-3"]
    bid = submit(c, ids, threshold=1, concurrency=1)
    d = wait_batch(c, bid, timeout=60)
    status = {s["sample_id"]: s["status"] for s in d["samples"]}
    check("取数失败记为调用异常", status.get("ERR-BAD-1") == "call_error", str(status))
    check("调用异常不计入拒绝计数", d["scheduling"]["reject_count"] == 0)
    check("调用异常不触发停止派发", d["scheduling"]["halted"] == 0)
    check("调用异常仍阻断批次完整成功", d["batch"]["status"] == "failed")
    check("其余样品仍照常入仓", len(d["warehouse"]) == 3, f"{len(d['warehouse'])}/3")

    # 11. 同时处理数：并发=2 时在途样品峰值不超过 2
    set_config(c, source_delay_ms=250)
    ids = [f"CC-{i}" for i in range(6)]
    bid = submit(c, ids, threshold=0, concurrency=2)
    peak = 0
    t_end = time.time() + 20
    dd = None
    while time.time() < t_end:
        dd = c.get(f"{BASE}/api/batches/{bid}", timeout=10).json()
        running = sum(1 for s in dd["samples"] if s["status"] == "running")
        peak = max(peak, running)
        if dd["batch"]["status"] != "running":
            break
        time.sleep(0.1)
    check("同时处理数限制在途样品", peak <= 2, f"峰值 {peak}")
    check("并发受限批次仍全部入仓", len(dd["warehouse"]) == 6, f"{len(dd['warehouse'])}/6")

    # 12. 迟到上报不重新启动未派发样品：慢源 + 阈值=1，停止派发后等待迟到跨度补记
    set_config(c, source_delay_ms=900, client_timeout_ms=300, max_attempts=2,
               qc_nonneg_delay_ms=100, qc_match_delay_ms=100)
    ids = ["LATE-NEG-1", "LATE-OK-1", "LATE-OK-2", "LATE-OK-3"]
    bid = submit(c, ids, threshold=1, concurrency=1)
    time.sleep(2.5)  # 等迟到跨度补记
    d = wait_batch(c, bid, timeout=60)
    status = {s["sample_id"]: s["status"] for s in d["samples"]}
    check("慢源下调阈值仍停止派发", d["scheduling"]["halted"] == 1)
    check("迟到补记后未启动样品仍为未启动",
          status.get("LATE-OK-2") == "not_started"
          and status.get("LATE-OK-3") == "not_started", str(status))
    check("迟到补记后无未启动样品取数调用",
          not [s for s in d["spans"] if s["sample_id"] in ("LATE-OK-2", "LATE-OK-3")])
    check("慢源+阈值批次最终定稿", d["batch"]["status"] in ("done", "failed"))
    set_config(c, source_delay_ms=300, client_timeout_ms=2000, max_attempts=3)

    # 13. 阈值批次停止派发不影响其他批次：与另一批并发提交
    b_halt = submit(c, ["ISO-NEG-1", "ISO-OK-1", "ISO-OK-2", "ISO-OK-3"],
                    threshold=1, concurrency=1)
    b_free = submit(c, [f"ISO-FREE-{i}" for i in range(4)], threshold=0)
    dh, df = wait_batch(c, b_halt, timeout=60), wait_batch(c, b_free, timeout=60)
    check("阈值批次停止派发", dh["scheduling"]["halted"] == 1)
    check("同批提交的独立批次不受影响",
          df["batch"]["status"] == "done" and len(df["warehouse"]) == 4,
          f"{df['batch']['status']} {len(df['warehouse'])}/4")

    print()
    if FAILS:
        print(f"共 {len(FAILS)} 项失败: {FAILS}")
        sys.exit(1)
    print("全部自测通过")


if __name__ == "__main__":
    main()
