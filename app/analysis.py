"""批次结论分析：关键链(决定完成时间的等待与执行)、瓶颈判定、失败阻断定位。
所有结论只依据 spans(真实调用) 与 warehouse(真实入仓记录)。"""
import json

import db

STAGE_LABELS = {
    "fetch": "取数",
    "qc_nonneg": "质控①非负值",
    "qc_match": "质控②样品号对应",
    "warehouse": "入仓",
}


def _f(x):
    return round(float(x), 3)


def _err_of(span):
    d = span.get("detail")
    if d:
        try:
            j = json.loads(d)
            if isinstance(j, dict):
                if j.get("error"):
                    return str(j["error"])
                if j.get("status_code"):
                    return f"HTTP {j['status_code']}"
                if j.get("response") is not None:
                    return json.dumps(j["response"], ensure_ascii=False)
        except Exception:
            pass
    return span["status"]


def analyze(batch_id):
    batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not batch:
        return {"status": "failed", "text": "批次不存在"}
    sample_ids = list(dict.fromkeys(json.loads(batch["sample_ids"])))
    spans = db.query("SELECT * FROM spans WHERE batch_id=? ORDER BY start_ts,id", (batch_id,))
    wh = db.query("SELECT * FROM warehouse WHERE batch_id=?", (batch_id,))
    client = [s for s in spans if s["side"] == "client"]
    server_by_req = {s["request_id"]: s for s in spans if s["side"] == "server"}

    t0 = min([s["start_ts"] for s in spans], default=batch["created_at"])
    ends = [s["end_ts"] for s in spans if s["end_ts"]] + [w["stored_at"] for w in wh]
    t1 = max(ends) if ends else t0
    wh_by_sample = {w["sample_id"]: w for w in wh}
    done = bool(sample_ids) and all(s in wh_by_sample for s in sample_ids)

    # 各阶段统计：sum(各次耗时求和) 与 wall(墙钟跨度) 分开，并行/重叠不相加
    per_stage = {}
    for st in ("fetch", "qc_nonneg", "qc_match", "warehouse"):
        ss = [s for s in client if s["stage"] == st and s["end_ts"] is not None]
        if not ss:
            continue
        per_stage[st] = {
            "label": STAGE_LABELS[st],
            "attempts": len(ss),
            "sum_s": _f(sum(s["end_ts"] - s["start_ts"] for s in ss)),
            "wall_s": _f(max(s["end_ts"] for s in ss) - min(s["start_ts"] for s in ss)),
            "effective": sum(1 for s in ss if s["status"] == "effective"),
            "ok": sum(1 for s in ss if s["status"] == "ok"),
            "failed": sum(1 for s in ss if s["status"] == "failed"),
            "timeout": sum(1 for s in ss if s["status"] == "timeout"),
            "late": sum(1 for s in ss if s["status"] == "late"),
            # 超时放弃是经历而非最终状态(之后会转为有效/迟到/失败)，从备注中统计
            "gave_up": sum(1 for s in ss if "超时" in (s["note"] or "")),
        }

    # 工作量归因：等名额(源站服务端上报的槽位等待) / 源站执行 / 两项质控 / 入仓
    eff_fetch = [s for s in client if s["stage"] == "fetch" and s["status"] == "effective"]
    slot_wait = sum((server_by_req.get(s["request_id"]) or {}).get("queue_ms") or 0
                    for s in eff_fetch) / 1000.0
    fetch_sum = sum(s["end_ts"] - s["start_ts"] for s in eff_fetch)
    parts = {
        "等名额(源站槽位)": slot_wait,
        "源站取数执行": max(fetch_sum - slot_wait, 0.0),
        "质控①非负值": per_stage.get("qc_nonneg", {}).get("sum_s", 0.0),
        "质控②样品号对应": per_stage.get("qc_match", {}).get("sum_s", 0.0),
        "入仓": per_stage.get("warehouse", {}).get("sum_s", 0.0),
    }
    bottleneck = max(parts, key=lambda k: parts[k]) if parts else None

    result = {
        "status": "done" if done else "failed",
        "t0": t0, "t1": t1, "wall_s": _f(t1 - t0),
        "samples_total": len(sample_ids),
        "samples_stored": len(wh_by_sample),
        "warehouse_rows": len(wh),
        "client_calls": len(client),
        "per_stage": per_stage,
        "work_parts_s": {k: _f(v) for k, v in parts.items()},
        "bottleneck": bottleneck,
        "chain": None,
        "blocking": None,
    }

    # 调度侧事实：明确拒绝次数、阈值、是否已停止派发、逐样品最终状态
    st = db.get_batch_state(batch_id) or {}
    results = {r["sample_id"]: r for r in db.get_sample_results(batch_id)}

    def _derive(sid):
        """旧批次(无逐样品记录)从真实跨度推导状态，绝不凭空标记。"""
        if sid in wh_by_sample:
            return "stored"
        sps = [s for s in client if s["sample_id"] == sid]
        if not sps:
            return "not_started"
        rejects = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match")
                   and s["status"] == "failed" and "明确判定未通过" in (s["note"] or "")]
        if rejects:
            return "rejected"
        return "call_error"  # 有调用记录但未入仓：取数失败或质控异常/入仓未完成
    counts = {"stored": 0, "rejected": 0, "call_error": 0, "not_started": 0, "running": 0}
    for sid in sample_ids:
        row = results.get(sid)
        s = row["status"] if row else _derive(sid)
        counts[s] = counts.get(s, 0) + 1
    result["sample_counts"] = counts
    result["samples_rejected"] = counts.get("rejected", 0)
    result["samples_call_error"] = counts.get("call_error", 0)
    result["samples_not_started"] = counts.get("not_started", 0)
    result["scheduling"] = {
        "qc_reject_threshold": st.get("qc_reject_threshold", 0),
        "sample_concurrency": st.get("sample_concurrency", 0),
        "reject_count": st.get("reject_count", 0),
        "halted": bool(st.get("halted")),
        "halted_at": st.get("halted_at"),
        "halt_reason": st.get("halt_reason", ""),
    }
    halt_note = ""
    if st.get("halted"):
        halt_note = (f"批次已达明确质控拒绝阈值 {st.get('qc_reject_threshold')}"
                     f"(明确拒绝 {st.get('reject_count')} 个)，已停止派发尚未开始的样品；"
                     f"在途样品仍完成质控与入仓。")

    if done:
        # 关键链：最后完成入仓的样品决定了批次完成时间
        last_w = max(wh, key=lambda w: w["stored_at"])
        sid = last_w["sample_id"]
        sps = [s for s in client if s["sample_id"] == sid]
        fe = next((s for s in sps if s["stage"] == "fetch" and s["status"] == "effective"), None)
        wh_span = next((s for s in sps if s["stage"] == "warehouse" and s["end_ts"]), None)
        qcs = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match") and s["end_ts"]]
        segs = []
        if fe and wh_span:
            srv = server_by_req.get(fe["request_id"])
            note = ""
            if srv and srv.get("queue_ms"):
                note = f"含等名额 {srv['queue_ms']:.0f}ms(源站服务端记录)"
            segs.append({"kind": "exec", "label": f"取数·尝试#{fe['attempt']}(有效)",
                         "start": fe["start_ts"], "end": fe["end_ts"],
                         "span_ids": [fe["id"]], "note": note})
            prev = fe["end_ts"]
            if qcs:
                qs, qe = min(s["start_ts"] for s in qcs), max(s["end_ts"] for s in qcs)
                if qs - prev > 0.0005:
                    segs.append({"kind": "wait", "label": "调度间隙",
                                 "start": prev, "end": qs, "span_ids": []})
                slower = max(qcs, key=lambda s: s["end_ts"] - s["start_ts"])
                segs.append({"kind": "exec",
                             "label": f"并行质控×{len(qcs)}(较慢:{STAGE_LABELS[slower['stage']]})",
                             "start": qs, "end": qe,
                             "span_ids": [s["id"] for s in qcs],
                             "note": "两项并行，耗时取较慢者，不相加"})
                prev = qe
            if wh_span["start_ts"] - prev > 0.0005:
                segs.append({"kind": "wait", "label": "调度间隙",
                             "start": prev, "end": wh_span["start_ts"], "span_ids": []})
            segs.append({"kind": "exec", "label": "入仓",
                         "start": wh_span["start_ts"], "end": wh_span["end_ts"],
                         "span_ids": [wh_span["id"]], "note": f"入仓记录#{last_w['id']}"})
            result["chain"] = {"sample_id": sid, "segments": segs,
                               "total_s": _f(wh_span["end_ts"] - fe["start_ts"])}
        bits = []
        for seg in segs:
            d = _f(seg["end"] - seg["start"])
            bits.append(f"{seg['label']} {d:.3f}s" + (f"({seg['note']})" if seg.get("note") else ""))
        fr = per_stage.get("fetch", {})
        result["text"] = (
            f"批次#{batch_id} 已完成:{len(wh_by_sample)}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条、跨服务调用 {len(client)} 次,结论以实际记录为据)。"
            f"墙钟总耗时 {_f(t1 - t0)}s。"
            f"决定完成时间的关键链(样品 {sid}): " + " → ".join(bits) + "。"
            f"瓶颈判定: {bottleneck}(各环节工作量秒: "
            + "、".join(f"{k}={v}" for k, v in result["work_parts_s"].items())
            + ";并行质控与重叠重试按墙钟计,不直接相加)。"
            f"取数尝试 {fr.get('attempts', 0)} 次:有效 {fr.get('effective', 0)}、"
            f"失败 {fr.get('failed', 0)}、迟到 {fr.get('late', 0)}"
            f"(其中曾超时放弃 {fr.get('gave_up', 0)} 次)。")
    else:
        blocking = []
        for sid in sample_ids:
            if sid in wh_by_sample:
                continue
            row = results.get(sid)
            derived = row["status"] if row else _derive(sid)
            if derived == "not_started":
                blocking.append({"sample_id": sid, "stage": "not_started",
                                 "kind": "not_started",
                                 "detail": (row["reason"] if row and row["reason"]
                                            else "未派发：批次已停止派发(无任何取数调用)")})
                continue
            sps = [s for s in client if s["sample_id"] == sid]
            fetches = [s for s in sps if s["stage"] == "fetch"]
            eff = [s for s in fetches if s["status"] == "effective"]
            if fetches and not eff:
                last = max(fetches, key=lambda s: s["start_ts"])
                tries = "、".join(f"#{s['attempt']}:{s['status']}"
                                 for s in sorted(fetches, key=lambda x: x["attempt"]))
                blocking.append({"sample_id": sid, "stage": "fetch", "kind": "call_error",
                                 "detail": f"取数 {len(fetches)} 次尝试均未成功({tries});"
                                           f"最后错误:{_err_of(last)};取数失败不是质控拒绝"})
                continue
            rejects = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match")
                       and s["status"] == "failed" and "明确判定未通过" in (s["note"] or "")]
            if rejects:
                s = rejects[0]
                blocking.append({"sample_id": sid, "stage": s["stage"], "kind": "rejected",
                                 "detail": f"{STAGE_LABELS[s['stage']]}明确未通过:{_err_of(s)}"})
                continue
            bad_qc = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match")
                      and s["status"] != "ok"]
            if bad_qc:
                s = bad_qc[0]
                blocking.append({"sample_id": sid, "stage": s["stage"], "kind": "call_error",
                                 "detail": f"{STAGE_LABELS[s['stage']]}调用异常/回执不明"
                                           f"(非明确拒绝):{_err_of(s)}"})
                continue
            if not fetches:
                blocking.append({"sample_id": sid, "stage": "fetch", "kind": "call_error",
                                 "detail": "尚无取数记录(批次可能仍在运行)"})
            else:
                blocking.append({"sample_id": sid, "stage": "warehouse",
                                 "kind": "call_error", "detail": "入仓环节未完成"})
        result["blocking"] = blocking
        bl = "；".join(
            f"样品 {b['sample_id']} "
            + (b["detail"] if b.get("kind") == "not_started"
               else f"阻断于{STAGE_LABELS.get(b['stage'], b['stage'])}({b['detail']})")
            for b in blocking) or "无阻断明细"
        result["text"] = (
            f"批次#{batch_id} 未完成:{len(wh_by_sample)}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条)。"
            f"逐样品状态:入仓 {counts.get('stored', 0)}、明确拒绝 {counts.get('rejected', 0)}、"
            f"调用异常 {counts.get('call_error', 0)}、未启动 {counts.get('not_started', 0)}。"
            + (halt_note + " " if halt_note else "")
            + f"阻断明细: {bl}。"
            f"取数失败、质控服务报错或回执不明均已计入调用异常，不当作质控拒绝；"
            f"任何未入仓样品都使批次不完整成功。失败批次不生成成功链。")
    return result
