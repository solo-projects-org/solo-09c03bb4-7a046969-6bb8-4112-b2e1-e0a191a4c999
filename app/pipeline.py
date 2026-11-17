"""批次流水线：取数(超时重试/迟到归类) -> 并行两项质控 -> 幂等入仓。
全程真实计时，每次调用一个 span，request_id 跨服务关联。"""
import asyncio
import json
import os
import time
import uuid

import httpx

import analysis
import db

SOURCE_URL = os.environ.get("SOURCE_URL", "http://localhost:8001")
QC_URL = os.environ.get("QC_URL", "http://localhost:8002")

# httpx 硬超时只是兜底；逻辑超时由 asyncio.wait 实现，超时后旧请求不被取消、继续运行
HARD_TIMEOUT_S = 30.0

_client = None

# batch_id -> 仍在收尾的迟到尝试任务；批次定稿前要等它们结束，结论才完整
_finalizers = {}


def _track_finalizer(batch_id, task):
    _finalizers.setdefault(batch_id, set()).add(task)
    task.add_done_callback(lambda t: _finalizers.get(batch_id, set()).discard(t))


def http():
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=HARD_TIMEOUT_S)
    return _client


def _rid():
    return uuid.uuid4().hex[:16]


# ---------------- 取数 ----------------

def _valid_fetch_receipt(data):
    """取数回执必须同时含样品号与数值浓度；缺字段/类型不对一律视为回执不明。"""
    if not isinstance(data, dict):
        return False, "响应不是 JSON 对象"
    sid = data.get("sample_id")
    if sid is None or not str(sid).strip():
        return False, "回执缺少 sample_id"
    conc = data.get("concentration")
    if isinstance(conc, bool) or not isinstance(conc, (int, float)):
        return False, "回执缺少数值型 concentration"
    return True, ""


async def call_source(batch_id, sample_id, attempt, req_id, span_id):
    try:
        r = await http().get(f"{SOURCE_URL}/fetch", params={
            "sample_id": sample_id, "request_id": req_id,
            "batch_id": batch_id, "attempt": attempt})
        end = time.time()
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                db.finish_span(span_id, end, "failed",
                               {"error": "回执不明: HTTP 200 但响应不是合法 JSON"},
                               note=" 回执不明，不算质控拒绝。")
                return ("failed", None)
            valid, why = _valid_fetch_receipt(data)
            if not valid:
                db.finish_span(span_id, end, "failed",
                               {"error": f"回执不明: {why}", "response": data},
                               note=" 回执不明，不算质控拒绝。")
                return ("failed", None)
            db.finish_span(span_id, end, "ok", {"response": data})
            return ("ok", data)
        db.finish_span(span_id, end, "failed",
                       {"status_code": r.status_code, "body": r.text[:200]})
        return ("failed", None)
    except Exception as e:
        db.finish_span(span_id, time.time(), "failed",
                       {"error": f"{type(e).__name__}: {e}"})
        return ("failed", None)


async def fetch_with_retry(batch_id, sample_id, cfg):
    """首个有效结果胜出。超时只放弃等待、不取消旧请求：
    尝试次数用完后仍在途的旧请求继续等待收尾(可能反超成为有效尝试)，
    其余迟到完成的记为 late，结果绝不参与入仓。"""
    timeout_s = cfg["client_timeout_ms"] / 1000.0
    max_attempts = int(cfg["max_attempts"])
    pending = {}   # task -> {attempt, req_id, span_id}
    state = {"winner": None, "winner_req": None}

    def collect(done):
        for t in done:
            meta = pending.pop(t)
            status, data = t.result()
            if status == "ok" and state["winner"] is None:
                state["winner"], state["winner_req"] = data, meta["req_id"]
                db.mark_status(meta["span_id"], "effective", note=" 该尝试结果被采用。")
            elif status == "ok":
                db.mark_status(meta["span_id"], "late", note=" 完成时已有有效结果，迟到未采用。")

    attempt = 0
    # 发起阶段：逻辑超时即追加新尝试，旧尝试继续运行
    while state["winner"] is None and attempt < max_attempts:
        attempt += 1
        req_id = _rid()
        span_id = db.insert_span(batch_id, sample_id, "fetch", "client", "orchestrator",
                                 attempt, req_id, "running", time.time())
        task = asyncio.create_task(call_source(batch_id, sample_id, attempt, req_id, span_id))
        pending[task] = {"attempt": attempt, "req_id": req_id, "span_id": span_id}
        done, _ = await asyncio.wait(set(pending), timeout=timeout_s,
                                     return_when=asyncio.FIRST_COMPLETED)
        collect(done)
        if state["winner"] is None and not done:
            # 逻辑超时：标记客户端放弃，但任务继续运行(源站可能仍在执行)
            for t, meta in pending.items():
                db.mark_status(meta["span_id"], "timeout",
                               note=f" 客户端超时(等待超 {cfg['client_timeout_ms']}ms)放弃并发起新尝试；"
                                    f"旧请求可能仍在源站运行。",
                               only_if="running")
    # 收尾阶段：不再发新请求，已在途的尝试仍可能给出首个有效结果
    while state["winner"] is None and pending:
        done, _ = await asyncio.wait(set(pending), timeout=HARD_TIMEOUT_S,
                                     return_when=asyncio.FIRST_COMPLETED)
        if not done:
            break
        collect(done)
    if pending:
        _track_finalizer(batch_id, asyncio.create_task(_finalize_late(dict(pending))))
    if state["winner"] is None:
        return None
    state["winner"]["_req_id"] = state["winner_req"]
    return state["winner"]


async def _finalize_late(pending):
    """胜出/放弃后仍在跑的旧尝试：完成时如实记录为 late，结果不入仓。"""
    for t, meta in pending.items():
        try:
            status, _ = await t
            if status == "ok":
                db.mark_status(meta["span_id"], "late",
                               note=" 迟到完成，结果未采用，不参与入仓。")
        except Exception:
            pass


# ---------------- 质控(两项并行) ----------------

# 返回 (结果, 原因)：结果 ∈ pass / reject / error。
# reject 只来自质控服务明确判定未通过；连接失败、非 200、回执不明都是 error，绝不算拒绝。
async def qc_call(batch_id, sample_id, check, payload):
    stage = f"qc_{check}"
    req_id = _rid()
    span_id = db.insert_span(batch_id, sample_id, stage, "client", "orchestrator",
                             1, req_id, "running", time.time())
    try:
        r = await http().post(f"{QC_URL}/check/{check}",
                              json={**payload, "request_id": req_id, "batch_id": batch_id})
        end = time.time()
        if r.status_code != 200:
            db.finish_span(span_id, end, "failed",
                           {"status_code": r.status_code, "body": r.text[:200]},
                           note=" 质控服务调用异常(非 200)，不算质控拒绝。")
            return "error", f"质控服务 HTTP {r.status_code}"
        try:
            res = r.json()
        except Exception:
            db.finish_span(span_id, end, "failed",
                           {"error": "回执不明: HTTP 200 但响应不是合法 JSON"},
                           note=" 质控回执不明，不算质控拒绝。")
            return "error", "质控回执不明(非 JSON)"
        if not isinstance(res, dict) or not isinstance(res.get("pass"), bool):
            db.finish_span(span_id, end, "failed",
                           {"error": "回执不明: 缺少布尔型 pass 字段", "response": res},
                           note=" 质控回执不明，不算质控拒绝。")
            return "error", "质控回执不明(缺 pass)"
        ok = res["pass"]
        if ok:
            db.finish_span(span_id, end, "ok", {"response": res})
            return "pass", ""
        db.finish_span(span_id, end, "failed", {"response": res},
                       note=" 质控明确判定未通过。")
        return "reject", json.dumps(res, ensure_ascii=False)
    except Exception as e:
        db.finish_span(span_id, time.time(), "failed",
                       {"error": f"{type(e).__name__}: {e}"},
                       note=" 质控服务调用异常，不算质控拒绝。")
        return "error", f"质控服务调用异常: {type(e).__name__}: {e}"


# ---------------- 单样品流程 ----------------

CHECK_LABELS = {"nonneg": "质控①非负值", "match": "质控②样品号对应"}


def _reject_reason(check, payload):
    if check == "nonneg":
        return f"{CHECK_LABELS[check]}明确拒绝: 浓度 {payload.get('concentration')} 为负"
    return (f"{CHECK_LABELS[check]}明确拒绝: 返回样品号 "
            f"{payload.get('sample_id')} 与委托 {payload.get('expected_sample_id')} 不对应")


async def process_sample(batch_id, sample_id, cfg):
    """返回 (status, stage, reason)，status ∈ stored/rejected/call_error。

    只有质控服务明确返回 pass=false 才算 rejected；
    取数失败、质控服务报错、回执不明一律为 call_error，不计入拒绝次数，
    但同样会让样品无法入仓，从而阻断批次完整成功。"""
    data = await fetch_with_retry(batch_id, sample_id, cfg)
    if data is None:
        return "call_error", "fetch", "取数失败(含超时/连接异常/回执不明)，非质控拒绝"
    req_id_used = data.pop("_req_id")
    returned_id = str(data.get("sample_id"))
    payload = {"sample_id": returned_id, "expected_sample_id": sample_id,
               "concentration": data.get("concentration")}
    # 两项质控并行：墙钟取较慢者，不相加
    (r1, w1), (r2, w2) = await asyncio.gather(
        qc_call(batch_id, sample_id, "nonneg", payload),
        qc_call(batch_id, sample_id, "match", payload))
    rejects = [(c, w) for c, r, w in (("nonneg", r1, w1), ("match", r2, w2))
               if r == "reject"]
    if rejects:
        check, _ = rejects[0]
        return "rejected", f"qc_{check}", _reject_reason(check, payload)
    errors = [(c, w) for c, r, w in (("nonneg", r1, w1), ("match", r2, w2))
              if r == "error"]
    if errors:
        check, why = errors[0]
        return "call_error", f"qc_{check}", f"质控调用异常/回执不明(非明确拒绝): {why}"
    span_id = db.insert_span(batch_id, sample_id, "warehouse", "client", "orchestrator",
                             1, _rid(), "running", time.time())
    inserted = db.warehouse_insert(batch_id, sample_id, data.get("concentration"), req_id_used)
    db.finish_span(span_id, time.time(), "ok",
                   {"inserted": inserted, "concentration": data.get("concentration"),
                    "source_request_id": req_id_used},
                   note=None if inserted else " 唯一约束去重：该样品已有入仓记录，未重复写入。")
    return "stored", "warehouse", ""


# ---------------- 批次调度(拒绝阈值 + 同时处理数) ----------------

async def _halt_locked(batch_id, threshold, rejected, queue, halted):
    """调用方已持有调度锁时使用：置位停止派发并把未派发样品标为未启动。"""
    if halted["v"]:
        return
    halted["v"] = True
    remaining = list(queue)
    queue.clear()
    db.mark_halted(
        batch_id, time.time(),
        f"明确质控拒绝样品数达到阈值 {threshold}(当前 {rejected})，停止派发尚未开始的样品")
    for sid in remaining:
        db.set_sample_result(
            batch_id, sid, "not_started", stage="",
            reason=f"未派发：批次已达到明确质控拒绝阈值 {threshold}")


async def run_batch(batch_id, sample_ids):
    cfg = db.get_config()  # 批次开始时快照配置，批内一致
    st = db.get_batch_state(batch_id) or {}
    threshold = int(st.get("qc_reject_threshold") or 0)
    concurrency = int(st.get("sample_concurrency") or 0)
    queue = list(sample_ids)
    lock = asyncio.Lock()
    halted = {"v": False}
    dispatched = {"n": 0}

    async def worker():
        while True:
            async with lock:
                if halted["v"] or not queue:
                    return
                # 派发前再核对一次：阈值已达到就不再派发(只可能因并发窗口略有滞后)
                if threshold > 0:
                    cur = db.get_batch_state(batch_id)
                    if cur and int(cur["reject_count"]) >= threshold:
                        await _halt_locked(batch_id, threshold,
                                           int(cur["reject_count"]), queue, halted)
                        return
                sid = queue.pop(0)
                dispatched["n"] += 1
                db.update_batch_state(batch_id, dispatched=dispatched["n"])
                db.set_sample_result(batch_id, sid, "running", stage="fetch",
                                     reason="已派发，取数中")
            try:
                status, stage, reason = await process_sample(batch_id, sid, cfg)
            except Exception as e:  # 流水线自身异常也不能冒充质控拒绝
                status, stage = "call_error", "pipeline"
                reason = f"流水线异常: {type(e).__name__}: {e}"
            db.set_sample_result(batch_id, sid, status, stage=stage, reason=reason)
            if status == "rejected":
                # 拒绝计数与"停止派发"置位在同一把调度锁内完成，
                # 保证阈值判定与派发决策有明确先后，不会因计数竞争多派发。
                async with lock:
                    db.incr_rejects(batch_id)
                    cur = db.get_batch_state(batch_id)
                    if threshold > 0 and int(cur["reject_count"]) >= threshold:
                        await _halt_locked(batch_id, threshold, int(cur["reject_count"]),
                                           queue, halted)

    n = len(sample_ids)
    workers = n if concurrency <= 0 else min(concurrency, n)
    await asyncio.gather(*(worker() for _ in range(workers)), return_exceptions=True)
    # 等在途的迟到尝试收尾(有 httpx 硬超时兜底)，使定稿结论如实包含迟到记录。
    # 迟到收尾只补记跨度，绝不会重新派发未启动样品，也不影响其他批次。
    pending = list(_finalizers.get(batch_id, ()))
    if pending:
        await asyncio.wait(pending)
    _finalizers.pop(batch_id, None)
    concl = analysis.analyze(batch_id)
    status = "done" if concl.get("status") == "done" else "failed"
    db.execute("UPDATE batches SET status=?, finished_at=?, conclusion=? WHERE id=?",
               (status, time.time(), json.dumps(concl, ensure_ascii=False), batch_id))
