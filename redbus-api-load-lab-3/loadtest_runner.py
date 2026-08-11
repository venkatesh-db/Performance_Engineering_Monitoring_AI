"""
Runs the k6 script (k6/payment_test.js) as a real subprocess so the lab UI
can trigger the Module 3 hands-on-lab scenarios (VU-based vs arrival-rate)
from the browser and show actual k6 output, not a simulation of one.
"""
import json
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

PROJECT_DIR = Path(__file__).parent
RUNS_DIR = PROJECT_DIR / "k6" / "runs"
RUNS_DIR.mkdir(exist_ok=True)

ALLOWED_SCENARIOS = {"vus", "arrival"}


@dataclass
class Run:
    run_id: str
    scenario: str
    rate: int | None
    status: str = "running"  # running | done | error
    summary: dict | None = None
    error: str | None = None
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None


RUNS: dict[str, Run] = {}
_runs_lock = threading.Lock()


def _validate_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost"):
        raise ValueError("base_url must be http://127.0.0.1 or http://localhost")
    return base_url


def _extract_metrics(summary_path: Path) -> dict:
    data = json.loads(summary_path.read_text())
    metrics = data.get("metrics", {})

    def get(name: str, key: str, default=0):
        return metrics.get(name, {}).get(key, default)

    # k6's summary JSON stores each threshold as {"condition string": bool},
    # where the bool is true when that threshold was BREACHED, not when it
    # passed -- inverted from what the key name suggests.
    any_breached = any(
        breached
        for metric in metrics.values()
        if isinstance(metric, dict)
        for breached in metric.get("thresholds", {}).values()
    )

    return {
        "checks_pass_rate": get("checks", "value", 0),
        "biz_success": get("biz_success", "count", 0),
        "biz_declined": get("biz_declined", "count", 0),
        "infra_error": get("infra_error", "count", 0),
        "http_reqs": get("http_reqs", "count", 0),
        "p50_ms": get("payment_latency_ms", "med", 0),
        "p95_ms": get("payment_latency_ms", "p(95)", 0),
        "p99_ms": get("payment_latency_ms", "p(99)", 0),
        "dropped_iterations": get("dropped_iterations", "count", 0),
        "thresholds_ok": not any_breached,
    }


def _run_k6(run: Run, base_url: str) -> None:
    summary_path = RUNS_DIR / f"{run.run_id}.json"
    cmd = [
        "k6", "run", "k6/payment_test.js",
        "--env", f"SCENARIO={run.scenario}",
        "--env", f"BASE_URL={base_url}",
        "--summary-export", str(summary_path),
    ]
    if run.scenario == "arrival" and run.rate:
        cmd += ["--env", f"RATE={run.rate}"]

    try:
        proc = subprocess.run(
            cmd, cwd=PROJECT_DIR, capture_output=True, text=True, timeout=120,
        )
        with _runs_lock:
            if summary_path.exists():
                run.summary = _extract_metrics(summary_path)
                run.status = "done"
            else:
                run.status = "error"
                run.error = (proc.stderr or proc.stdout)[-2000:]
            run.finished_at = time.time()
    except Exception as e:  # noqa: BLE001 -- surfaced to the UI, not swallowed
        with _runs_lock:
            run.status = "error"
            run.error = str(e)
            run.finished_at = time.time()


def start_run(scenario: str, rate: int | None, base_url: str) -> Run:
    if scenario not in ALLOWED_SCENARIOS:
        raise ValueError(f"scenario must be one of {ALLOWED_SCENARIOS}")
    base_url = _validate_base_url(base_url)
    if rate is not None and not (1 <= rate <= 500):
        raise ValueError("rate must be between 1 and 500")

    run = Run(run_id=uuid.uuid4().hex[:10], scenario=scenario, rate=rate)
    with _runs_lock:
        RUNS[run.run_id] = run
    threading.Thread(target=_run_k6, args=(run, base_url), daemon=True).start()
    return run


def get_run(run_id: str) -> Run | None:
    with _runs_lock:
        return RUNS.get(run_id)


def list_runs() -> list[Run]:
    with _runs_lock:
        return sorted(RUNS.values(), key=lambda r: r.started_at, reverse=True)


@dataclass
class JMeterEntry:
    label: str
    p95_ms: float
    error_rate_pct: float
    throughput_rps: float
    recorded_at: float = field(default_factory=time.time)


JMETER_ENTRIES: list[JMeterEntry] = []
_jmeter_lock = threading.Lock()


def record_jmeter_entry(label: str, p95_ms: float, error_rate_pct: float, throughput_rps: float) -> JMeterEntry:
    entry = JMeterEntry(label=label, p95_ms=p95_ms, error_rate_pct=error_rate_pct, throughput_rps=throughput_rps)
    with _jmeter_lock:
        JMETER_ENTRIES.append(entry)
    return entry


def list_jmeter_entries() -> list[JMeterEntry]:
    with _jmeter_lock:
        return list(reversed(JMETER_ENTRIES))
