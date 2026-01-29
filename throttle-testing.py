#!/usr/bin/env python3
"""
Iterated OpenSearch Benchmark (OSB) runner that varies search_clients and writes
key metrics to a single CSV.

This version avoids scraping stdout/stderr entirely for results, because OSB may
suppress the terminal summary when stdout is not a TTY. Instead, it requests a
summary report written to a per-run file via:

  --results-format=csv
  --results-file=<path>

Then it parses that CSV to extract:
- all percentiles for *service time*
- error rate
- throughput values (min/mean/median/max and any percentiles if present)

It also writes:
- a clean event log for monitoring
- optional per-run dumps (cmd/stdout/stderr + results CSV copy) for debugging

Notes:
- Nonzero errors are expected.
- Timeouts are enforced by Python (kills OSB after timeout).
- All runs include --kill-running-processes (required to restart after a kill).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple


DEFAULT_WORKLOAD_PATH = "/home/ec2-user/osb/opensearch-benchmark-workloads/big5"
DEFAULT_INCLUDE_TASKS = "term"

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_event(path: Path, msg: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(f"{utc_ts()} | {msg}\n")


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


def try_float(v: str) -> Any:
    v = v.strip()
    try:
        return float(v)
    except ValueError:
        return v


def norm(s: str) -> str:
    s = s.lower().strip()
    s = s.replace("%", "pct")
    s = re.sub(r"[^\w\s]+", "", s)
    s = re.sub(r"\s+", "_", s)
    return s


def safe_op_prefix(op: str) -> str:
    opn = norm(op)
    return opn


def make_metric_key(op: str, metric: str, unit: str) -> str:
    """
    Convert OSB summary CSV rows into stable column keys.

    Examples:
      op="term", metric="50th percentile service time", unit="ms"
        -> "term__p50_service_time_ms"
      op="term", metric="Mean Throughput", unit="ops/s"
        -> "term__mean_throughput_ops_per_s"
      op="term", metric="error rate", unit="%"
        -> "term__error_rate_pct"
    """
    opn = safe_op_prefix(op)
    metric_n = norm(metric)
    unit_n = norm(unit).replace("ops_s", "ops_per_s")  # common nicety

    # Normalize "ops/s" nicely:
    unit_n = unit_n.replace("ops_per_s", "ops_per_s")
    unit_n = unit_n.replace("ops_s", "ops_per_s")

    # Fix unit normalization for common patterns:
    if unit.strip() == "ops/s":
        unit_n = "ops_per_s"
    elif unit.strip() == "%":
        unit_n = "pct"

    # Special handling for percentiles:
    m = re.match(r"^(?P<pct>\d+)(?:st|nd|rd|th)_percentile_(?P<rest>.+)$", metric_n)
    if m:
        metric_n = f"p{m.group('pct')}_{m.group('rest')}"

    key = f"{metric_n}_{unit_n}" if unit_n else metric_n
    return f"{opn}__{key}" if opn else key


def read_csv_file(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def extract_metrics_from_osb_results_csv(results_csv: Path) -> Dict[str, Any]:
    """
    Parse OSB --results-format=csv output.

    OSB results-file CSV typically contains rows like:
      Metric,Task,Value,Unit

    But some versions may use headers like:
      Metric,Operation,Value,Unit

    We handle both.
    """
    rows = read_csv_file(results_csv)
    if not rows:
        return {}

    # Identify column names
    headers = {h.lower(): h for h in rows[0].keys() if h is not None}

    def pick(*candidates: str) -> Optional[str]:
        for c in candidates:
            if c in headers:
                return headers[c]
        return None

    metric_col = pick("metric")
    op_col = pick("task", "operation", "op", "name")
    value_col = pick("value")
    unit_col = pick("unit")

    # If required columns are missing, bail out (caller will log/debug)
    if not metric_col or not op_col or not value_col or not unit_col:
        return {}

    out: Dict[str, Any] = {}

    for r in rows:
        metric = (r.get(metric_col) or "").strip()
        op = (r.get(op_col) or "").strip()
        value = (r.get(value_col) or "").strip()
        unit = (r.get(unit_col) or "").strip()

        if not metric or not op:
            continue

        metric_l = metric.lower()

        want = (
            "service time" in metric_l
            or "throughput" in metric_l
            or metric_l == "error rate"
            or metric_l.endswith(" error rate")
        )
        if not want:
            continue

        key = make_metric_key(op, metric, unit)
        out[key] = try_float(value)

    return out


def read_master_csv(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not path.exists():
        return [], []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return reader.fieldnames or [], rows


def write_master_csv(path: Path, header: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in header})


def append_master_row(csv_path: Path, row: Dict[str, Any]) -> None:
    header, rows = read_master_csv(csv_path)
    new_cols = [k for k in row.keys() if k not in header]

    if not header:
        header = sorted(row.keys())
        write_master_csv(csv_path, header, [row])
        return

    if new_cols:
        header = header + sorted(new_cols)
        rows.append(row)
        write_master_csv(csv_path, header, rows)
    else:
        with csv_path.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writerow({k: row.get(k, "") for k in header})


def build_command(
    osb_bin: str,
    workload_path: str,
    target_host: str,
    search_clients: int,
    test_iterations: int,
    include_tasks: str,
    results_file: Path,
) -> List[str]:
    params = {
        "number_of_replicas": "0",
        "search_clients": str(search_clients),
        "target_throughput": "",
        "test_iterations": test_iterations,
    }

    # IMPORTANT: results_file must be a string path; ensure parent exists.
    results_file.parent.mkdir(parents=True, exist_ok=True)

    return [
        osb_bin,
        "run",
        "--kill-running-processes",
        f"--workload-path={workload_path}",
        f"--target-host={target_host}",
        f"--workload-params={json.dumps(params, separators=(',', ':'))}",
        f"--include-tasks={include_tasks}",
        "--results-format=csv",
        f"--results-file={str(results_file)}",
    ]


def run_osb(cmd: List[str], timeout_s: int) -> Tuple[bool, int, str, str]:
    """
    Returns: (timed_out, return_code, stdout, stderr)
    stderr is kept for debugging even if empty.
    """
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        return False, proc.returncode, proc.stdout or "", proc.stderr or ""
    except subprocess.TimeoutExpired as e:
        return True, -1, e.stdout or "", e.stderr or ""


def dump_attempt_artifacts(
    dump_dir: Path,
    label: str,
    cmd: List[str],
    stdout: str,
    stderr: str,
    results_csv_path: Path,
    metrics: Dict[str, Any],
    note: str,
) -> None:
    d = dump_dir / label
    d.mkdir(parents=True, exist_ok=True)

    (d / "cmd.txt").write_text(" ".join(cmd) + "\n")
    (d / "stdout.txt").write_text(strip_ansi(stdout))
    (d / "stderr.txt").write_text(strip_ansi(stderr))
    (d / "note.txt").write_text(note + "\n")
    (d / "parsed_metrics.json").write_text(json.dumps(metrics if metrics else {"note": "no metrics parsed"}, indent=2))

    # Copy results file if it exists
    if results_csv_path.exists():
        (d / "osb_results.csv").write_text(results_csv_path.read_text())


def parse_search_clients(values: List[str]) -> List[int]:
    out: List[int] = []
    for v in values:
        for p in v.split(","):
            p = p.strip()
            if p:
                out.append(int(p))
    if not out:
        raise ValueError("No --search-clients values provided.")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Iterate OSB runs over search_clients and capture metrics via results-file CSV.")
    ap.add_argument("--target-host", required=True, help="e.g. 3.146.206.173:9200")
    ap.add_argument("--search-clients", nargs="+", required=True, help="e.g. 50 100 150 or 50,100,150")
    ap.add_argument("--test-iterations", type=int, default=1000)
    ap.add_argument("--timeout-minutes", type=int, default=20)
    ap.add_argument("--pause-seconds", type=int, default=180)
    ap.add_argument("--max-retries", type=int, default=3, help="Retries on timeout (0 = unlimited).")
    ap.add_argument("--csv-path", default="osb_results_master.csv", help="Master CSV for all runs.")
    ap.add_argument("--event-log", default="osb_events.log", help="Clean event log for monitoring.")
    ap.add_argument("--osb-bin", default="opensearch-benchmark")
    ap.add_argument("--workload-path", default=DEFAULT_WORKLOAD_PATH)
    ap.add_argument("--include-tasks", default=DEFAULT_INCLUDE_TASKS)
    ap.add_argument(
        "--results-dir",
        default="osb_run_results",
        help="Directory where per-run OSB --results-file CSVs are written.",
    )
    ap.add_argument(
        "--dump-dir",
        default="",
        help="Optional directory to dump cmd/stdout/stderr/results/parsed-metrics per attempt.",
    )
    args = ap.parse_args()

    search_clients_list = parse_search_clients(args.search_clients)
    timeout_s = int(args.timeout_minutes) * 60

    master_csv = Path(args.csv_path)
    event_log = Path(args.event_log)
    results_dir = Path(args.results_dir)
    dump_dir = Path(args.dump_dir) if args.dump_dir else None

    log_event(event_log, "script started")

    for idx, sc in enumerate(search_clients_list):
        attempt = 0

        while True:
            attempt += 1
            start_ts = utc_ts()
            log_event(event_log, f"starting run: search_clients={sc}, attempt={attempt}")

            # Per-attempt results file path
            label = f"sc{sc}_attempt{attempt}_{int(time.time())}"
            results_csv_path = results_dir / f"{label}.results.csv"

            cmd = build_command(
                osb_bin=args.osb_bin,
                workload_path=args.workload_path,
                target_host=args.target_host,
                search_clients=sc,
                test_iterations=args.test_iterations,
                include_tasks=args.include_tasks,
                results_file=results_csv_path,
            )

            timed_out, rc, stdout, stderr = run_osb(cmd, timeout_s)
            end_ts = utc_ts()

            metrics: Dict[str, Any] = {}
            note_parts: List[str] = []

            if timed_out:
                note_parts.append("timed_out=True (killed by Python timeout)")
                log_event(event_log, f"run timed out: search_clients={sc}, attempt={attempt}")
            else:
                note_parts.append(f"timed_out=False rc={rc}")
                # Parse the results file OSB should have written
                if results_csv_path.exists():
                    metrics = extract_metrics_from_osb_results_csv(results_csv_path)
                    note_parts.append(f"results_file_written=True metrics_found={len(metrics)}")
                else:
                    note_parts.append("results_file_written=False (OSB did not create results file)")

                if metrics:
                    log_event(event_log, f"run completed: search_clients={sc}, attempt={attempt}, metrics={len(metrics)}")
                else:
                    log_event(
                        event_log,
                        f"WARNING: run completed but no metrics parsed from results file: "
                        f"search_clients={sc}, attempt={attempt} (check {results_csv_path})"
                    )

            note = " | ".join(note_parts)

            # Write master CSV row
            row: Dict[str, Any] = {
                "start_ts_utc": start_ts,
                "end_ts_utc": end_ts,
                "target_host": args.target_host,
                "search_clients": sc,
                "test_iterations": args.test_iterations,
                "attempt": attempt,
                "timed_out": timed_out,
                "return_code": rc,
                "osb_results_file": str(results_csv_path),
                "note": note,
            }
            row.update(metrics)
            append_master_row(master_csv, row)

            # Optional dump for debugging
            if dump_dir:
                dump_attempt_artifacts(
                    dump_dir=dump_dir,
                    label=label,
                    cmd=cmd,
                    stdout=stdout,
                    stderr=stderr,
                    results_csv_path=results_csv_path,
                    metrics=metrics,
                    note=note,
                )

            if timed_out:
                if args.max_retries != 0 and attempt >= args.max_retries:
                    log_event(event_log, f"giving up after {attempt} timeouts: search_clients={sc}")
                    break
                log_event(event_log, f"retrying timed-out run: search_clients={sc}, next_attempt={attempt + 1}")
                continue

            # Completed: move on after pause
            if idx < len(search_clients_list) - 1:
                log_event(event_log, f"pausing {args.pause_seconds}s before next search_clients value")
                time.sleep(args.pause_seconds)
            break

    log_event(event_log, "script finished")
    print(f"[OK] Master CSV: {master_csv.resolve()}")
    print(f"[OK] Event log: {event_log.resolve()}")
    print(f"[OK] Per-run results dir: {results_dir.resolve()}")
    if dump_dir:
        print(f"[OK] Dump dir: {dump_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
