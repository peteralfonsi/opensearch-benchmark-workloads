#!/usr/bin/env python3
"""
Iterated OpenSearch Benchmark (OSB) runner varying search_clients.

Metrics are obtained via OSB writing a results CSV file:
  --results-format=csv
  --results-file=<path>

Why this script is robust:
- Uses subprocess.Popen + communicate(timeout=...) instead of subprocess.run(timeout=...).
  If a timeout occurs, it kills the entire process group (SIGTERM then SIGKILL),
  then reaps the process so the script cannot "stall" after logging a timeout.
- Retries happen only on timeout.
- Master CSV is append-only (never drops rows). If new columns appear, the master CSV
  is rewritten with an expanded header but *all existing rows are preserved*.
- Re-runs play nicely with existing folders:
  - By default, per-run results filenames include a timestamp (no collisions).
  - If you pass --run-id, filenames become deterministic; rerunning with same run-id
    and same params will overwrite the per-run OSB results file (allowed).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple


DEFAULT_WORKLOAD_PATH = "/home/ec2-user/osb/opensearch-benchmark-workloads/big5"
DEFAULT_INCLUDE_TASKS = "term"

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


# ----------------------------
# Utilities
# ----------------------------

def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s or "")


def log_event(path: Path, msg: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(f"{utc_ts()} | {msg}\n")


def try_float(v: str) -> Any:
    v = (v or "").strip()
    try:
        return float(v)
    except ValueError:
        return v


def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("%", "pct")
    s = re.sub(r"[^\w\s]+", "", s)
    s = re.sub(r"\s+", "_", s)
    return s


def make_metric_key(op: str, metric: str, unit: str) -> str:
    """
    Normalize OSB summary CSV rows into stable column names.

    Example:
      op="term", metric="50th percentile service time", unit="ms"
        -> "term__p50_service_time_ms"
      op="term", metric="Mean Throughput", unit="ops/s"
        -> "term__mean_throughput_ops_per_s"
      op="term", metric="error rate", unit="%"
        -> "term__error_rate_pct"
    """
    opn = norm(op)
    metric_n = norm(metric)

    unit_raw = (unit or "").strip()
    if unit_raw == "ops/s":
        unit_n = "ops_per_s"
    elif unit_raw == "%":
        unit_n = "pct"
    else:
        unit_n = norm(unit_raw)

    # Convert "50th_percentile_service_time" -> "p50_service_time"
    m = re.match(r"^(?P<pct>\d+)(?:st|nd|rd|th)_percentile_(?P<rest>.+)$", metric_n)
    if m:
        metric_n = f"p{m.group('pct')}_{m.group('rest')}"

    key = f"{metric_n}_{unit_n}" if unit_n else metric_n
    return f"{opn}__{key}" if opn else key


# ----------------------------
# OSB results CSV parsing
# ----------------------------

def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def extract_metrics_from_osb_results_csv(results_csv: Path) -> Dict[str, Any]:
    """
    Parse OSB --results-format=csv output.

    Typical headers:
      Metric,Task,Value,Unit
    or:
      Metric,Operation,Value,Unit

    We accept Task/Operation for the op name column.
    """
    if not results_csv.exists():
        return {}

    rows = read_csv_dicts(results_csv)
    if not rows:
        return {}

    headers = {h.lower(): h for h in rows[0].keys() if h}
    metric_col = headers.get("metric")
    value_col = headers.get("value")
    unit_col = headers.get("unit")
    op_col = headers.get("task") or headers.get("operation") or headers.get("op") or headers.get("name")

    if not (metric_col and op_col and value_col and unit_col):
        return {}

    out: Dict[str, Any] = {}
    for r in rows:
        metric = (r.get(metric_col) or "").strip()
        op = (r.get(op_col) or "").strip()
        value = (r.get(value_col) or "").strip()
        unit = (r.get(unit_col) or "").strip()

        if not metric or not op:
            continue

        ml = metric.lower()
        want = (
            "service time" in ml
            or "throughput" in ml
            or ml == "error rate"
            or ml.endswith(" error rate")
        )
        if not want:
            continue

        out[make_metric_key(op, metric, unit)] = try_float(value)

    return out


# ----------------------------
# Master CSV append-only writer
# ----------------------------

def read_master_csv(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not path.exists():
        return [], []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        return (reader.fieldnames or []), list(reader)


def write_master_csv(path: Path, header: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in header})


def append_master_row(csv_path: Path, row: Dict[str, Any]) -> None:
    """
    Append-only semantics:
    - If file doesn't exist: create it and write header+row.
    - If new columns appear: rewrite file with expanded header, preserving all rows.
    - Otherwise append one row.
    """
    header, rows = read_master_csv(csv_path)
    if not header:
        header = sorted(row.keys())
        write_master_csv(csv_path, header, [row])
        return

    new_cols = [k for k in row.keys() if k not in header]
    if new_cols:
        header = header + sorted(new_cols)
        rows.append(row)
        write_master_csv(csv_path, header, rows)
        return

    # append
    with csv_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow({k: row.get(k, "") for k in header})


# ----------------------------
# OSB invocation
# ----------------------------

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


def run_osb_popen(
    cmd: List[str],
    timeout_s: int,
    event_log: Path,
) -> Tuple[bool, int, str, str]:
    """
    Run OSB with robust timeout handling.

    Returns: (timed_out, return_code, stdout, stderr)

    On timeout:
    - send SIGTERM to process group
    - wait briefly
    - send SIGKILL to process group
    - reap the process
    """
    # New process group so we can kill everything OSB started.
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,  # creates a new session => new pgid == pid
    )

    try:
        out, err = proc.communicate(timeout=timeout_s)
        return False, proc.returncode, out or "", err or ""

    except subprocess.TimeoutExpired:
        # We timed out. Kill the process group deterministically.
        timed_out = True
        pgid = os.getpgid(proc.pid)

        log_event(event_log, f"timeout reached; terminating process group pgid={pgid}")

        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            pass

        # Give it a moment to exit gracefully
        try:
            out, err = proc.communicate(timeout=10)
            return timed_out, proc.returncode if proc.returncode is not None else -1, out or "", err or ""
        except subprocess.TimeoutExpired:
            log_event(event_log, f"SIGTERM did not stop pgid={pgid}; sending SIGKILL")
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass

            # Now reap; this should not block indefinitely.
            out, err = proc.communicate()
            return timed_out, -1, out or "", err or ""


# ----------------------------
# Dump artifacts (optional)
# ----------------------------

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

    # Copy results file if it exists; if it's huge/unexpected, don't block forever:
    if results_csv_path.exists():
        try:
            (d / "osb_results.csv").write_text(results_csv_path.read_text())
        except Exception as e:
            (d / "osb_results_copy_error.txt").write_text(repr(e))


# ----------------------------
# Argument helpers
# ----------------------------

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


def make_label(
    run_id: str,
    include_tasks: str,
    sc: int,
    attempt: int,
    stable: bool,
) -> str:
    """
    If stable=True (run-id provided), omit timestamp so reruns overwrite per-run outputs.
    If stable=False, include timestamp to avoid collisions with preexisting directories/files.
    """
    base = f"{run_id}_task{norm(include_tasks)}_sc{sc}_attempt{attempt}"
    if stable:
        return base
    return f"{base}_{int(time.time())}"


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Iterate OSB runs over search_clients using results-file CSV output.")
    ap.add_argument("--target-host", required=True, help="e.g. 3.146.206.173:9200")
    ap.add_argument("--search-clients", nargs="+", required=True, help="e.g. 50 100 150 or 50,100,150")
    ap.add_argument("--test-iterations", type=int, default=1000)
    ap.add_argument("--timeout-minutes", type=int, default=20)
    ap.add_argument("--pause-seconds", type=int, default=180)
    ap.add_argument("--max-retries", type=int, default=3, help="Retries on timeout only (0 = unlimited).")

    ap.add_argument("--csv-path", default="osb_results_master.csv", help="Master CSV for all runs (append-only).")
    ap.add_argument("--event-log", default="osb_events.log", help="Clean event log for monitoring.")
    ap.add_argument("--results-dir", default="osb_run_results", help="Directory for per-run OSB results CSVs.")
    ap.add_argument("--dump-dir", default="", help="Optional directory for per-attempt debug artifacts.")

    ap.add_argument("--osb-bin", default="opensearch-benchmark")
    ap.add_argument("--workload-path", default=DEFAULT_WORKLOAD_PATH)
    ap.add_argument("--include-tasks", default=DEFAULT_INCLUDE_TASKS)

    ap.add_argument(
        "--run-id",
        default="",
        help="Optional stable run identifier. If set, per-run output filenames are deterministic and may overwrite on rerun.",
    )
    args = ap.parse_args()

    search_clients_list = parse_search_clients(args.search_clients)
    timeout_s = int(args.timeout_minutes) * 60

    master_csv = Path(args.csv_path)
    event_log = Path(args.event_log)
    results_dir = Path(args.results_dir)
    dump_dir = Path(args.dump_dir) if args.dump_dir else None

    stable = bool(args.run_id.strip())
    run_id = args.run_id.strip() if stable else f"run_{int(time.time())}"

    log_event(event_log, f"script started (run_id={run_id}, stable_names={stable})")

    for idx, sc in enumerate(search_clients_list):
        attempt = 0
        while True:
            attempt += 1
            start_ts = utc_ts()

            label = make_label(run_id, args.include_tasks, sc, attempt, stable=stable)
            results_csv_path = results_dir / f"{label}.results.csv"

            log_event(event_log, f"starting run: search_clients={sc}, attempt={attempt}, results_file={results_csv_path}")

            cmd = build_command(
                osb_bin=args.osb_bin,
                workload_path=args.workload_path,
                target_host=args.target_host,
                search_clients=sc,
                test_iterations=args.test_iterations,
                include_tasks=args.include_tasks,
                results_file=results_csv_path,
            )

            timed_out, rc, stdout, stderr = run_osb_popen(cmd, timeout_s, event_log)
            end_ts = utc_ts()

            metrics: Dict[str, Any] = {}
            note_parts: List[str] = []

            if timed_out:
                note_parts.append("timed_out=True (killed by script)")
                log_event(event_log, f"run timed out: search_clients={sc}, attempt={attempt}")
            else:
                note_parts.append(f"timed_out=False rc={rc}")
                if results_csv_path.exists():
                    metrics = extract_metrics_from_osb_results_csv(results_csv_path)
                    note_parts.append(f"results_file_written=True metrics_found={len(metrics)}")
                else:
                    note_parts.append("results_file_written=False (OSB did not create results file)")
                if metrics:
                    log_event(event_log, f"run completed: search_clients={sc}, attempt={attempt}, metrics={len(metrics)}")
                else:
                    log_event(event_log, f"WARNING: run completed but no metrics parsed (check results file): {results_csv_path}")

            note = " | ".join(note_parts)

            # Append-only master CSV row
            row: Dict[str, Any] = {
                "run_id": run_id,
                "label": label,
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

            # Optional debug dump; wrap so it can never wedge retries
            if dump_dir:
                try:
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
                except Exception as e:
                    log_event(event_log, f"WARNING: dump-dir write failed for {label}: {e!r}")

            if timed_out:
                # IMPORTANT: log retry decision BEFORE looping
                if args.max_retries != 0 and attempt >= args.max_retries:
                    log_event(event_log, f"giving up after {attempt} timeouts: search_clients={sc}")
                    break
                log_event(event_log, f"retrying timed-out run: search_clients={sc}, next_attempt={attempt + 1}")
                continue

            # Completed: move to next search_clients after pause
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
