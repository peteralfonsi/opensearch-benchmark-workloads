#!/usr/bin/env python3
"""
Iterated OpenSearch Benchmark (OSB) runner using OSB CSV results output.

Behavior:
- OSB writes per-run results via --results-format=csv --results-file=...
- Script parses that CSV and appends metrics to a master CSV
- Retries on timeout
- Append-only master CSV (never deletes rows)
- Safe reuse of existing results/dump directories
- Optional --parallelisms / --parallelism flag to set the dynamic cluster setting
  search_virtual_threads.parallelism before each run

If no parallelism flag is provided, parallelism is recorded as -1 and no cluster
setting update is sent.
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
from typing import Dict, Any, List, Tuple, Optional
from urllib import request, error


DEFAULT_WORKLOAD_PATH = "/home/ec2-user/osb/opensearch-benchmark-workloads/big5"
DEFAULT_INCLUDE_TASKS = "term"

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


# -----------------------
# Utilities
# -----------------------

def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_text(s: Optional[object]) -> str:
    if s is None:
        return ""
    if isinstance(s, bytes):
        return s.decode("utf-8", errors="replace")
    return str(s)


def strip_ansi(s: Optional[object]) -> str:
    return ANSI_RE.sub("", safe_text(s))


def log_event(path: Path, msg: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(f"{utc_ts()} | {msg}\n")


def try_float(v: str) -> Any:
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


def ensure_http_url(target_host: str) -> str:
    if target_host.startswith("http://") or target_host.startswith("https://"):
        return target_host.rstrip("/")
    return f"http://{target_host.rstrip('/')}"


def parse_int_list_arg(values: Optional[List[str]]) -> List[int]:
    out: List[int] = []
    if not values:
        return out
    for v in values:
        for p in v.split(","):
            p = p.strip()
            if p:
                out.append(int(p))
    return out


# -----------------------
# Cluster setting update
# -----------------------

def set_cluster_parallelism(target_host: str, parallelism: int, timeout_s: int = 30) -> str:
    """
    Set transient cluster setting search_virtual_threads.parallelism.

    Returns response body as text.
    Raises RuntimeError on failure.
    """
    base_url = ensure_http_url(target_host)
    url = f"{base_url}/_cluster/settings"
    payload = {
        "transient": {
            "search_virtual_threads.parallelism": parallelism
        }
    }
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )

    try:
        with request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status < 200 or resp.status >= 300:
                raise RuntimeError(f"HTTP {resp.status}: {body}")
            return body
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {body}") from e
    except error.URLError as e:
        raise RuntimeError(f"URL error updating cluster setting: {e}") from e


# -----------------------
# Metric keying
# -----------------------

def make_metric_key(op: str, metric: str, unit: str) -> str:
    opn = norm(op)
    metric_n = norm(metric)

    unit_raw = unit.strip()
    if unit_raw == "ops/s":
        unit_n = "ops_per_s"
    elif unit_raw == "%":
        unit_n = "pct"
    else:
        unit_n = norm(unit_raw)

    m = re.match(r"^(?P<pct>\d+)(?:st|nd|rd|th)_percentile_(?P<rest>.+)$", metric_n)
    if m:
        metric_n = f"p{m.group('pct')}_{m.group('rest')}"

    key = f"{metric_n}_{unit_n}" if unit_n else metric_n
    return f"{opn}__{key}" if opn else key


# -----------------------
# OSB results CSV parsing
# -----------------------

def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def extract_metrics_from_osb_results_csv(results_csv: Path) -> Dict[str, Any]:
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
        if (
            "service time" not in ml
            and "throughput" not in ml
            and ml != "error rate"
            and not ml.endswith(" error rate")
        ):
            continue

        out[make_metric_key(op, metric, unit)] = try_float(value)

    return out


# -----------------------
# Master CSV (append-only)
# -----------------------

def read_master_csv(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not path.exists():
        return [], []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames or [], list(reader)


def write_master_csv(path: Path, header: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in header})


def append_master_row(csv_path: Path, row: Dict[str, Any]) -> None:
    header, rows = read_master_csv(csv_path)

    if not header:
        write_master_csv(csv_path, sorted(row.keys()), [row])
        return

    new_cols = [k for k in row if k not in header]
    if new_cols:
        header = header + sorted(new_cols)
        rows.append(row)
        write_master_csv(csv_path, header, rows)
        return

    with csv_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow({k: row.get(k, "") for k in header})


# -----------------------
# OSB invocation
# -----------------------

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


def run_osb(cmd: List[str], timeout_s: int) -> Tuple[bool, int, object, object]:
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_s,
            check=False,
        )
        return False, proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as e:
        return True, -1, e.stdout, e.stderr


# -----------------------
# Debug dump (safe)
# -----------------------

def dump_attempt_artifacts(
    dump_dir: Path,
    label: str,
    cmd: List[str],
    stdout: object,
    stderr: object,
    results_csv_path: Path,
    metrics: Dict[str, Any],
    note: str,
) -> None:
    try:
        d = dump_dir / label
        d.mkdir(parents=True, exist_ok=True)

        (d / "cmd.txt").write_text(" ".join(cmd) + "\n")
        (d / "stdout.txt").write_text(strip_ansi(stdout))
        (d / "stderr.txt").write_text(strip_ansi(stderr))
        (d / "note.txt").write_text(note + "\n")
        (d / "parsed_metrics.json").write_text(json.dumps(metrics or {"note": "no metrics"}, indent=2))

        if results_csv_path.exists():
            (d / "osb_results.csv").write_text(results_csv_path.read_text())
    except Exception:
        # Dump failures must never stop retries or later runs.
        pass


# -----------------------
# Main
# -----------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-host", required=True)
    ap.add_argument("--search-clients", nargs="+", required=True)
    ap.add_argument("--test-iterations", type=int, default=1000)
    ap.add_argument("--timeout-minutes", type=int, default=20)
    ap.add_argument("--pause-seconds", type=int, default=180)
    ap.add_argument("--max-retries", type=int, default=3)

    ap.add_argument("--csv-path", required=True)
    ap.add_argument("--event-log", required=True)
    ap.add_argument("--results-dir", default="osb_run_results")
    ap.add_argument("--dump-dir", default="")

    ap.add_argument("--osb-bin", default="opensearch-benchmark")
    ap.add_argument("--workload-path", default=DEFAULT_WORKLOAD_PATH)
    ap.add_argument("--include-tasks", default=DEFAULT_INCLUDE_TASKS)

    # Optional dynamic cluster setting values.
    # Supports either --parallelisms or --parallelism for convenience.
    ap.add_argument("--parallelisms", nargs="+", default=None)
    ap.add_argument("--parallelism", nargs="+", default=None)

    args = ap.parse_args()

    search_clients = parse_int_list_arg(args.search_clients)
    parallelism_values = parse_int_list_arg(args.parallelisms) or parse_int_list_arg(args.parallelism)
    if not parallelism_values:
        parallelism_values = [-1]

    timeout_s = args.timeout_minutes * 60

    master_csv = Path(args.csv_path)
    event_log = Path(args.event_log)
    results_dir = Path(args.results_dir)
    dump_dir = Path(args.dump_dir) if args.dump_dir else None

    log_event(
        event_log,
        f"script started | search_clients={search_clients} | parallelisms={parallelism_values}",
    )

    total_run_index = 0
    total_runs = len(search_clients) * len(parallelism_values)

    for p_idx, parallelism in enumerate(parallelism_values):
        for s_idx, sc in enumerate(search_clients):
            attempt = 0

            while True:
                attempt += 1
                total_run_index += 1
                label = f"par{parallelism}_sc{sc}_attempt{attempt}_{int(time.time())}"
                results_csv = results_dir / f"{label}.results.csv"

                log_event(
                    event_log,
                    f"starting run {total_run_index}/{total_runs}: "
                    f"parallelism={parallelism}, search_clients={sc}, attempt={attempt}",
                )

                setting_update_note = ""
                if parallelism != -1:
                    try:
                        response_body = set_cluster_parallelism(args.target_host, parallelism)
                        setting_update_note = f"parallelism_set={parallelism}"
                        log_event(
                            event_log,
                            f"updated cluster setting search_virtual_threads.parallelism={parallelism}",
                        )
                        if dump_dir:
                            try:
                                d = dump_dir / label
                                d.mkdir(parents=True, exist_ok=True)
                                (d / "cluster_setting_update_response.json").write_text(response_body)
                            except Exception:
                                pass
                    except Exception as e:
                        setting_update_note = f"parallelism_update_failed={parallelism}"
                        log_event(
                            event_log,
                            f"ERROR: failed to set search_virtual_threads.parallelism={parallelism}: {e}",
                        )
                        row = {
                            "timestamp_utc": utc_ts(),
                            "target_host": args.target_host,
                            "search_clients": sc,
                            "parallelism": parallelism,
                            "attempt": attempt,
                            "timed_out": False,
                            "return_code": "",
                            "results_file": str(results_csv),
                            "note": f"{setting_update_note} | cluster_setting_error={safe_text(e)}",
                        }
                        append_master_row(master_csv, row)

                        # Treat this as a failed run setup; do not retry forever.
                        break
                else:
                    setting_update_note = "parallelism_unset"

                cmd = build_command(
                    args.osb_bin,
                    args.workload_path,
                    args.target_host,
                    sc,
                    args.test_iterations,
                    args.include_tasks,
                    results_csv,
                )

                timed_out, rc, stdout, stderr = run_osb(cmd, timeout_s)

                metrics: Dict[str, Any] = {}
                note_parts = [setting_update_note]

                if timed_out:
                    note_parts.append("timed_out=True")
                    log_event(
                        event_log,
                        f"run timed out: parallelism={parallelism}, search_clients={sc}, attempt={attempt}",
                    )
                else:
                    if results_csv.exists():
                        metrics = extract_metrics_from_osb_results_csv(results_csv)
                        note_parts.append(f"metrics_found={len(metrics)}")
                        if metrics:
                            log_event(
                                event_log,
                                f"run completed: parallelism={parallelism}, search_clients={sc}, "
                                f"attempt={attempt}, metrics_found={len(metrics)}",
                            )
                        else:
                            log_event(
                                event_log,
                                f"WARNING: run completed but no metrics parsed: "
                                f"parallelism={parallelism}, search_clients={sc}, attempt={attempt}",
                            )
                    else:
                        note_parts.append("results_file_missing")
                        log_event(
                            event_log,
                            f"WARNING: results file missing: parallelism={parallelism}, "
                            f"search_clients={sc}, attempt={attempt}",
                        )

                row = {
                    "timestamp_utc": utc_ts(),
                    "target_host": args.target_host,
                    "search_clients": sc,
                    "parallelism": parallelism,
                    "attempt": attempt,
                    "timed_out": timed_out,
                    "return_code": rc,
                    "results_file": str(results_csv),
                    "note": " | ".join(note_parts),
                }
                row.update(metrics)
                append_master_row(master_csv, row)

                if dump_dir:
                    dump_attempt_artifacts(
                        dump_dir,
                        label,
                        cmd,
                        stdout,
                        stderr,
                        results_csv,
                        metrics,
                        row["note"],
                    )

                if timed_out:
                    if args.max_retries and attempt >= args.max_retries:
                        log_event(
                            event_log,
                            f"giving up after {attempt} retries: parallelism={parallelism}, search_clients={sc}",
                        )
                        break
                    log_event(
                        event_log,
                        f"retrying run: parallelism={parallelism}, search_clients={sc}, "
                        f"next_attempt={attempt + 1}",
                    )
                    continue

                break

            is_last_combo = (p_idx == len(parallelism_values) - 1) and (s_idx == len(search_clients) - 1)
            if not is_last_combo:
                log_event(event_log, f"pausing {args.pause_seconds}s before next run")
                time.sleep(args.pause_seconds)

    log_event(event_log, "script finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())