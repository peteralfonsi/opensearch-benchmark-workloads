#!/usr/bin/env python3
"""
Iterated OpenSearch Benchmark (OSB) runner with exhaustive debugging output.

This version:
- Enforces timeout via Python
- Always uses --kill-running-processes
- Captures stdout/stderr separately
- Strips ANSI escape codes
- Falls back to stderr if stdout has no table
- Writes a detailed per-run "results dump" for debugging
- Writes clean CSV output
- Writes a concise event log
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
from typing import Dict, Any, List, Tuple


DEFAULT_WORKLOAD_PATH = "/home/ec2-user/osb/opensearch-benchmark-workloads/big5"
DEFAULT_INCLUDE_TASKS = "term"

PIPE_ROW_RE = re.compile(
    r"^\|\s*(?P<metric>.*?)\s*\|\s*(?P<op>.*?)\s*\|\s*(?P<value>.*?)\s*\|\s*(?P<unit>.*?)\s*\|\s*$"
)

PERCENTILE_RE = re.compile(
    r"^\s*(?P<pct>\d+)(?:st|nd|rd|th)\s+percentile\s+(?P<what>.+?)\s*$",
    re.I,
)

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


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


def make_column_name(op: str, metric: str, unit: str) -> str:
    unit_n = norm(unit)
    m = PERCENTILE_RE.match(metric)
    if m:
        base = f"p{m.group('pct')}_{norm(m.group('what'))}_{unit_n}"
    else:
        base = f"{norm(metric)}_{unit_n}"

    op_n = norm(op)
    return f"{op_n}__{base}" if op_n else base


def parse_osb_table(text: str) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    for line in text.splitlines():
        m = PIPE_ROW_RE.match(line)
        if not m:
            continue

        metric_l = m.group("metric").lower()
        if (
            "percentile service time" not in metric_l
            and "throughput" not in metric_l
            and metric_l != "error rate"
        ):
            continue

        key = make_column_name(
            m.group("op"),
            m.group("metric"),
            m.group("unit"),
        )
        metrics[key] = try_float(m.group("value").strip())

    return metrics


def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not path.exists():
        return [], []
    with path.open() as f:
        reader = csv.DictReader(f)
        return reader.fieldnames or [], list(reader)


def write_csv(path: Path, header: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in header})


def append_row(csv_path: Path, row: Dict[str, Any]) -> None:
    header, rows = read_csv(csv_path)
    new_cols = [k for k in row if k not in header]

    if not header:
        write_csv(csv_path, sorted(row), [row])
        return

    if new_cols:
        header = header + sorted(new_cols)
        rows.append(row)
        write_csv(csv_path, header, rows)
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
) -> List[str]:
    params = {
        "number_of_replicas": "0",
        "search_clients": str(search_clients),
        "target_throughput": "",
        "test_iterations": test_iterations,
    }

    return [
        osb_bin,
        "run",
        "--kill-running-processes",
        "--report-format=table",
        f"--workload-path={workload_path}",
        f"--target-host={target_host}",
        f"--workload-params={json.dumps(params, separators=(',', ':'))}",
        f"--include-tasks={include_tasks}",
    ]


def run_osb(cmd: List[str], timeout_s: int) -> Tuple[bool, int, str, str]:
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


def dump_results(
    base_dir: Path,
    label: str,
    cmd: List[str],
    stdout_raw: str,
    stderr_raw: str,
    parser_input: str,
    metrics: Dict[str, Any],
) -> None:
    d = base_dir / label
    d.mkdir(parents=True, exist_ok=True)

    (d / "cmd.txt").write_text(" ".join(cmd) + "\n")
    (d / "stdout.raw.txt").write_text(stdout_raw)
    (d / "stderr.raw.txt").write_text(stderr_raw)

    combined = (
        "===== STDOUT (ANSI STRIPPED) =====\n"
        + strip_ansi(stdout_raw)
        + "\n\n===== STDERR (ANSI STRIPPED) =====\n"
        + strip_ansi(stderr_raw)
    )
    (d / "combined.stripped.txt").write_text(combined)
    (d / "parser_input.txt").write_text(parser_input)

    if metrics:
        (d / "parsed_metrics.json").write_text(json.dumps(metrics, indent=2))
    else:
        (d / "parsed_metrics.json").write_text(
            json.dumps({"note": "no metrics parsed"}, indent=2)
        )


def parse_search_clients(values: List[str]) -> List[int]:
    out: List[int] = []
    for v in values:
        for p in v.split(","):
            out.append(int(p.strip()))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-host", required=True)
    ap.add_argument("--search-clients", nargs="+", required=True)
    ap.add_argument("--test-iterations", type=int, default=1000)
    ap.add_argument("--timeout-minutes", type=int, default=20)
    ap.add_argument("--pause-seconds", type=int, default=180)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--csv-path", default="osb_results.csv")
    ap.add_argument("--event-log", default="osb_events.log")
    ap.add_argument("--results-dump-dir", default="osb_results_dump")
    ap.add_argument("--osb-bin", default="opensearch-benchmark")
    ap.add_argument("--workload-path", default=DEFAULT_WORKLOAD_PATH)
    ap.add_argument("--include-tasks", default=DEFAULT_INCLUDE_TASKS)
    args = ap.parse_args()

    search_clients_list = parse_search_clients(args.search_clients)
    timeout_s = args.timeout_minutes * 60
    csv_path = Path(args.csv_path)
    event_log = Path(args.event_log)
    dump_dir = Path(args.results_dump_dir)

    log_event(event_log, "script started")

    for idx, sc in enumerate(search_clients_list):
        attempt = 0
        while True:
            attempt += 1
            log_event(event_log, f"starting run: search_clients={sc}, attempt={attempt}")
            start_ts = utc_ts()

            cmd = build_command(
                args.osb_bin,
                args.workload_path,
                args.target_host,
                sc,
                args.test_iterations,
                args.include_tasks,
            )

            timed_out, rc, stdout_raw, stderr_raw = run_osb(cmd, timeout_s)
            end_ts = utc_ts()

            stdout = strip_ansi(stdout_raw)
            stderr = strip_ansi(stderr_raw)

            metrics = {}
            parser_input = ""

            if not timed_out:
                metrics = parse_osb_table(stdout)
                parser_input = stdout
                if not metrics:
                    metrics = parse_osb_table(stderr)
                    parser_input = stderr

            label = f"sc{sc}_attempt{attempt}_{int(time.time())}"
            dump_results(
                dump_dir,
                label,
                cmd,
                stdout_raw,
                stderr_raw,
                parser_input,
                metrics,
            )

            row = {
                "start_ts_utc": start_ts,
                "end_ts_utc": end_ts,
                "target_host": args.target_host,
                "search_clients": sc,
                "test_iterations": args.test_iterations,
                "attempt": attempt,
                "timed_out": timed_out,
                "return_code": rc,
            }
            row.update(metrics)
            append_row(csv_path, row)

            if timed_out:
                log_event(event_log, f"run timed out: search_clients={sc}, attempt={attempt}")
                if args.max_retries and attempt >= args.max_retries:
                    log_event(event_log, f"giving up after {attempt} timeouts: search_clients={sc}")
                    break
                continue

            log_event(event_log, f"run completed: search_clients={sc}, attempt={attempt}")
            if idx < len(search_clients_list) - 1:
                time.sleep(args.pause_seconds)
            break

    log_event(event_log, "script finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
