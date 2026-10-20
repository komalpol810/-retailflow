"""
Quality gate between Silver and Gold.

Runs the Silver quality suite. If every check passes, exits 0 and the
pipeline continues to Gold. If anything fails, it:
  1. moves the Bronze and Silver data into lakehouse/quarantine/<timestamp>/
  2. writes report.json (all check results) next to it
  3. appends a line to lakehouse/quarantine/alerts.log and prints a banner
  4. exits with code 3, which halts run_pipeline.py before Gold runs

Optional argument: path to a different lakehouse folder (used for testing).
"""

import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from quality.silver_suite import get_spark, print_report, run_all  # noqa: E402

QUALITY_FAILURE_EXIT_CODE = 3


def quarantine(lakehouse: Path, results: list) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    qdir = lakehouse / "quarantine" / stamp
    qdir.mkdir(parents=True)

    for layer in ("bronze", "silver"):
        src = lakehouse / layer
        if src.exists():
            shutil.move(str(src), str(qdir / layer))

    failed = [r for r in results if not r["passed"]]
    report = {
        "quarantined_at": stamp,
        "total_checks": len(results),
        "failed_checks": len(failed),
        "failures": failed,
    }
    (qdir / "report.json").write_text(json.dumps(report, indent=2))
    return qdir


def send_alert(lakehouse: Path, qdir: Path, failed: list) -> None:
    message = (
        f"{datetime.now().isoformat(timespec='seconds')} "
        f"QUALITY GATE FAILED: {len(failed)} check(s) failed. "
        f"Batch quarantined at {qdir}"
    )
    log = lakehouse / "quarantine" / "alerts.log"
    with open(log, "a") as f:
        f.write(message + "\n")

    print("\n" + "!" * 70)
    print("ALERT: SILVER QUALITY GATE FAILED - GOLD WAS NOT PUBLISHED")
    print("!" * 70)
    for r in failed:
        print(f"  - {r['table']}: {r['check']}  {r['detail']}")
    print(f"\nBatch moved to: {qdir}")
    print(f"Details: {qdir / 'report.json'}")
    print(f"Alert log: {log}")


def main() -> int:
    lakehouse = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "lakehouse"

    spark = get_spark()
    results = run_all(spark, silver_dir=str(lakehouse / "silver"))
    spark.stop()  # release file handles before any move

    print_report(results)
    failed = [r for r in results if not r["passed"]]

    if not failed:
        print("\nQUALITY GATE PASSED - Gold may proceed.")
        return 0

    qdir = quarantine(lakehouse, results)
    send_alert(lakehouse, qdir, failed)
    return QUALITY_FAILURE_EXIT_CODE


if __name__ == "__main__":
    sys.exit(main())
