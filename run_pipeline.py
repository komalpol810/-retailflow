"""
Phase 3 completion script: chains Bronze -> Silver -> Gold end-to-end.

Each stage is run as a subprocess (not imported) because 01-05 are all
top-level scripts, not functions. Running as a subprocess also mirrors
how a real orchestrator (Airflow, etc.) executes each task in Phase 4 -
a clean process per stage, and a failure in one stage can't leave
half-initialized state that silently affects the next stage.
"""

import subprocess
import sys
import time
from pathlib import Path

SPARK_DIR = Path(__file__).parent / "spark"

# Order matters: bronze must finish before silver reads it, etc.
# Diagnostic/one-off scripts (00_smoke_test, 04a-04e) are excluded -
# they were investigation tools, not pipeline stages.
STAGES = [
    ("Bronze", "01_bronze_ingest.py"),
    ("Silver", "02_silver_transform.py"),
    ("Gold - Type-1 dimensions", "03_gold_dimensions.py"),
    ("Gold - dim_customer (SCD2)", "04_gold_dim_customer_scd2.py"),
    ("Gold - fct_order_items", "05_gold_fct_order_items.py"),
]


def run_stage(label: str, script_name: str) -> None:
    script_path = SPARK_DIR / script_name
    print(f"\n{'=' * 60}")
    print(f"STAGE: {label}  ({script_name})")
    print(f"{'=' * 60}")

    start = time.time()
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=Path(__file__).parent,  # run from project root, same as manual runs
    )
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\nFAILED: {label} exited with code {result.returncode} after {elapsed:.1f}s")
        print("Pipeline halted - later stages were not run.")
        sys.exit(result.returncode)

    print(f"\nOK: {label} completed in {elapsed:.1f}s")


def main() -> None:
    pipeline_start = time.time()
    print(f"Starting RetailFlow pipeline: {len(STAGES)} stages")

    for label, script_name in STAGES:
        run_stage(label, script_name)

    total_elapsed = time.time() - pipeline_start
    print(f"\n{'=' * 60}")
    print(f"PIPELINE COMPLETE - all {len(STAGES)} stages passed in {total_elapsed:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
