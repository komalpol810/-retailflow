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

PROJECT_ROOT = Path(__file__).parent
SPARK_DIR = PROJECT_ROOT / "spark"
LAKEHOUSE_LAYERS = ["bronze", "silver", "gold"]

# Order matters: bronze must finish before silver reads it, etc.
# Diagnostic/one-off scripts (00_smoke_test, 04a-04e) are excluded -
# they were investigation tools, not pipeline stages.
STAGES = [
    ("Bronze", "01_bronze_ingest.py"),
    ("Silver", "02_silver_transform.py"),
    ("Quality gate - Silver", "02b_silver_quality_gate.py"),
    ("Gold - Type-1 dimensions", "03_gold_dimensions.py"),
    ("Gold - dim_customer (SCD2)", "04_gold_dim_customer_scd2.py"),
    ("Gold - fct_order_items", "05_gold_fct_order_items.py"),
]


def check_lakehouse_is_clean() -> None:
    """Bronze is append-only, so rerunning on top of existing data silently
    multiplies row counts downstream. Refuse to run unless the layers are empty."""
    dirty = []
    for layer in LAKEHOUSE_LAYERS:
        layer_dir = PROJECT_ROOT / "lakehouse" / layer
        if layer_dir.exists() and any(layer_dir.iterdir()):
            dirty.append(str(layer_dir.relative_to(PROJECT_ROOT)))

    if dirty:
        print("REFUSING TO RUN: these folders already contain data:")
        for d in dirty:
            print(f"  - {d}")
        print("Bronze is append-only, so a rerun would duplicate rows.")
        print("Wipe first, then run again:")
        print("  rm -rf lakehouse/bronze lakehouse/silver lakehouse/gold")
        sys.exit(1)


def run_stage(label: str, script_name: str) -> None:
    script_path = SPARK_DIR / script_name
    print(f"\n{'=' * 60}")
    print(f"STAGE: {label}  ({script_name})")
    print(f"{'=' * 60}")

    start = time.time()
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_ROOT,  # run from project root, same as manual runs
    )
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\nFAILED: {label} exited with code {result.returncode} after {elapsed:.1f}s")
        print("Pipeline halted - later stages were not run.")
        sys.exit(result.returncode)

    print(f"\nOK: {label} completed in {elapsed:.1f}s")


def main() -> None:
    check_lakehouse_is_clean()

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
