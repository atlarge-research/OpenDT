#!/usr/bin/env python3
"""Verify OpenDT development environment setup."""

import sys
from pathlib import Path


def check_imports():
    """Check that all required packages can be imported."""
    errors = []

    packages = [
        ("opendt_common", "Shared library"),
        ("pandas", "Data processing"),
        ("pyarrow", "Parquet support"),
        ("yaml", "YAML config"),
        ("pydantic", "Data validation"),
        ("kafka", "Kafka client"),
    ]

    print("Checking package imports...")
    for pkg, desc in packages:
        try:
            __import__(pkg)
            print(f"  ✅ {pkg:20s} - {desc}")
        except ImportError as e:
            print(f"  ❌ {pkg:20s} - {desc} - {e}")
            errors.append(pkg)

    return errors


def check_data_files():
    """Check that required data files exist."""
    errors = []

    data_dir = Path(__file__).parent.parent / "data" / "SURF"
    files = ["tasks.parquet", "fragments.parquet", "consumption.parquet"]

    print("\nChecking data files...")
    for filename in files:
        filepath = data_dir / filename
        if filepath.exists():
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"  ✅ {filename:25s} - {size_mb:.2f} MB")
        else:
            print(f"  ❌ {filename:25s} - Not found")
            errors.append(filename)

    return errors


def check_models():
    """Check that models can parse data."""
    try:
        import pandas as pd
        from opendt_common import Consumption, Fragment, Task

        print("\nChecking model parsing...")

        data_dir = Path(__file__).parent.parent / "data" / "SURF"

        # Test task parsing
        tasks_file = data_dir / "tasks.parquet"
        if tasks_file.exists():
            df = pd.read_parquet(tasks_file)
            task = Task(**df.iloc[0].to_dict())
            print(f"  ✅ Task model - Parsed task {task.id}")
        else:
            print("  ⚠️  Task model - Data file missing")

        # Test fragment parsing
        fragments_file = data_dir / "fragments.parquet"
        if fragments_file.exists():
            df = pd.read_parquet(fragments_file)
            fragment = Fragment(**df.iloc[0].to_dict())
            print(f"  ✅ Fragment model - Parsed fragment for task {fragment.task_id}")
        else:
            print("  ⚠️  Fragment model - Data file missing")

        # Test consumption parsing
        consumption_file = data_dir / "consumption.parquet"
        if consumption_file.exists():
            df = pd.read_parquet(consumption_file)
            consumption = Consumption(**df.iloc[0].to_dict())
            print(f"  ✅ Consumption model - Parsed record at {consumption.timestamp}")
        else:
            print("  ⚠️  Consumption model - Data file missing")

        return []

    except Exception as e:
        print(f"  ❌ Model parsing failed: {e}")
        return [str(e)]


def main():
    """Run all verification checks."""
    print("=" * 60)
    print("OpenDT Development Environment Verification")
    print("=" * 60)

    errors = []

    errors.extend(check_imports())
    errors.extend(check_data_files())
    errors.extend(check_models())

    print("\n" + "=" * 60)
    if errors:
        print(f"❌ Verification failed with {len(errors)} error(s)")
        print("\nTo fix:")
        print("  1. Run: make setup")
        print("  2. Activate: source .venv/bin/activate")
        print("  3. Copy data: cp surf-workload/*.parquet data/SURF/")
        sys.exit(1)
    else:
        print("✅ All checks passed! Environment is ready.")
        sys.exit(0)


if __name__ == "__main__":
    main()
