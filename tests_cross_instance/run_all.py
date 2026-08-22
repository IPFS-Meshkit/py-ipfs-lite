#!/usr/bin/env python3
"""Run all cross-instance tests in order."""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests_cross_instance import test_01_swarm
from tests_cross_instance import test_02_content
from tests_cross_instance import test_03_dag
from tests_cross_instance import test_04_car
from tests_cross_instance import test_05_pin_gc
from tests_cross_instance import test_06_ipns
from tests_cross_instance import test_07_block
from tests_cross_instance import test_08_diagnostics

tests = [
    ("01 Swarm", test_01_swarm),
    ("02 Content", test_02_content),
    ("03 DAG", test_03_dag),
    ("04 CAR", test_04_car),
    ("05 Pin/GC", test_05_pin_gc),
    ("06 IPNS", test_06_ipns),
    ("07 Block", test_07_block),
    ("08 Diagnostics", test_08_diagnostics),
]

total_passed = 0
total_failed = 0
total_skipped = 0

print()
print("=" * 70)
print("  CROSS-INSTANCE FEATURE TESTS")
print("=" * 70)
print()

for name, module in tests:
    print(f"Running {name}...")
    rc = module.main()
    if rc == 0:
        total_passed += 1
    else:
        total_failed += 1

print()
print("=" * 70)
print("  FINAL SUMMARY")
print("=" * 70)
print(f"  Test groups passed: {len(tests) - total_failed}")
print(f"  Test groups failed: {total_failed}")
print("=" * 70)
print()
