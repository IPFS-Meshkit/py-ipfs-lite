#!/usr/bin/env python3
"""
Production CPU profiler — injects into a running py-ipfs-lite process.
Captures 30s of cProfile data and outputs the top functions by cumulative time.

Usage: Send SIGUSR2 to the process to trigger profiling, or run standalone.
"""
import cProfile
import pstats
import io
import os
import signal
import sys
import time
import resource
import tracemalloc


def profile_for_seconds(duration=30):
    """Run cProfile for `duration` seconds and print results."""
    pr = cProfile.Profile()
    pr.enable()

    # Also track CPU
    cpu_before = resource.getrusage(resource.RUSAGE_SELF)
    wall_before = time.monotonic()

    time.sleep(duration)

    pr.disable()
    wall_after = time.monotonic()
    cpu_after = resource.getrusage(resource.RUSAGE_SELF)

    wall_elapsed = wall_after - wall_before
    cpu_user = cpu_after.ru_utime - cpu_before.ru_utime
    cpu_sys = cpu_after.ru_stime - cpu_before.ru_stime

    print(f"\n{'='*70}")
    print(f"  PROFILING RESULTS ({duration}s)")
    print(f"{'='*70}")
    print(f"  Wall time: {wall_elapsed:.1f}s")
    print(f"  CPU user:  {cpu_user:.2f}s")
    print(f"  CPU sys:   {cpu_sys:.2f}s")
    print(f"  CPU total: {cpu_user+cpu_sys:.2f}s ({(cpu_user+cpu_sys)/wall_elapsed*100:.1f}%)")
    print()

    # Print top functions by cumulative time
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s)
    ps.sort_stats('cumulative')

    print("  TOP 30 BY CUMULATIVE TIME:")
    print("-" * 70)
    ps.print_stats(30)
    print(s.getvalue())

    # Also print by total time (self time)
    s2 = io.StringIO()
    ps2 = pstats.Stats(pr, stream=s2)
    ps2.sort_stats('tottime')

    print("  TOP 30 BY TOTAL (SELF) TIME:")
    print("-" * 70)
    ps2.print_stats(30)
    print(s2.getvalue())

    # Print callers of top functions
    s3 = io.StringIO()
    ps3 = pstats.Stats(pr, stream=s3)
    ps3.sort_stats('tottime')
    ps3.print_callers(10)

    print("  TOP CALLERS OF HOT FUNCTIONS:")
    print("-" * 70)
    print(s3.getvalue())

    # Dump raw profile for later analysis
    prof_file = f"/tmp/prod_profile_{os.getpid()}.prof"
    pr.dump_stats(prof_file)
    print(f"  Raw profile saved to: {prof_file}")

    # Also get a tracemalloc snapshot
    if tracemalloc.is_tracing():
        snapshot = tracemalloc.take_snapshot()
        snapshot_file = f"/tmp/prod_tracemalloc_{os.getpid()}.pickle"
        with open(snapshot_file, 'wb') as f:
            snapshot.dump(f)
        print(f"  Tracemalloc snapshot saved to: {snapshot_file}")

    print(f"{'='*70}\n")


if __name__ == "__main__":
    # Start tracemalloc
    tracemalloc.start(10)  # 10 frames deep

    # Profile for 30 seconds
    profile_for_seconds(30)
