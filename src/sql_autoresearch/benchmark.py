from __future__ import annotations

import statistics
from dataclasses import dataclass

from sql_autoresearch.adapters.postgres import time_query

# Improvement thresholds: candidate must be both
#   ≥10% faster AND ≥50ms faster than current_best
RELATIVE_THRESHOLD = 0.10
ABSOLUTE_THRESHOLD_MS = 50.0


@dataclass
class BenchmarkResult:
    baseline_median_ms: float
    candidate_median_ms: float
    baseline_timings: list[float]
    candidate_timings: list[float]
    is_faster: bool


def benchmark_pair(
    cur,
    baseline_sql: str,
    candidate_sql: str,
) -> BenchmarkResult:
    """Interleaved benchmark of baseline vs candidate.

    Protocol:
      1. Warm both (1 run each)
      2. Three interleaved pairs: B,C / C,B / B,C
      3. Compare medians
      4. Candidate must be ≥10% AND ≥50ms faster

    Returns BenchmarkResult with median timings and is_faster flag.
    """
    # Warm-up (discard timings)
    time_query(cur, baseline_sql)
    time_query(cur, candidate_sql)

    # Interleaved: B,C / C,B / B,C
    baseline_timings: list[float] = []
    candidate_timings: list[float] = []

    # Pair 1: B, C
    baseline_timings.append(time_query(cur, baseline_sql))
    candidate_timings.append(time_query(cur, candidate_sql))

    # Pair 2: C, B
    candidate_timings.append(time_query(cur, candidate_sql))
    baseline_timings.append(time_query(cur, baseline_sql))

    # Pair 3: B, C
    baseline_timings.append(time_query(cur, baseline_sql))
    candidate_timings.append(time_query(cur, candidate_sql))

    baseline_median = statistics.median(baseline_timings)
    candidate_median = statistics.median(candidate_timings)

    is_faster = _is_improvement(baseline_median, candidate_median)

    return BenchmarkResult(
        baseline_median_ms=baseline_median,
        candidate_median_ms=candidate_median,
        baseline_timings=baseline_timings,
        candidate_timings=candidate_timings,
        is_faster=is_faster,
    )


def _is_improvement(baseline_ms: float, candidate_ms: float) -> bool:
    """Check if candidate is a meaningful improvement over baseline.

    Both conditions must hold:
      - candidate is ≥10% faster (relative)
      - candidate is ≥50ms faster (absolute)
    """
    if baseline_ms <= 0:
        return False
    speedup_ratio = (baseline_ms - candidate_ms) / baseline_ms
    absolute_improvement = baseline_ms - candidate_ms
    return (
        speedup_ratio >= RELATIVE_THRESHOLD
        and absolute_improvement >= ABSOLUTE_THRESHOLD_MS
    )
