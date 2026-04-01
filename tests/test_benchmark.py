from unittest.mock import MagicMock, patch

from sql_autoresearch.benchmark import _is_improvement, benchmark_pair


class TestIsImprovement:
    def test_significant_improvement(self):
        # 200ms -> 100ms = 50% improvement, 100ms absolute
        assert _is_improvement(200.0, 100.0) is True

    def test_below_relative_threshold(self):
        # 100ms -> 95ms = 5% improvement (< 10%)
        assert _is_improvement(100.0, 95.0) is False

    def test_below_absolute_threshold(self):
        # 100ms -> 80ms = 20% but only 20ms absolute (< 50ms)
        assert _is_improvement(100.0, 80.0) is False

    def test_meets_both_thresholds(self):
        # 1000ms -> 800ms = 20% improvement, 200ms absolute
        assert _is_improvement(1000.0, 800.0) is True

    def test_candidate_slower(self):
        assert _is_improvement(100.0, 150.0) is False

    def test_zero_baseline(self):
        assert _is_improvement(0.0, 50.0) is False

    def test_exact_threshold(self):
        # 500ms -> 400ms = 20% improvement, 100ms absolute (both >= thresholds)
        assert _is_improvement(500.0, 400.0) is True


class TestBenchmarkPair:
    def test_benchmark_calls_time_query(self):
        cur = MagicMock()
        # warm(2) + 6 timed runs = 8 total calls
        timings = [
            10.0, 8.0,           # warm-up: baseline, candidate
            100.0, 80.0,         # pair 1: B, C
            80.0, 100.0,         # pair 2: C, B
            100.0, 80.0,         # pair 3: B, C
        ]

        with patch(
            "sql_autoresearch.benchmark.time_query",
            side_effect=timings,
        ):
            result = benchmark_pair(cur, "SELECT 1", "SELECT 2")

        assert len(result.baseline_timings) == 3
        assert len(result.candidate_timings) == 3
        assert result.baseline_median_ms == 100.0
        assert result.candidate_median_ms == 80.0
