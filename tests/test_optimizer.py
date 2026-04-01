from sql_autoresearch.models import IterationResult, IterationStatus, QueryOutcome
from sql_autoresearch.optimizer import _determine_outcome

_S1 = "SELECT 1"


def _iter(status: IterationStatus, n: int = 1) -> IterationResult:
    return IterationResult(iteration=n, status=status)


def _outcome(iters, final=_S1):
    return _determine_outcome(_S1, final, iters)


class TestDetermineOutcome:
    def test_optimized_when_sql_changed(self):
        iters = [_iter(IterationStatus.KEPT)]
        assert _outcome(iters, "SELECT 2") == QueryOutcome.OPTIMIZED

    def test_unchanged_when_verified_but_not_faster(self):
        iters = [_iter(IterationStatus.DISCARDED_SLOWER)]
        assert _outcome(iters) == QueryOutcome.UNCHANGED

    def test_verification_failed_on_mismatch(self):
        iters = [_iter(IterationStatus.FAILED_MISMATCH)]
        assert _outcome(iters) == QueryOutcome.VERIFICATION_FAILED

    def test_verification_tie_all_tie_reorder(self):
        iters = [
            _iter(IterationStatus.FAILED_TIE_REORDER, 1),
            _iter(IterationStatus.FAILED_TIE_REORDER, 2),
        ]
        assert _outcome(iters) == QueryOutcome.VERIFICATION_TIE

    def test_no_valid_candidate_all_errors(self):
        iters = [
            _iter(IterationStatus.CANDIDATE_ERROR, 1),
            _iter(IterationStatus.CANDIDATE_ERROR, 2),
        ]
        assert _outcome(iters) == QueryOutcome.NO_VALID_CANDIDATE

    def test_no_verified_candidate_mix(self):
        iters = [
            _iter(IterationStatus.FAILED_SAFETY, 1),
            _iter(IterationStatus.CANDIDATE_ERROR, 2),
        ]
        assert _outcome(iters) == QueryOutcome.NO_VERIFIED_CANDIDATE

    def test_mismatch_takes_precedence_over_tie(self):
        iters = [
            _iter(IterationStatus.FAILED_TIE_REORDER, 1),
            _iter(IterationStatus.FAILED_MISMATCH, 2),
        ]
        assert _outcome(iters) == QueryOutcome.VERIFICATION_FAILED

    def test_empty_iterations(self):
        assert _outcome([]) == QueryOutcome.NO_VALID_CANDIDATE
