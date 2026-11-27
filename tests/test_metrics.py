import numpy as np
import pytest

from fraud_lakehouse.metrics import (
    average_precision,
    best_threshold,
    confusion,
    pr_curve,
    roc_auc,
    roc_curve,
    summary,
)

LABELS = np.array([1, 0, 1, 1, 0, 0])
SCORES = np.array([0.9, 0.8, 0.6, 0.2, 0.1, 0.05])


def test_confusion_counts_rows_at_or_above_the_threshold_as_flagged():
    counts = confusion(LABELS, SCORES, threshold=0.5)

    assert counts == {"tp": 2, "fp": 1, "fn": 1, "tn": 2}


def test_summary_derives_precision_recall_and_f1_from_the_counts():
    metrics = summary(LABELS, SCORES, threshold=0.5)

    assert metrics["precision"] == pytest.approx(2 / 3)
    assert metrics["recall"] == pytest.approx(2 / 3)
    assert metrics["f1"] == pytest.approx(2 / 3)


def test_summary_reports_zero_precision_when_nothing_is_flagged():
    metrics = summary(LABELS, SCORES, threshold=0.99)

    assert metrics["precision"] == 0.0
    assert metrics["recall"] == 0.0
    assert metrics["f1"] == 0.0


def test_average_precision_is_one_for_a_perfect_ranking():
    labels = np.array([1, 1, 0, 0, 0])
    scores = np.array([0.9, 0.8, 0.3, 0.2, 0.1])

    assert average_precision(labels, scores) == pytest.approx(1.0)


def test_average_precision_weights_precision_by_each_recall_step():
    # Frauds found at ranks 1, 3 and 4: precision 1, 2/3 and 3/4, each worth a third of recall.
    assert average_precision(LABELS, SCORES) == pytest.approx((1 + 2 / 3 + 3 / 4) / 3)


def test_roc_auc_is_the_share_of_fraud_legit_pairs_ranked_correctly():
    # Pairs (fraud, legit) with fraud scored higher: 0.9 beats 3, 0.6 beats 2, 0.2 beats 2 → 7 of 9.
    assert roc_auc(LABELS, SCORES) == pytest.approx(7 / 9)


def test_roc_auc_is_one_half_when_every_score_ties():
    assert roc_auc(LABELS, np.full(6, 0.5)) == pytest.approx(0.5)


def test_tied_scores_form_one_curve_point():
    labels = np.array([1, 0, 1, 0])
    scores = np.array([0.7, 0.7, 0.4, 0.4])

    precision, recall, thresholds = pr_curve(labels, scores)

    assert thresholds.tolist() == [0.7, 0.4]
    assert precision.tolist() == [0.5, 0.5]
    assert recall.tolist() == [0.5, 1.0]


def test_roc_curve_starts_at_the_origin_and_ends_at_one_one():
    fpr, tpr = roc_curve(LABELS, SCORES)

    assert (fpr[0], tpr[0]) == (0.0, 0.0)
    assert (fpr[-1], tpr[-1]) == (1.0, 1.0)
    assert np.all(np.diff(fpr) >= 0) and np.all(np.diff(tpr) >= 0)


def test_best_threshold_maximises_f1():
    labels = np.array([1, 1, 1, 0, 0, 0, 0])
    scores = np.array([0.95, 0.9, 0.85, 0.6, 0.55, 0.3, 0.1])

    assert best_threshold(labels, scores) == pytest.approx(0.85)


def test_best_threshold_works_on_any_score_scale():
    labels = np.array([0, 1, 1, 0, 0])
    amounts = np.array([12.0, 950.0, 720.0, 640.0, 30.0])

    assert best_threshold(labels, amounts) == pytest.approx(720.0)


def test_best_threshold_needs_at_least_one_fraud():
    with pytest.raises(ValueError, match="no fraud"):
        best_threshold(np.array([0, 0]), np.array([0.2, 0.8]))
