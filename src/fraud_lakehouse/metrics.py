"""Classification metrics on (label, score) arrays: any score scale, ties handled as one point."""

import numpy as np


def _cumulative_counts(labels: np.ndarray, scores: np.ndarray):
    """Frauds and flagged rows at each distinct threshold, from the highest score down."""
    order = np.argsort(-scores, kind="stable")
    sorted_scores, sorted_labels = scores[order], labels[order]
    # A threshold flags every row scored at or above it, so tied rows join together.
    last_of_group = np.r_[sorted_scores[1:] != sorted_scores[:-1], True]
    tp = np.cumsum(sorted_labels)[last_of_group]
    flagged = np.arange(1, len(scores) + 1)[last_of_group]
    return sorted_scores[last_of_group], tp, flagged


def pr_curve(labels: np.ndarray, scores: np.ndarray):
    """Precision, recall and threshold at each distinct score, highest threshold first."""
    thresholds, tp, flagged = _cumulative_counts(labels, scores)
    return tp / flagged, tp / labels.sum(), thresholds


def roc_curve(labels: np.ndarray, scores: np.ndarray):
    """False and true positive rates, from (0, 0) to (1, 1)."""
    _, tp, flagged = _cumulative_counts(labels, scores)
    positives = labels.sum()
    fpr = np.r_[0.0, (flagged - tp) / (len(labels) - positives)]
    tpr = np.r_[0.0, tp / positives]
    return fpr, tpr


def average_precision(labels: np.ndarray, scores: np.ndarray) -> float:
    """Area under the PR curve as a step function, the way scikit-learn computes it."""
    precision, recall, _ = pr_curve(labels, scores)
    return float(np.sum(np.diff(np.r_[0.0, recall]) * precision))


def roc_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    fpr, tpr = roc_curve(labels, scores)
    return float(np.trapezoid(tpr, fpr))


def best_threshold(labels: np.ndarray, scores: np.ndarray) -> float:
    """The threshold with the best F1 (highest on a tie); chosen on validation, never on test."""
    positives = labels.sum()
    if positives == 0:
        raise ValueError("no fraud in these labels: F1 is undefined")
    thresholds, tp, flagged = _cumulative_counts(labels, scores)
    f1 = 2 * tp / (flagged + positives)
    return float(thresholds[np.argmax(f1)])


def confusion(labels: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, int]:
    flagged = scores >= threshold
    fraud = labels == 1
    return {
        "tp": int(np.sum(flagged & fraud)),
        "fp": int(np.sum(flagged & ~fraud)),
        "fn": int(np.sum(~flagged & fraud)),
        "tn": int(np.sum(~flagged & ~fraud)),
    }


def summary(labels: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, float]:
    """Precision, recall and F1 at one threshold, plus the confusion counts behind them."""
    counts = confusion(labels, scores, threshold)
    tp, flagged, frauds = counts["tp"], counts["tp"] + counts["fp"], counts["tp"] + counts["fn"]
    precision = tp / flagged if flagged else 0.0
    recall = tp / frauds if frauds else 0.0
    f1 = 2 * tp / (flagged + frauds) if flagged + frauds else 0.0
    return {"precision": precision, "recall": recall, "f1": f1, **counts}
