"""Small statistics helpers shared by the evaluation scripts.

Everything here is paired / resampling based so it stays valid for the
bounded, non-normal per-question scores the benchmark produces, and it only
needs numpy.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np


def bootstrap_ci(values: Sequence[float], n_boot: int = 10000, alpha: float = 0.05, seed: int = 0) -> tuple[float, float, float]:
    """Return (mean, lower, upper) with a percentile bootstrap interval."""
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return float("nan"), float("nan"), float("nan")
    if arr.size == 1:
        return float(arr[0]), float(arr[0]), float(arr[0])
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, arr.size, size=(n_boot, arr.size))
    means = arr[idx].mean(axis=1)
    lo, hi = np.quantile(means, [alpha / 2, 1 - alpha / 2])
    return float(arr.mean()), float(lo), float(hi)


def paired_permutation_test(a: Sequence[float], b: Sequence[float], n_perm: int = 20000, seed: int = 0) -> float:
    """Two-sided sign-flip permutation p-value for mean(a - b) on paired samples."""
    diff = np.asarray(a, dtype=float) - np.asarray(b, dtype=float)
    if diff.size == 0:
        return float("nan")
    if not np.any(diff):
        return 1.0
    observed = abs(diff.mean())
    rng = np.random.default_rng(seed)
    signs = rng.choice([-1.0, 1.0], size=(n_perm, diff.size))
    permuted = np.abs((signs * diff).mean(axis=1))
    # +1 correction keeps the Monte Carlo p-value strictly positive.
    return float((np.sum(permuted >= observed - 1e-12) + 1) / (n_perm + 1))


def holm_adjust(p_values: Sequence[float]) -> list[float]:
    """Holm-Bonferroni step-down adjustment (NaN entries are left untouched)."""
    adjusted = [float("nan")] * len(p_values)
    valid = sorted((p, i) for i, p in enumerate(p_values) if p == p)
    running = 0.0
    for rank, (p, i) in enumerate(valid):
        running = max(running, min(1.0, (len(valid) - rank) * p))
        adjusted[i] = running
    return adjusted


def prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    precision = tp / (tp + fp) if (tp + fp) else float("nan")
    recall = tp / (tp + fn) if (tp + fn) else float("nan")
    if precision != precision or recall != recall or (precision + recall) == 0:
        return precision, recall, float("nan")
    return precision, recall, 2 * precision * recall / (precision + recall)


def cohen_kappa(labels_a: Sequence, labels_b: Sequence, weights: str | None = None) -> float:
    """Cohen's kappa for two raters; ``weights='quadratic'`` for ordinal scales."""
    if len(labels_a) != len(labels_b):
        raise ValueError("두 평가자의 항목 수가 다릅니다.")
    if not labels_a:
        return float("nan")
    categories = sorted(set(labels_a) | set(labels_b))
    if len(categories) < 2:
        return float("nan")  # no variation: agreement is undefined, not perfect
    index = {c: i for i, c in enumerate(categories)}
    k = len(categories)
    observed = np.zeros((k, k))
    for x, y in zip(labels_a, labels_b):
        observed[index[x], index[y]] += 1
    observed /= observed.sum()
    expected = np.outer(observed.sum(axis=1), observed.sum(axis=0))
    if weights == "quadratic":
        positions = np.asarray([float(c) for c in categories])
        span = positions.max() - positions.min()
        disagreement = ((positions[:, None] - positions[None, :]) / span) ** 2
    else:
        disagreement = 1.0 - np.eye(k)
    denom = float((disagreement * expected).sum())
    if denom == 0:
        return float("nan")
    return 1.0 - float((disagreement * observed).sum()) / denom
