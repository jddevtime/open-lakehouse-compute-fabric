# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Versioned compatibility analyzer for Fabric notebooks (rules in ``rules/fabric-2.0.yaml``, engine in :mod:`olcf.fabric.analyzer.engine`)."""

from __future__ import annotations

from olcf.fabric.analyzer.engine import (
    ANALYZER_VERSION,
    AnalysisOptions,
    AnalysisResult,
    analyze,
    analyze_text,
)
from olcf.fabric.analyzer.rules import CLASSES, RuleSet, load_ruleset

__all__ = ["ANALYZER_VERSION", "CLASSES", "AnalysisOptions", "AnalysisResult", "RuleSet", "analyze", "analyze_text", "load_ruleset"]
