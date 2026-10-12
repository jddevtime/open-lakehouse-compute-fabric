# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Metadata persistence (SQLite for all-in-one installs, PostgreSQL for production)."""

from olcf.db.engine import Database

__all__ = ["Database"]
