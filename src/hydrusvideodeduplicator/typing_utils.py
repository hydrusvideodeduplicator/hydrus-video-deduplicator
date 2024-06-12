from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ValueRange:
    lo: int
    hi: int
