from dataclasses import dataclass
from typing import Any, LiteralString, Optional, Sequence


@dataclass(kw_only=True, frozen=True)
class Predicate:
    name: str
    value: Any
    maximum: Optional[Any] = None
    minimum: Optional[Any] = None


@dataclass(kw_only=True, frozen=True)
class Query:
    query_str: LiteralString
    predicates: Sequence[Predicate]
