"""Strict, versioned JSON plans for the built-in corpus pipeline steps."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import InputError
from .pipeline import PipelineStep, drop_fields, rename_field, select_fields

PLAN_FORMAT = "corpusledger.pipeline-plan.v1"
_KINDS = frozenset({"select", "drop", "rename"})


@dataclass(frozen=True, slots=True)
class PlanStep:
    """One serializable built-in transformation."""

    kind: str
    fields: tuple[str, ...] = ()
    old: str | None = None
    new: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in _KINDS:
            raise ValueError(f"unknown pipeline plan step kind: {self.kind!r}")
        if self.kind in {"select", "drop"}:
            if not self.fields or any(not isinstance(field, str) or not field for field in self.fields):
                raise ValueError(f"{self.kind} plan step requires non-empty fields")
            if len(set(self.fields)) != len(self.fields):
                raise ValueError(f"{self.kind} plan step fields must be unique")
            if self.old is not None or self.new is not None:
                raise ValueError(f"{self.kind} plan step does not accept old/new")
        else:
            if (
                self.fields
                or not isinstance(self.old, str)
                or not self.old
                or not isinstance(self.new, str)
                or not self.new
            ):
                raise ValueError("rename plan step requires old and new fields")
            if self.old == self.new:
                raise ValueError("rename plan step requires distinct non-empty fields")

    def compile(self) -> PipelineStep:
        """Compile to the same checked callable used by the imperative API."""

        if self.kind == "select":
            return select_fields(self.fields)
        if self.kind == "drop":
            return drop_fields(self.fields)
        return rename_field(self.old or "", self.new or "")

    def to_dict(self) -> dict[str, Any]:
        if self.kind == "rename":
            return {"kind": self.kind, "old": self.old, "new": self.new}
        return {"kind": self.kind, "fields": list(self.fields)}


@dataclass(frozen=True, slots=True)
class PipelinePlan:
    """An immutable, versioned sequence of serializable pipeline steps."""

    steps: tuple[PlanStep, ...]

    def __post_init__(self) -> None:
        if not self.steps:
            raise ValueError("pipeline plan requires at least one step")
        if not all(isinstance(step, PlanStep) for step in self.steps):
            raise TypeError("pipeline plan steps must be PlanStep values")

    def compile(self) -> tuple[PipelineStep, ...]:
        return tuple(step.compile() for step in self.steps)

    def to_dict(self) -> dict[str, Any]:
        return {"format": PLAN_FORMAT, "steps": [step.to_dict() for step in self.steps]}

    def save(self, path: str | Path) -> None:
        """Write canonical UTF-8 JSON and create parent directories."""

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def from_dict(cls, value: object) -> PipelinePlan:
        if not isinstance(value, dict):
            raise ValueError("pipeline plan must be an object")
        if value.get("format") != PLAN_FORMAT:
            raise ValueError(f"pipeline plan format must be {PLAN_FORMAT!r}")
        raw_steps = value.get("steps")
        if not isinstance(raw_steps, list):
            raise ValueError("pipeline plan steps must be an array")
        steps: list[PlanStep] = []
        for position, raw in enumerate(raw_steps, 1):
            if not isinstance(raw, dict):
                raise ValueError(f"pipeline plan step {position} must be an object")
            kind = raw.get("kind")
            if not isinstance(kind, str):
                raise ValueError(f"pipeline plan step {position} kind must be a string")
            allowed = {"kind", "fields"} if kind in {"select", "drop"} else {"kind", "old", "new"}
            unknown = set(raw) - allowed
            if unknown:
                raise ValueError(f"pipeline plan step {position} has unknown fields: {', '.join(sorted(unknown))}")
            try:
                if kind in {"select", "drop"}:
                    fields = raw.get("fields")
                    if not isinstance(fields, list) or not all(isinstance(item, str) for item in fields):
                        raise ValueError("fields must be an array of strings")
                    steps.append(PlanStep(kind, tuple(fields)))
                else:
                    steps.append(PlanStep(kind, old=raw.get("old"), new=raw.get("new")))
            except (TypeError, ValueError) as error:
                raise ValueError(f"invalid pipeline plan step {position}: {error}") from error
        return cls(tuple(steps))

    @classmethod
    def load(cls, path: str | Path) -> PipelinePlan:
        source = Path(path)
        try:
            value = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise InputError(f"cannot load pipeline plan {source}: {error}") from error
        try:
            return cls.from_dict(value)
        except (TypeError, ValueError) as error:
            raise InputError(f"invalid pipeline plan {source}: {error}") from error


def load_pipeline_plan(path: str | Path) -> PipelinePlan:
    """Convenience wrapper for callers that prefer a function API."""

    return PipelinePlan.load(path)
