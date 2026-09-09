"""Local processor DAGs over immutable, typed annotation documents.

Processors are explicit Python callables supplied by the application. Planning
checks the entire schema dependency graph before any processor is invoked.
"""

from __future__ import annotations

import heapq
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import islice
from typing import Any

from .annotations import MAX_ANNOTATIONS, AnnotationDocument, AnnotationType, SpanAnnotation

AnnotationTransform = Callable[[AnnotationDocument], Iterable[SpanAnnotation]]


class AnnotationPipelineError(ValueError):
    """A processor declaration, dependency, or produced annotation is invalid."""


def _token(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(char.isspace() for char in value):
        raise AnnotationPipelineError(f"{label} must be a non-empty token")
    return value


def _types(values: Iterable[AnnotationType], label: str) -> tuple[AnnotationType, ...]:
    materialized = tuple(values)
    if any(not isinstance(value, AnnotationType) for value in materialized):
        raise AnnotationPipelineError(f"{label} must contain AnnotationType values")
    names = [value.name for value in materialized]
    if len(names) != len(set(names)):
        raise AnnotationPipelineError(f"{label} must contain unique type names")
    return tuple(sorted(materialized, key=lambda value: value.name))


@dataclass(frozen=True, slots=True)
class AnnotationProcessor:
    """A versioned callable with complete input and output type declarations.

    ``requires`` declares exact schemas rather than only names. ``produces``
    declares new layers whose annotations this processor exclusively owns.
    Change ``version`` whenever implementation, configuration, or resources change.
    """

    name: str
    version: str
    process: AnnotationTransform
    requires: tuple[AnnotationType, ...] = ()
    produces: tuple[AnnotationType, ...] = ()

    def __post_init__(self) -> None:
        _token(self.name, "processor name")
        _token(self.version, "processor version")
        if not callable(self.process):
            raise AnnotationPipelineError("processor process must be callable")
        object.__setattr__(self, "requires", _types(self.requires, "requires"))
        object.__setattr__(self, "produces", _types(self.produces, "produces"))
        if not self.produces:
            raise AnnotationPipelineError("processor must declare at least one produced type")


@dataclass(frozen=True, slots=True)
class AnnotationStepReport:
    """One completed transformation and its input/output content digests."""

    name: str
    version: str
    input_digest: str
    output_digest: str
    required_types: tuple[str, ...]
    produced_types: tuple[str, ...]
    annotation_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "input_digest": self.input_digest,
            "output_digest": self.output_digest,
            "required_types": list(self.required_types),
            "produced_types": list(self.produced_types),
            "annotation_ids": list(self.annotation_ids),
        }


@dataclass(frozen=True, slots=True)
class AnnotationPipelineResult:
    """The successor document and the ordered provenance chain that produced it."""

    document: AnnotationDocument
    input_digest: str
    steps: tuple[AnnotationStepReport, ...]

    @property
    def output_digest(self) -> str:
        return self.document.digest

    def to_dict(self) -> dict[str, Any]:
        """Return provenance metadata without copying document text or features."""
        return {
            "schema_version": 1,
            "document_id": self.document.document_id,
            "input_digest": self.input_digest,
            "output_digest": self.output_digest,
            "steps": [step.to_dict() for step in self.steps],
        }


class AnnotationPipeline:
    """Plan and execute append-only annotation layers in deterministic DAG order."""

    def __init__(self, processors: Iterable[AnnotationProcessor]) -> None:
        self._processors = tuple(processors)
        if any(not isinstance(processor, AnnotationProcessor) for processor in self._processors):
            raise AnnotationPipelineError("processors must contain AnnotationProcessor values")
        names = [processor.name for processor in self._processors]
        if len(names) != len(set(names)):
            raise AnnotationPipelineError("processor names must be unique")

    @property
    def processors(self) -> tuple[AnnotationProcessor, ...]:
        return self._processors

    def plan(self, document: AnnotationDocument) -> tuple[str, ...]:
        """Validate every dependency and return stable, topologically ordered names.

        Existing document types are complete seed layers. Every new type has
        exactly one producer and may not overwrite a seed layer.
        """
        if not isinstance(document, AnnotationDocument):
            raise AnnotationPipelineError("document must be an AnnotationDocument")
        schemas = {schema.name: schema for schema in document.annotation_types}
        producers: dict[str, str] = {}
        for processor in self._processors:
            for schema in processor.produces:
                if schema.name in producers:
                    raise AnnotationPipelineError(
                        f"type {schema.name!r} has duplicate producers "
                        f"{producers[schema.name]!r} and {processor.name!r}"
                    )
                if schema.name in schemas:
                    raise AnnotationPipelineError(
                        f"processor {processor.name!r} would overwrite input type {schema.name!r}"
                    )
                schemas[schema.name] = schema
                producers[schema.name] = processor.name

        dependencies: dict[str, set[str]] = {}
        dependents: dict[str, set[str]] = {processor.name: set() for processor in self._processors}
        for processor in self._processors:
            dependencies[processor.name] = set()
            declared = {schema.name for schema in (*processor.requires, *processor.produces)}
            for output_type in processor.produces:
                for feature in output_type.fields.values():
                    if feature.target_type is not None and feature.target_type not in declared:
                        raise AnnotationPipelineError(
                            f"processor {processor.name!r} must require or produce "
                            f"reference target type {feature.target_type!r}"
                        )
            for required in processor.requires:
                actual = schemas.get(required.name)
                if actual is None:
                    raise AnnotationPipelineError(
                        f"processor {processor.name!r} requires missing type {required.name!r}"
                    )
                if actual != required:
                    raise AnnotationPipelineError(
                        f"processor {processor.name!r} requires a different schema for {required.name!r}"
                    )
                producer = producers.get(required.name)
                if producer is not None:
                    dependencies[processor.name].add(producer)
                    dependents[producer].add(processor.name)

        available = [name for name, required in dependencies.items() if not required]
        heapq.heapify(available)
        ordered: list[str] = []
        while available:
            name = heapq.heappop(available)
            ordered.append(name)
            for dependent in sorted(dependents[name]):
                dependencies[dependent].remove(name)
                if not dependencies[dependent]:
                    heapq.heappush(available, dependent)
        if len(ordered) != len(self._processors):
            unresolved = sorted(name for name, required in dependencies.items() if required)
            raise AnnotationPipelineError(f"annotation dependencies contain a cycle: {', '.join(unresolved)}")
        return tuple(ordered)

    def run(self, document: AnnotationDocument) -> AnnotationPipelineResult:
        """Run a validated graph and return a new, fully validated document.

        Callbacks receive immutable snapshots. A callback or validation failure
        returns no partial result and cannot modify the original document. This
        does not roll back external effects performed by an application callback.
        """
        ordered = self.plan(document)
        by_name = {processor.name: processor for processor in self._processors}
        current = document
        reports: list[AnnotationStepReport] = []
        for name in ordered:
            processor = by_name[name]
            output = processor.process(current)
            try:
                iterator = iter(output)
            except TypeError as error:
                raise AnnotationPipelineError(
                    f"processor {name!r} must return an iterable of SpanAnnotation values"
                ) from error
            remaining = MAX_ANNOTATIONS - len(current.annotations)
            produced = tuple(islice(iterator, remaining + 1))
            if len(produced) > remaining:
                raise AnnotationPipelineError(f"processor {name!r} exceeds the document annotation limit")
            if any(not isinstance(annotation, SpanAnnotation) for annotation in produced):
                raise AnnotationPipelineError(f"processor {name!r} must return SpanAnnotation values")
            produced = tuple(
                sorted(produced, key=lambda annotation: (annotation.start, annotation.end, annotation.annotation_id))
            )
            allowed = {schema.name for schema in processor.produces}
            if any(annotation.type_name not in allowed for annotation in produced):
                raise AnnotationPipelineError(f"processor {name!r} emitted an undeclared annotation type")
            successor = AnnotationDocument(
                current.document_id,
                current.text,
                (*current.annotation_types, *processor.produces),
                (*current.annotations, *produced),
            )
            reports.append(
                AnnotationStepReport(
                    processor.name,
                    processor.version,
                    current.digest,
                    successor.digest,
                    tuple(schema.name for schema in processor.requires),
                    tuple(schema.name for schema in processor.produces),
                    tuple(annotation.annotation_id for annotation in produced),
                )
            )
            current = successor
        return AnnotationPipelineResult(current, document.digest, tuple(reports))
