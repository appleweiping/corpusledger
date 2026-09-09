from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import FrozenInstanceError, replace
from itertools import permutations
from typing import Any

import pytest

from corpusledger.annotation_pipeline import AnnotationPipeline, AnnotationPipelineError, AnnotationProcessor
from corpusledger.annotations import AnnotationDocument, AnnotationField, AnnotationType, SpanAnnotation
from corpusledger.errors import InputError

TOKEN = AnnotationType("Token", {"surface": AnnotationField()})
SENTENCE = AnnotationType("Sentence", {"tokens": AnnotationField("references", target_type="Token")})
TAG = AnnotationType("Tag", {"token": AnnotationField("reference", target_type="Token"), "value": AnnotationField()})
SUMMARY = AnnotationType(
    "Summary",
    {
        "sentence": AnnotationField("reference", target_type="Sentence"),
        "tags": AnnotationField("references", target_type="Tag"),
    },
)


def tokenize(document: AnnotationDocument) -> Iterable[SpanAnnotation]:
    return (
        SpanAnnotation(f"t{index}", "Token", match.start(), match.end(), {"surface": match.group()})
        for index, match in enumerate(re.finditer(r"\w+", document.text))
    )


def test_diamond_dag_is_deterministic_and_validates_reference_layers() -> None:
    document = AnnotationDocument("unicode", "Café 🌍 speaks.")
    observed: list[str] = []

    def sentence(value: AnnotationDocument) -> list[SpanAnnotation]:
        observed.append("sentence")
        tokens = [item.annotation_id for item in value.annotations if item.type_name == "Token"]
        return [SpanAnnotation("s0", "Sentence", 0, len(value.text), {"tokens": tokens})]

    def tag(value: AnnotationDocument) -> list[SpanAnnotation]:
        observed.append("tag")
        return [
            SpanAnnotation(
                f"tag-{item.annotation_id}", "Tag", item.start, item.end, {"token": item.annotation_id, "value": "N"}
            )
            for item in value.annotations
            if item.type_name == "Token"
        ]

    def summarize(value: AnnotationDocument) -> list[SpanAnnotation]:
        observed.append("summary")
        assert {schema.name for schema in value.annotation_types} == {"Token", "Sentence", "Tag"}
        tags = [item.annotation_id for item in value.annotations if item.type_name == "Tag"]
        return [SpanAnnotation("summary", "Summary", 0, len(value.text), {"sentence": "s0", "tags": tags})]

    processors = (
        AnnotationProcessor("a-tokenize", "1", tokenize, produces=(TOKEN,)),
        AnnotationProcessor("b-sentence", "2", sentence, requires=(TOKEN,), produces=(SENTENCE,)),
        AnnotationProcessor("c-tag", "3", tag, requires=(TOKEN,), produces=(TAG,)),
        AnnotationProcessor("d-summary", "4", summarize, requires=(SENTENCE, TAG), produces=(SUMMARY,)),
    )
    reports = []
    for declared in permutations(processors):
        observed.clear()
        pipeline = AnnotationPipeline(declared)
        assert pipeline.plan(document) == tuple(processor.name for processor in processors)
        assert observed == []  # Planning does not invoke processors.
        result = pipeline.run(document)
        assert observed == ["sentence", "tag", "summary"]
        assert result.document.text == document.text
        assert result.document.span_text("t1") == "speaks"
        assert (result.document.get("t1").start, result.document.get("t1").end) == (7, 13)
        assert result.document.get("s0").features["tokens"] == ("t0", "t1")
        assert result.document.get("summary").features["tags"] == ("tag-t0", "tag-t1")
        reports.append(result.to_dict())
    assert all(report == reports[0] for report in reports)
    assert document.annotation_types == document.annotations == ()


def test_provenance_chain_records_version_and_exact_input_output_digests() -> None:
    document = AnnotationDocument("doc", "one two")
    first = AnnotationProcessor("tokens", "2026.09.09", tokenize, produces=(TOKEN,))
    result = AnnotationPipeline((first,)).run(document)
    step = result.steps[0]
    assert (step.name, step.version) == ("tokens", "2026.09.09")
    assert step.input_digest == result.input_digest == document.digest
    assert step.output_digest == result.output_digest == result.document.digest
    assert step.annotation_ids == ("t0", "t1")
    assert result.to_dict()["steps"][0]["produced_types"] == ["Token"]
    assert "text" not in result.to_dict()
    version_changed = AnnotationPipeline((replace(first, version="2"),)).run(document)
    assert version_changed.output_digest == result.output_digest
    assert version_changed.to_dict() != result.to_dict()
    changed_input = AnnotationDocument("doc", "one three")
    changed_result = AnnotationPipeline((first,)).run(changed_input)
    assert changed_result.input_digest != result.input_digest
    assert changed_result.output_digest != result.output_digest
    with pytest.raises(FrozenInstanceError):
        first.version = "oops"  # type: ignore[misc]


def test_seed_layers_and_empty_producer_output_are_available_to_consumers() -> None:
    seed = AnnotationDocument("doc", "", (TOKEN,))
    calls: list[str] = []

    def empty(document: AnnotationDocument) -> list[SpanAnnotation]:
        calls.append("empty")
        assert TOKEN in document.annotation_types
        return []

    def final(document: AnnotationDocument) -> list[SpanAnnotation]:
        calls.append("final")
        assert SENTENCE in document.annotation_types
        return []

    output = AnnotationType("Finished")
    pipeline = AnnotationPipeline(
        (
            AnnotationProcessor("z-consumer", "1", final, requires=(SENTENCE,), produces=(output,)),
            AnnotationProcessor("a-empty", "1", empty, requires=(TOKEN,), produces=(SENTENCE,)),
        )
    )
    result = pipeline.run(seed)
    assert calls == ["empty", "final"]
    assert result.document.annotations == ()
    assert {value.name for value in result.document.annotation_types} == {"Token", "Sentence", "Finished"}
    assert result.steps[0].output_digest == result.steps[1].input_digest


@pytest.mark.parametrize("problem", ["missing", "schema", "duplicate", "overwrite", "cycle", "self-cycle", "reference"])
def test_whole_graph_is_checked_before_any_callback(problem: str) -> None:
    calls: list[str] = []

    def callback(document: AnnotationDocument) -> list[SpanAnnotation]:
        calls.append("called")
        return []

    a, b = AnnotationType("A"), AnnotationType("B")
    document = AnnotationDocument("doc", "x")
    valid = AnnotationProcessor("a-first", "1", callback, produces=(AnnotationType("Valid"),))
    if problem == "missing":
        processors = (valid, AnnotationProcessor("b-bad", "1", callback, requires=(a,), produces=(b,)))
        message = "missing type"
    elif problem == "schema":
        actual = AnnotationType("A", {"number": AnnotationField("integer")})
        processors = (
            valid,
            AnnotationProcessor("b-source", "1", callback, produces=(actual,)),
            AnnotationProcessor("c-bad", "1", callback, requires=(a,), produces=(b,)),
        )
        message = "different schema"
    elif problem == "duplicate":
        processors = (
            valid,
            AnnotationProcessor("b-one", "1", callback, produces=(a,)),
            AnnotationProcessor("c-two", "1", callback, produces=(a,)),
        )
        message = "duplicate producers"
    elif problem == "overwrite":
        document = AnnotationDocument("doc", "x", (a,))
        processors = (valid, AnnotationProcessor("b-bad", "1", callback, produces=(a,)))
        message = "overwrite input type"
    elif problem == "cycle":
        processors = (
            valid,
            AnnotationProcessor("b-one", "1", callback, requires=(b,), produces=(a,)),
            AnnotationProcessor("c-two", "1", callback, requires=(a,), produces=(b,)),
        )
        message = "cycle"
    elif problem == "self-cycle":
        processors = (valid, AnnotationProcessor("b-bad", "1", callback, requires=(a,), produces=(a,)))
        message = "cycle"
    else:
        unresolved = AnnotationType("Ref", {"target": AnnotationField("reference", target_type="Missing")})
        processors = (valid, AnnotationProcessor("b-bad", "1", callback, produces=(unresolved,)))
        message = "reference target"
    before = document.to_dict()
    with pytest.raises(AnnotationPipelineError, match=message):
        AnnotationPipeline(processors).run(document)
    assert calls == [] and document.to_dict() == before


def test_typed_output_validation_rejects_wrong_layers_ids_and_features() -> None:
    source = AnnotationDocument("doc", "word")
    invalid_outputs = (
        [SpanAnnotation("x", "Other", 0, 1)],
        ["not-an-annotation"],
        42,
        [SpanAnnotation("x", "Token", 0, 5, {"surface": "word"})],
        [SpanAnnotation("x", "Token", 0, 4, {"surface": 42})],
        [SpanAnnotation("x", "Token", 0, 4, {"surface": "word"})] * 2,
    )
    for output in invalid_outputs:

        def invalid(document: AnnotationDocument, result: Any = output) -> Any:
            return result

        processor = AnnotationProcessor("invalid", "1", invalid, produces=(TOKEN,))
        with pytest.raises((AnnotationPipelineError, InputError)):
            AnnotationPipeline((processor,)).run(source)
        assert source.annotations == source.annotation_types == ()


def test_output_references_are_validated_before_next_processor_runs() -> None:
    source = AnnotationDocument("doc", "word", (TOKEN,), (SpanAnnotation("t0", "Token", 0, 4, {"surface": "word"}),))
    downstream: list[bool] = []
    producer = AnnotationProcessor(
        "a-invalid-ref",
        "1",
        lambda document: [SpanAnnotation("s0", "Sentence", 0, 4, {"tokens": ["absent"]})],
        requires=(TOKEN,),
        produces=(SENTENCE,),
    )
    consumer = AnnotationProcessor(
        "b-final",
        "1",
        lambda document: downstream.append(True) or [],
        requires=(SENTENCE,),
        produces=(AnnotationType("Final"),),
    )
    original = source.digest
    with pytest.raises(InputError, match="dangling reference"):
        AnnotationPipeline((producer, consumer)).run(source)
    assert downstream == [] and source.digest == original


def test_mutual_references_created_in_one_processor_are_valid() -> None:
    link = AnnotationType("Link", {"target": AnnotationField("reference", target_type="Link")})
    processor = AnnotationProcessor(
        "links",
        "1",
        lambda document: [
            SpanAnnotation("a", "Link", 0, 1, {"target": "b"}),
            SpanAnnotation("b", "Link", 1, 2, {"target": "a"}),
        ],
        produces=(link,),
    )
    result = AnnotationPipeline((processor,)).run(AnnotationDocument("doc", "ab"))
    assert result.document.get("a").features["target"] == "b"
    assert result.document.get("b").features["target"] == "a"


def test_failure_preserves_original_and_cannot_roll_back_external_callback_effects() -> None:
    source = AnnotationDocument("doc", "word")
    effects: list[str] = []
    intermediate: list[AnnotationDocument] = []
    original = source.to_dict()

    def fail(document: AnnotationDocument) -> Iterable[SpanAnnotation]:
        effects.append("external effect")
        intermediate.append(document)
        yield SpanAnnotation("s0", "Sentence", 0, 4, {"tokens": ["t0"]})
        raise RuntimeError("processor failed while yielding")

    processors = (
        AnnotationProcessor("tokens", "1", tokenize, produces=(TOKEN,)),
        AnnotationProcessor("sentences", "1", fail, requires=(TOKEN,), produces=(SENTENCE,)),
    )
    with pytest.raises(RuntimeError, match="failed while yielding"):
        AnnotationPipeline(processors).run(source)
    assert source.to_dict() == original
    assert effects == ["external effect"]
    assert intermediate[0].get("t0").features["surface"] == "word"
    with pytest.raises(TypeError):
        intermediate[0].get("t0").features["surface"] = "changed"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        intermediate[0].text = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("existing", [False, True])
def test_output_limit_stops_an_unbounded_annotation_generator(monkeypatch: pytest.MonkeyPatch, existing: bool) -> None:
    yielded: list[int] = []
    kind = AnnotationType("Anchor")

    def forever(document: AnnotationDocument) -> Iterable[SpanAnnotation]:
        index = 0
        while True:
            yielded.append(index)
            yield SpanAnnotation(str(index), "Anchor", 0, 0)
            index += 1

    monkeypatch.setattr("corpusledger.annotation_pipeline.MAX_ANNOTATIONS", 2)
    source = (
        AnnotationDocument("doc", "word", (TOKEN,), (SpanAnnotation("seed", "Token", 0, 4, {"surface": "word"}),))
        if existing
        else AnnotationDocument("doc", "")
    )
    before = source.digest
    with pytest.raises(AnnotationPipelineError, match="annotation limit"):
        AnnotationPipeline((AnnotationProcessor("anchors", "1", forever, produces=(kind,)),)).run(source)
    assert yielded == ([0, 1] if existing else [0, 1, 2])
    assert source.digest == before


def test_empty_pipeline_and_output_iteration_order_preserve_canonical_provenance() -> None:
    source = AnnotationDocument("doc", "one two")
    empty = AnnotationPipeline(()).run(source)
    assert empty.document is source and empty.input_digest == empty.output_digest and empty.steps == ()
    annotations = tuple(tokenize(source))
    first = AnnotationProcessor("tokens", "1", lambda document: annotations, produces=(TOKEN,))
    reverse = replace(first, process=lambda document: reversed(annotations))
    assert AnnotationPipeline((first,)).run(source).to_dict() == AnnotationPipeline((reverse,)).run(source).to_dict()


def test_invalid_processor_contracts_and_duplicate_names_are_rejected() -> None:
    callback = lambda document: []  # noqa: E731 - callable fixture for constructor validation
    for overrides in (
        {"name": ""},
        {"version": "has spaces"},
        {"process": None},
        {"requires": ("Token",)},
        {"requires": (TOKEN, TOKEN)},
        {"produces": ()},
        {"produces": (TOKEN, TOKEN)},
    ):
        fields: dict[str, Any] = {"name": "tokens", "version": "1", "process": callback, "produces": (TOKEN,)}
        fields.update(overrides)
        with pytest.raises(AnnotationPipelineError):
            AnnotationProcessor(**fields)
    processor = AnnotationProcessor("tokens", "1", callback, produces=(TOKEN,))
    with pytest.raises(AnnotationPipelineError, match="names must be unique"):
        AnnotationPipeline((processor, processor))
    with pytest.raises(AnnotationPipelineError, match="AnnotationProcessor"):
        AnnotationPipeline(("not-a-processor",))  # type: ignore[arg-type]
    with pytest.raises(AnnotationPipelineError, match="AnnotationDocument"):
        AnnotationPipeline(()).plan({})  # type: ignore[arg-type]
    assert AnnotationPipeline((processor,)).processors == (processor,)
