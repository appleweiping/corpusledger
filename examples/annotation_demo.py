"""Run with `python examples/annotation_demo.py`; no network or file writes."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable

from corpusledger import (
    AnnotationDocument,
    AnnotationField,
    AnnotationPipeline,
    AnnotationProcessor,
    AnnotationType,
    SpanAnnotation,
)

TOKEN = AnnotationType("token", {"text": AnnotationField()})
SUMMARY = AnnotationType("summary", {"tokens": AnnotationField("references", target_type="token")})


def tokens(document: AnnotationDocument) -> Iterable[SpanAnnotation]:
    for index, match in enumerate(re.finditer(r"\w+|[^\w\s]", document.text)):
        yield SpanAnnotation(f"t{index}", "token", match.start(), match.end(), {"text": match.group()})


def summary(document: AnnotationDocument) -> Iterable[SpanAnnotation]:
    ids = [item.annotation_id for item in document.index("token").annotations]
    yield SpanAnnotation("summary", "summary", 0, len(document.text), {"tokens": ids})


def main() -> None:
    original = AnnotationDocument("demo", "Café 🌍 works.")
    pipeline = AnnotationPipeline(
        (
            AnnotationProcessor("summary", "1", summary, requires=(TOKEN,), produces=(SUMMARY,)),
            AnnotationProcessor("tokens", "unicode-word-punctuation-v1", tokens, produces=(TOKEN,)),
        )
    )
    result = pipeline.run(original)
    assert pipeline.plan(original) == ("tokens", "summary")
    assert original.annotations == ()
    assert result.document.span_text("t1") == "🌍"
    assert AnnotationDocument.from_dict(result.document.to_dict()) == result.document
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
