"""Version and process related documents in a temporary local SQLite store."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from corpusledger import (
    AnnotationDocument,
    AnnotationEvent,
    AnnotationField,
    AnnotationPipeline,
    AnnotationProcessor,
    AnnotationStore,
    AnnotationType,
    SpanAnnotation,
)


def mark(document: AnnotationDocument) -> tuple[SpanAnnotation, ...]:
    return (SpanAnnotation("review", "review", 0, len(document.text), {"status": "example"}),)


def main() -> None:
    source = AnnotationEvent(
        "article:1",
        (AnnotationDocument("original", "Hello 🌍!"), AnnotationDocument("translation", "Bonjour 🌍!")),
        {"source": "authored demo"},
    )
    schema = AnnotationType("review", {"status": AnnotationField()})
    pipeline = AnnotationPipeline((AnnotationProcessor("mark", "1", mark, produces=(schema,)),))
    with tempfile.TemporaryDirectory(prefix="corpusledger-event-demo-") as temporary:
        path = Path(temporary) / "events.db"
        with AnnotationStore(path) as store:
            first = store.put(source)
            second = store.process("article:1", {"original": pipeline}, expected_revision=first.revision)
            assert store.get("article:1", 1).event.get_document("original").annotations == ()
            assert second.event.get_document("original").span_text("review") == "Hello 🌍!"
            assert second.event.get_document("translation") == source.get_document("translation")
        with AnnotationStore(path, create=False) as reopened:
            assert reopened.get("article:1").digest == second.digest
            print(
                json.dumps(
                    {
                        "history": [row.to_dict() for row in reopened.history("article:1")],
                        "verified": reopened.verify().to_dict(),
                        "pipeline_provenance": second.to_dict()["provenance"],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )


if __name__ == "__main__":
    main()
