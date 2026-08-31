from __future__ import annotations

import json
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from corpusledger.canonical import CanonicalPolicy, canonical_json, canonicalize
from corpusledger.manifest import Manifest, build_manifest

_SCALARS = st.none() | st.booleans() | st.integers(min_value=-(2**53), max_value=2**53) | st.text()
_JSON = st.recursive(
    _SCALARS,
    lambda children: (
        st.lists(children, max_size=6)
        | st.dictionaries(
            st.text(alphabet=st.characters(codec="ascii"), min_size=1, max_size=12),
            children,
            max_size=6,
        )
    ),
    max_leaves=30,
)


@given(_JSON)
@settings(max_examples=80, deadline=None)
def test_canonicalization_is_idempotent(value: object) -> None:
    policy = CanonicalPolicy()
    normalized = canonicalize(value, policy)
    assert canonicalize(normalized, policy) == normalized
    assert canonical_json(normalized, policy) == canonical_json(value, policy)


@given(
    st.dictionaries(
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=8),
        _SCALARS,
        max_size=20,
    )
)
def test_canonical_mapping_hash_input_is_insertion_order_independent(
    value: dict[str, object],
) -> None:
    reversed_value = dict(reversed(tuple(value.items())))
    assert canonical_json(value) == canonical_json(reversed_value)


@given(
    st.lists(
        st.tuples(
            st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=1, max_size=10),
            st.text(max_size=60),
        ),
        min_size=1,
        max_size=30,
        unique_by=lambda item: item[0],
    )
)
@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=(HealthCheck.function_scoped_fixture,),
)
def test_manifest_roundtrip_property(
    tmp_path: Path,
    values: list[tuple[str, str]],
) -> None:
    source = tmp_path / "property.jsonl"
    source.write_text(
        "".join(json.dumps({"id": identifier, "text": text}) + "\n" for identifier, text in values),
        encoding="utf-8",
    )
    manifest = build_manifest(source)
    target = tmp_path / "property.manifest.json"
    manifest.save(target)
    assert Manifest.load(target) == manifest
