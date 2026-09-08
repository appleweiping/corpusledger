from datetime import date, datetime
from decimal import Decimal

import pytest

from corpusledger.canonical import CanonicalPolicy, canonical_json
from corpusledger.errors import CanonicalizationError


def test_key_order_and_unicode_are_deterministic() -> None:
    composed = {"é": "café", "a": 1}
    decomposed = {"e\u0301": "cafe\u0301", "a": 1}
    assert canonical_json(composed) == canonical_json(decomposed)
    assert canonical_json(composed).startswith('{"a":1')


def test_list_policy_and_special_python_values() -> None:
    assert canonical_json([2, 1]) == "[2,1]"
    assert canonical_json([2, 1], CanonicalPolicy(list_strategy="sort")) == "[1,2]"


def test_per_field_list_policy_sorts_only_selected_paths() -> None:
    policy = CanonicalPolicy(sort_paths=("labels", "metadata.languages"))
    value = {
        "labels": ["z", "a"],
        "metadata": {"languages": ["fr", "en"], "turns": [2, 1]},
    }
    assert canonical_json(value, policy) == ('{"labels":["a","z"],"metadata":{"languages":["en","fr"],"turns":[2,1]}}')
    assert CanonicalPolicy(sort_paths=("labels",)).to_dict()["sort_paths"] == ["labels"]


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_non_finite_rejected(value: float) -> None:
    with pytest.raises(CanonicalizationError, match="NaN and infinity"):
        canonical_json(value)


def test_ambiguous_values_rejected() -> None:
    for value in (datetime(2026, 1, 1), date(2026, 1, 1), Decimal("1.2"), (1, 2)):
        with pytest.raises(CanonicalizationError, match="unsupported"):
            canonical_json(value)
    with pytest.raises(CanonicalizationError, match="not a string"):
        canonical_json({1: "bad"})
    with pytest.raises(CanonicalizationError, match="unsupported"):
        canonical_json(object())


def test_tag_like_json_objects_remain_ordinary_unambiguous_data() -> None:
    assert canonical_json({"$non_finite": "nan"}) == '{"$non_finite":"nan"}'


def test_surrogates_and_resource_exhausting_integers_are_rejected() -> None:
    assert canonical_json({"text": "\ud83d\ude00"}) == canonical_json({"text": "😀"})
    with pytest.raises(CanonicalizationError, match="Unicode scalar"):
        canonical_json({"text": chr(0xD800)})
    with pytest.raises(CanonicalizationError, match="4300-digit"):
        canonical_json(10**4300)
