"""Unit tests for the fuzzy transform and its matching query generators.

Focus is on the non-obvious behavior that would break the benchmark:

  - each Levenshtein edit type must transform length correctly and (for
    substitute) must actually change a character
  - the ``len < 2`` early return in ``_apply_fuzzy_edit`` (edge case)
  - the ``fuzzy`` transform must produce a stable base word for variant 0 and a
    variant within ``target_distance`` edits otherwise
  - the whole transform must be deterministic across runs (benchmarks depend
    on reproducibility)
  - the ``fuzzy`` query generator must produce the exact same base words that
    ``fuzzy`` transform emits as variant 0 (otherwise queries wouldn't match
    any dataset row)
  - the ``tag_only`` query generator must rotate through the tag list
"""

import csv
import random
import sys
from pathlib import Path

# Ensure scripts/ is importable
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import setup_datasets  # noqa: E402


def _levenshtein(a: str, b: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            curr.append(
                min(
                    prev[j] + 1,  # deletion
                    curr[j - 1] + 1,  # insertion
                    prev[j - 1] + (ca != cb),  # substitution
                )
            )
        prev = curr
    return prev[-1]


# ---- _apply_fuzzy_edit ------------------------------------------------------


class TestApplyFuzzyEdit:
    def test_insert_increases_length_by_one(self):
        assert (
            len(setup_datasets._apply_fuzzy_edit("hello", "insert", random.Random(1)))
            == 6
        )

    def test_delete_decreases_length_by_one(self):
        assert (
            len(setup_datasets._apply_fuzzy_edit("hello", "delete", random.Random(1)))
            == 4
        )

    def test_substitute_preserves_length_and_changes_a_character(self):
        """Substitute must retry until a different character is produced."""
        result = setup_datasets._apply_fuzzy_edit(
            "hello", "substitute", random.Random(1)
        )
        assert len(result) == 5
        assert result != "hello"

    def test_short_word_returned_unchanged(self):
        """len < 2 must short-circuit and return the input verbatim."""
        assert setup_datasets._apply_fuzzy_edit("a", "delete", random.Random(1)) == "a"


# ---- fuzzy transform -------------------------------------------------------


def _fuzzy_transform(**overrides):
    t = {
        "type": "fuzzy",
        "variant_count": 5,
        "docs_per_variant": 2,
        "term_count": 10,
        "min_word_length": 8,
        "max_word_length": 8,
        "target_distance": 1,
    }
    t.update(overrides)
    return t


class TestFuzzyTransform:
    def test_all_docs_in_variant_zero_share_the_same_base_word(self):
        """Every doc that maps to variant 0 of a term must be identical.

        This is the contract that makes the fuzzy query type meaningful.
        """
        t = _fuzzy_transform()  # docs_per_variant=2 → doc 1 and doc 2 are v0/term1
        doc1 = setup_datasets.apply_transforms("", [t], 100, 1, 100)
        doc2 = setup_datasets.apply_transforms("", [t], 100, 2, 100)
        assert doc1 == doc2

    def test_variant_one_differs_from_base_by_exactly_target_distance(self):
        """target_distance=1 must give exactly 1 Levenshtein edit."""
        t = _fuzzy_transform(target_distance=1)
        base = setup_datasets.apply_transforms("", [t], 100, 1, 100)
        variant = setup_datasets.apply_transforms("", [t], 100, 3, 100)
        assert variant != base
        assert _levenshtein(base, variant) == 1

    def test_variant_distance_bounded_by_target_distance(self):
        """target_distance=3 must produce at most 3 edits (edits may compose)."""
        t = _fuzzy_transform(target_distance=3)
        base = setup_datasets.apply_transforms("", [t], 100, 1, 100)
        variant = setup_datasets.apply_transforms("", [t], 100, 3, 100)
        assert 1 <= _levenshtein(base, variant) <= 3

    def test_transform_is_deterministic_across_invocations(self):
        """Benchmark reproducibility depends on stable output for same inputs."""
        t = _fuzzy_transform()
        for doc_num in [1, 5, 17, 42]:
            a = setup_datasets.apply_transforms("", [t], 100, doc_num, 100)
            b = setup_datasets.apply_transforms("", [t], 100, doc_num, 100)
            assert a == b, f"non-deterministic for doc {doc_num}"


# ---- Query generation ------------------------------------------------------


class TestGenerateFuzzyQueries:
    def test_query_terms_equal_dataset_variant_zero(self, tmp_path: Path):
        """Each query term must equal the base word for that term_id in the dataset.

        If this contract breaks, fuzzy queries will not match anything.
        """
        query_config = {
            "type": "fuzzy",
            "doc_count": 5,
            "min_word_length": 8,
            "max_word_length": 8,
        }
        setup_datasets.generate_queries(tmp_path, query_config, "fuzzy_q.csv")

        with open(tmp_path / "fuzzy_q.csv") as f:
            reader = csv.reader(f)
            assert next(reader) == ["term"]
            query_terms = [row[0] for row in reader]

        assert len(query_terms) == 5

        dataset_transform = _fuzzy_transform(
            variant_count=5, docs_per_variant=1, term_count=5
        )
        # docs_per_variant=1, variant_count=5 → doc 1 = term 1 v0,
        # doc 6 = term 2 v0, doc 11 = term 3 v0, ...
        for i, expected_term in enumerate(query_terms, start=1):
            doc_num_for_v0 = (i - 1) * 5 + 1
            dataset_v0 = setup_datasets.apply_transforms(
                "", [dataset_transform], 100, doc_num_for_v0, 100
            )
            assert dataset_v0 == expected_term


class TestGenerateTagOnlyQueries:
    def test_rotates_through_tag_list_with_category_header(self, tmp_path: Path):
        query_config = {
            "type": "tag_only",
            "doc_count": 7,
            "tags": ["electronics", "books", "clothing"],
        }
        setup_datasets.generate_queries(tmp_path, query_config, "tag_q.csv")

        with open(tmp_path / "tag_q.csv") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = [row[0] for row in reader]

        assert header == ["category"]
        assert rows == [
            "electronics",
            "books",
            "clothing",
            "electronics",
            "books",
            "clothing",
            "electronics",
        ]
