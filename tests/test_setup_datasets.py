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
import json
import random
import sys
from pathlib import Path

import h5py
import numpy as np
import pytest

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


# ---- vector NPY: dataset ↔ query token alignment ---------------------------


class TestVectorHybridQueryAlignment:
    """The vector benchmark's ingest side stores ``phrase{qid}`` per doc, and
    the vector query side emits ``phrase{i}`` as its ``search_term``. If either
    side changes format (say, ``phrase_{i}`` or ``phrase-{i}``), Group 15 will
    silently return zero results and the whole KNN + text-prefilter benchmark
    becomes meaningless. This is the same class of fragile contract that
    ``TestGenerateFuzzyQueries`` guards for fuzzy.
    """

    def test_every_query_term_appears_in_the_hybrid_dataset(self, tmp_path: Path):
        dims = 4
        doc_count = 10
        repeats = 2  # → 5 distinct phrase ids: phrase0..phrase4
        num_queries = 5

        dataset_config = {
            "doc_count": doc_count,
            "fields": [
                {
                    "name": "title",
                    "size": 50,
                    "transforms": [
                        {
                            "type": "proximity_phrase",
                            "term_count": 1,
                            "combinations": 1,
                            "repeats": repeats,
                        }
                    ],
                },
                {
                    "name": "embedding",
                    "size": 1,
                    "transforms": [{"type": "vector", "dimensions": dims}],
                },
            ],
        }
        setup_datasets.generate_structured_npy(tmp_path, "hybrid.npy", dataset_config)

        query_config = {"type": "vector", "doc_count": num_queries, "dimensions": dims}
        setup_datasets.generate_queries(tmp_path, query_config, "queries.csv")

        hybrid = np.load(tmp_path / "hybrid.npy", allow_pickle=False)
        queries = np.load(tmp_path / "queries.npy", allow_pickle=False)

        titles = {t.rstrip(b"\x00").decode("ascii") for t in hybrid["title"]}
        for term in queries["search_term"]:
            decoded = term.rstrip(b"\x00").decode("ascii")
            assert decoded in titles, (
                f"query term {decoded!r} does not appear in hybrid dataset titles "
                f"({sorted(titles)}) — KNN benchmark would silently return no hits"
            )


# ---- HDF5 conversion -------------------------------------------------


class TestHdf5Dataset:
    def test_converts_hdf5(self, tmp_path: Path, monkeypatch):
        train = np.arange(24, dtype=np.float64).reshape(6, 4)
        queries = train[:2]
        neighbors = np.array([[0, 1], [1, 0]], dtype=np.int32)
        source = tmp_path / "source.hdf5"
        with h5py.File(source, "w") as dataset:
            dataset["train"] = train
            dataset["test"] = queries
            dataset["neighbors"] = neighbors
        dataset_generation = {
            "base.npy": {
                "source": source.name,
                "hdf5_dataset": "train",
                "field": "embedding",
                "dtype": "f4",
                "chunk_size": 2,
            },
            "queries.npy": {
                "source": source.name,
                "hdf5_dataset": "test",
                "field": "query_vector",
                "dtype": "f4",
                "chunk_size": 2,
            },
            "neighbors.npy": {
                "source": source.name,
                "hdf5_dataset": "neighbors",
                "field": "neighbors",
                "chunk_size": 2,
            },
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(
            json.dumps([{"dataset_generation": dataset_generation}]),
            encoding="utf-8",
        )
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "setup_datasets.py",
                "--output-dir",
                str(tmp_path),
                "--config",
                str(config_path),
                "--files",
                "base.npy",
                "queries.npy",
                "neighbors.npy",
            ],
        )

        setup_datasets.main()

        base = np.load(tmp_path / "base.npy", allow_pickle=False)
        assert base.dtype.names == ("embedding",)
        assert base["embedding"].dtype == np.float32
        assert np.array_equal(base["embedding"], train)

        query_data = np.load(tmp_path / "queries.npy", allow_pickle=False)[
            "query_vector"
        ]
        assert query_data.dtype == np.float32
        assert np.array_equal(query_data, queries)

        neighbor_data = np.load(tmp_path / "neighbors.npy", allow_pickle=False)[
            "neighbors"
        ]
        assert neighbor_data.dtype == neighbors.dtype
        assert np.array_equal(neighbor_data, neighbors)

    def test_rejects_non_npy_output(self, tmp_path: Path):
        config = {
            "source": "source.hdf5",
            "hdf5_dataset": "train",
            "field": "embedding",
        }

        with pytest.raises(ValueError, match="must use the .npy extension"):
            setup_datasets.generate_hdf5_dataset(tmp_path, config, "base.csv")
