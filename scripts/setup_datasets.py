#!/usr/bin/env python3
"""Download and generate FTS test datasets."""

import argparse
import csv
import hashlib
import json
import logging
import random
import subprocess
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np


def _make_rng(filename: str) -> "np.random.Generator":
    """Return a numpy Generator seeded deterministically from `filename`.

    Two invocations against the same filename produce byte-identical output,
    so vector datasets and query vectors are reproducible across machines.
    Different filenames get different content (avoids accidentally emitting
    the same random values for e.g. hybrid data and queries).
    """
    seed = int(hashlib.sha256(filename.encode("utf-8")).hexdigest()[:8], 16)
    return np.random.default_rng(seed)


# Alphabet used for deterministic random-word generation (fuzzy datasets)
ALPHABET = "abcdefghijklmnopqrstuvwxyz"

# Constants for query generation
STOP_WORDS = {
    "a",
    "is",
    "the",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "if",
    "in",
    "into",
    "it",
    "no",
    "not",
    "of",
    "on",
    "or",
    "such",
    "that",
    "their",
    "then",
    "there",
    "these",
    "they",
    "this",
    "to",
    "was",
    "will",
    "with",
}


def _generate_random_word(rng: random.Random, min_length: int, max_length: int) -> str:
    """Generate a deterministic random word using the supplied local RNG.

    A local RNG (not the global one) is used so that fuzzy dataset generation
    remains reproducible regardless of other transforms.
    """
    word_length = rng.randint(min_length, max_length)
    return "".join(rng.choices(ALPHABET, k=word_length))


def _apply_fuzzy_edit(word: str, edit_type: str, rng: random.Random) -> str:
    """Apply a single Levenshtein edit (insert/delete/substitute) to ``word``.

    Uses the supplied local RNG for reproducibility. Substitute retries until a
    different character is produced to avoid no-op edits.
    """
    if len(word) < 2:
        return word

    if edit_type == "insert":
        pos = rng.randint(0, len(word))
        return word[:pos] + rng.choice(ALPHABET) + word[pos:]
    if edit_type == "delete":
        pos = rng.randint(0, len(word) - 1)
        return word[:pos] + word[pos + 1 :]
    if edit_type == "substitute":
        pos = rng.randint(0, len(word) - 1)
        original_char = word[pos]
        new_char = rng.choice(ALPHABET)
        while new_char == original_char and len(ALPHABET) > 1:
            new_char = rng.choice(ALPHABET)
        return word[:pos] + new_char + word[pos + 1 :]

    return word


def download_wikipedia(output_dir: Path) -> Path:
    """Download and extract Wikipedia dataset."""
    compressed = output_dir / "enwiki-latest-pages-articles.xml.bz2"
    extracted = output_dir / "enwiki-latest-pages-articles.xml"

    if extracted.exists():
        return extracted

    if compressed.exists():
        logging.info(f"Extracting {compressed.name}...")
        result = subprocess.run(["bunzip2", "-k", str(compressed)])
        if result.returncode == 0:
            return extracted
        else:
            logging.warning(f"Extraction failed (possibly partial download). Removing {compressed.name} and re-downloading...")
            compressed.unlink()

    url = (
        "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
    )
    logging.info(f"Downloading Wikipedia (~20GB, 30-60 min)...")

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "valkey-perf-benchmark/1.0 (benchmark dataset download)"},
        )
        with urllib.request.urlopen(req) as response, open(compressed, "wb") as out_file:
            while True:
                chunk = response.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                out_file.write(chunk)
        subprocess.run(["bunzip2", "-k", str(compressed)], check=True)
        return extracted
    except Exception as e:
        logging.error(f"Download failed: {e}")
        logging.error("Manual: https://dumps.wikimedia.org/enwiki/latest/")
        sys.exit(1)


def _read_source_terms(source_path: Path) -> list:
    """Read and filter source terms from CSV file.

    Returns list of non-stop-word terms from source file.
    """
    source_terms = []
    with open(source_path, "r", encoding="utf-8") as src:
        reader = csv.reader(src)
        # Skip header if present
        first_line = src.readline()
        src.seek(0)
        if not first_line.lower().startswith("term"):
            next(reader)

        for row in reader:
            if row and row[0].strip():
                term = row[0].strip().lower()
                # Skip stop words
                if term not in STOP_WORDS:
                    source_terms.append(row[0].strip())

    return source_terms


def build_field_configs(config: dict) -> list:
    """Build field configurations from config."""
    if "generate_fields" in config:
        # Compact format for field explosion
        gen = config["generate_fields"]
        count = gen["count"]
        prefix = gen.get("prefix", "field")
        size = gen["size"]
        transforms = gen["transforms"]
        return [
            {"name": f"{prefix}{i}", "size": size, "transforms": transforms}
            for i in range(1, count + 1)
        ]
    elif "fields" in config:
        # Explicit field definitions
        return config["fields"]
    else:
        raise ValueError("Config needs 'generate_fields' or 'fields'")


def apply_transforms(
    wiki_text: str, transforms: list, field_size: int, doc_num: int, total_docs: int
) -> str:
    """Apply transformation pipeline."""
    content = ""

    for t in transforms:
        ttype = t.get("type", "wikipedia")

        if ttype == "wikipedia":
            offset = t.get("offset", 0)
            end = offset + field_size

            if offset >= len(wiki_text):
                content = wiki_text[:field_size]
            elif end > len(wiki_text):
                content = wiki_text[offset:]
                if len(content) < field_size:
                    content += " " + wiki_text[: field_size - len(content)]
            else:
                content = wiki_text[offset:end]

        elif ttype == "inject":
            term = t.get("term", "")
            pct = t.get("percentage", 1.0)
            if doc_num <= int(total_docs * pct):
                content += f" {term}"

        elif ttype == "repeat":
            content += f" {(t.get('term', '') + ' ') * t.get('count', 1)}"

        elif ttype == "prefix_gen":
            base = t.get("base", "word")
            variations = t.get("variations", 10)
            prefixes = [f"{base}{i}" for i in range(variations)]
            content += " " + " ".join(prefixes[:10])

        elif ttype == "proximity_phrase":
            # Generate unique phrases per query partition
            # Each unique phrase is repeated N times
            repeats = t.get("repeats", 1000)
            query_id = (doc_num - 1) // repeats
            term_count = t.get("term_count", 5)
            combinations = t.get("combinations", 1)

            # Generate unique terms for this query partition
            terms = [f"phrase{query_id}_term{i}" for i in range(1, term_count + 1)]

            if combinations == 1:
                # Best case: adjacent terms → 1 position tuple check
                content = " ".join(terms)
            else:
                # Worst case: repeated terms with noise, valid combo at end
                # Pattern from test_fulltext.py doc:5
                parts = []
                for term in terms[:-1]:
                    parts.extend([term, term, term, "x", "x"])
                parts.extend([terms[-1], terms[-1]])
                # Valid combination at end
                parts.extend(terms)
                content = " ".join(parts)

        elif ttype == "fuzzy":
            # Generate fuzzy-match dataset: multiple misspelling variants per base term.
            # Structure: variant_count × docs_per_variant. Variant 0 is the
            # correctly-spelled base word; other variants apply
            # ``target_distance`` Levenshtein edits (insert/delete/substitute).
            #
            # ``min_word_length`` / ``max_word_length`` / ``target_distance``
            # MUST match the corresponding fuzzy query-generator config so that
            # each query resolves to the same base word its dataset variant 0
            # holds. See TestGenerateFuzzyQueries.
            variant_count = t.get("variant_count", 5)
            docs_per_variant = t.get("docs_per_variant", 20)
            min_word_length = t.get("min_word_length", 5)
            max_word_length = t.get("max_word_length", 6)
            target_distance = t.get("target_distance", 1)

            # Determine which term / variant this document belongs to
            docs_per_term = variant_count * docs_per_variant
            term_id = ((doc_num - 1) // docs_per_term) + 1
            within_term = (doc_num - 1) % docs_per_term
            variant_id = within_term // docs_per_variant

            # Deterministic base word seeded by term_id
            term_rng = random.Random(term_id)
            base_word = _generate_random_word(
                term_rng, min_word_length, max_word_length
            )

            if variant_id == 0:
                # First variant is the correctly-spelled base word (matches queries)
                variant = base_word
            else:
                # Deterministic seed: term_id * 1_000_000 + variant_id
                # (assumes variant_id < 1_000_000, which is trivially true for
                # realistic ``variant_count`` values).
                variant_seed = term_id * 1_000_000 + variant_id
                variant_rng = random.Random(variant_seed)
                variant = base_word
                for _ in range(target_distance):
                    edit_type = variant_rng.choice(["insert", "delete", "substitute"])
                    variant = _apply_fuzzy_edit(variant, edit_type, variant_rng)

            content = variant

        elif ttype == "expansion":
            # Generate expansion variants: prefix_a suffix_a, prefix_aa suffix_aa, etc.
            # Tests wildcard expansion with multiple documents per variant
            expansion_count = t.get(
                "expansion_count", 5
            )  # Word variants (a, aa, aaa...)
            docs_per_expansion = t.get("docs_per_expansion", 20)  # Copies per variant
            term_count = t.get("term_count", 100)  # Base terms (term1, term2...)

            # Total docs = expansion_count × docs_per_expansion × term_count
            # Calculate which term, expansion, and copy we're on
            docs_per_term = expansion_count * docs_per_expansion
            term_id = ((doc_num - 1) // docs_per_term) + 1
            within_term = (doc_num - 1) % docs_per_term
            expansion_id = within_term // docs_per_expansion

            # Generate expansion pattern (a, aa, aaa, ...)
            expansion = "a" * (expansion_id + 1)

            # Zero-pad term ID to prevent wildcard collision (term001, not term1)
            padded_term_id = f"term{term_id:03d}"

            # Both patterns: term001_a a_term001 (space-separated in same field)
            content = f"{padded_term_id}_{expansion} {expansion}_{padded_term_id}"

        elif ttype == "numeric_range":
            # Generate random numeric values in range
            min_val = t.get("min", 0)
            max_val = t.get("max", 100)
            content = str(random.uniform(min_val, max_val))

        elif ttype == "tag_list":
            # Generate tag combinations
            tags = t.get("tags", ["tag1", "tag2", "tag3"])
            # Select 1-2 random tags and join with pipe
            num_tags = random.randint(1, min(2, len(tags)))
            selected = random.sample(tags, num_tags)
            content = "|".join(selected)

        elif ttype == "vector":
            # Vector fields live in structured NPY, not CSV. This marker exists
            # so build_field_configs()/generate_csv_dataset() can detect a vector
            # field and route generation to generate_structured_npy().
            content = ""

    return content[:field_size]


def _l2_normalize(vectors: "np.ndarray") -> "np.ndarray":
    """Return `vectors` scaled to unit L2 norm along the last axis (in-place safe).

    Rows with zero norm are left as-is (division would produce NaN). In practice
    this never triggers for `np.random.randn` output, but keeping the branch
    keeps the function safe for any caller that might pass sparser input.
    """
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    return np.divide(vectors, norms, out=np.zeros_like(vectors), where=(norms != 0))


def generate_structured_npy(output_dir: Path, filename: str, config: dict) -> Path:
    """Generate a structured NPY combining vector + text/numeric/tag fields.

    The output is consumable by the dataset-enabled valkey-benchmark
    (DATASET_FORMAT_NPY with a structured dtype). All random draws are
    seeded via ``_make_rng(filename)`` so two invocations against the same
    filename produce byte-identical output — required for cross-machine
    benchmark comparability.

    Each field is realized as:

      - vector       → ('name', 'f4', (dims,))  L2-normalized random Gaussian
      - phrase_label → ('name', 'SN')  ASCII bytes, stores 'phrase{qid}' where
                        qid = doc_num // repeats. A short single-token value is
                        intentional: FT.SEARCH "phrase{i}" matches ~`repeats`
                        docs (~1% selectivity at repeats=1000, docs=100k).
      - proximity_phrase → deprecated alias for phrase_label, kept for
                        backward compatibility with existing configs. In the
                        NPY path it ignores term_count/combinations; use the
                        CSV path if you need those semantics.
      - numeric_range → ('name', 'SN')  ASCII decimal string; NUMERIC index
                        parses at ingest.
      - tag_list     → ('name', 'SN')  ASCII bytes, single tag per doc.
    """
    output = output_dir / filename
    if output.exists():
        logging.info(f"Exists: {filename}")
        return output

    doc_count = config["doc_count"]
    field_configs = build_field_configs(config)
    rng = _make_rng(filename)

    # Build structured dtype from field transforms.
    dtype_list = []
    for field in field_configs:
        field_name = field["name"]
        for t in field.get("transforms", []):
            ttype = t.get("type")
            if ttype == "vector":
                dims = t.get("dimensions", 256)
                dtype_list.append((field_name, "f4", (dims,)))
            elif ttype in ("phrase_label", "proximity_phrase"):
                if ttype == "proximity_phrase":
                    logging.warning(
                        f"field '{field_name}': type 'proximity_phrase' in a "
                        f"vector-bearing dataset is treated as 'phrase_label' "
                        f"(term_count / combinations are ignored). Rename to "
                        f"'phrase_label' to silence this warning."
                    )
                size = field.get("size", 50)
                dtype_list.append((field_name, f"S{size}"))
            elif ttype == "numeric_range":
                size = field.get("size", 15)
                # Sanity: an unsigned decimal with 2 fractional digits needs
                # len(str(int(max_val))) + 3 bytes at minimum. Fail loudly at
                # generation time rather than silently truncating in the file.
                max_val = t.get("max", 1000)
                needed = len(f"{max_val:.2f}")
                if size < needed:
                    raise ValueError(
                        f"numeric_range field '{field_name}' has size={size} "
                        f"but max={max_val} requires at least {needed} bytes"
                    )
                dtype_list.append((field_name, f"S{size}"))
            elif ttype == "tag_list":
                size = field.get("size", 30)
                dtype_list.append((field_name, f"S{size}"))

    if not dtype_list:
        raise ValueError(
            f"No supported fields (vector / phrase_label / proximity_phrase / "
            f"numeric_range / tag_list) declared for {filename}; nothing to "
            f"generate"
        )

    logging.info(f"Generating {filename} ({doc_count} docs, {len(dtype_list)} fields)")
    data = np.zeros(doc_count, dtype=dtype_list)

    # Fill each field. All inner loops are numpy-vectorized so throughput scales
    # linearly with doc_count instead of degrading at 10^5+ rows. All random
    # draws go through the seeded `rng` for byte-identical reproducibility.
    for field in field_configs:
        field_name = field["name"]
        for t in field.get("transforms", []):
            ttype = t.get("type")

            if ttype == "vector":
                dims = t.get("dimensions", 256)
                vectors = rng.standard_normal((doc_count, dims), dtype=np.float32)
                data[field_name] = _l2_normalize(vectors)

            elif ttype in ("phrase_label", "proximity_phrase"):
                # Short single-token value: "phrase{qid}". With repeats=1000 and
                # doc_count=100_000, there are 100 unique phrase ids and each
                # matches ~1000 docs when queried.
                repeats = t.get("repeats", 1000)
                qids = np.arange(doc_count) // repeats
                # Encode the small set of unique labels once and gather.
                unique_qids = np.unique(qids)
                labels = np.array(
                    [f"phrase{q}".encode("ascii") for q in unique_qids],
                    dtype=data[field_name].dtype,
                )
                data[field_name] = labels[qids - unique_qids[0]]

            elif ttype == "numeric_range":
                min_val = t.get("min", 0)
                max_val = t.get("max", 1000)
                prices = rng.uniform(min_val, max_val, doc_count)
                formatted = np.char.mod("%.2f", prices)  # returns U-dtype
                data[field_name] = formatted.astype(data[field_name].dtype)

            elif ttype == "tag_list":
                tags = t.get("tags", ["electronics", "books", "clothing"])
                # Pick a random tag per row via the seeded rng.
                choices = rng.choice(tags, size=doc_count)
                data[field_name] = choices.astype(data[field_name].dtype)

    np.save(output, data)
    logging.info(f"Complete: {filename}")
    return output


def generate_csv_dataset(
    output_dir: Path, config: dict, filename: str, wiki_file: Path = None
) -> Path:
    """Generate CSV dataset with optional Wikipedia support."""
    output = output_dir / filename

    if output.exists():
        logging.info(f"Exists: {filename}")
        return output

    doc_count = config["doc_count"]
    field_configs = build_field_configs(config)

    # If any field has a `vector` transform, output is a structured NPY (not CSV).
    # Route to generate_structured_npy() and rewrite the .csv suffix to .npy.
    has_vector = any(
        any(t.get("type") == "vector" for t in field.get("transforms", []))
        for field in field_configs
    )
    if has_vector:
        npy_filename = str(Path(filename).with_suffix(".npy"))
        return generate_structured_npy(output_dir, npy_filename, config)

    # Check if any field needs Wikipedia
    needs_wiki = any(
        any(
            t.get("type", "wikipedia") == "wikipedia"
            for t in field.get("transforms", [])
        )
        for field in field_configs
    )

    if needs_wiki and not wiki_file:
        logging.error(f"Wikipedia source needed for {filename} but not provided")
        return output

    logging.info(
        f"Generating {filename} ({len(field_configs)} fields, {doc_count} docs)"
    )

    # If Wikipedia needed, prepare iterator
    wiki_texts = []
    if needs_wiki and wiki_file:
        logging.info(f"Loading Wikipedia content for {filename}...")
        context = ET.iterparse(wiki_file, events=("end",))
        for event, elem in context:
            if (elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag) != "page":
                continue

            if len(wiki_texts) >= doc_count:
                elem.clear()
                break

            text_elem = None
            for child in elem.iter():
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag == "text" and child.text:
                    text_elem = child
                    break

            if (
                text_elem is None
                or not text_elem.text
                or text_elem.text.startswith("#REDIRECT")
            ):
                elem.clear()
                continue

            wiki_texts.append(text_elem.text)
            elem.clear()

            if len(wiki_texts) % 10000 == 0:
                logging.info(f"Loaded {len(wiki_texts)} Wikipedia articles")

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Header
        writer.writerow([field["name"] for field in field_configs])

        # Data rows
        for doc_num in range(1, doc_count + 1):
            row = []
            wiki_text = (
                wiki_texts[doc_num - 1]
                if needs_wiki and doc_num <= len(wiki_texts)
                else ""
            )

            for field in field_configs:
                content = apply_transforms(
                    wiki_text,
                    field.get("transforms", []),
                    field["size"],
                    doc_num,
                    doc_count,
                )
                row.append(content)
            writer.writerow(row)

            if doc_num % 10000 == 0:
                logging.info(f"Generated {doc_num} docs")

    logging.info(f"Complete: {filename} ({doc_count} docs)")
    return output


def generate_dataset(
    output_dir: Path, source_wiki: Path, config: dict, filename: str
) -> Path:
    """Generate dataset from config."""
    output = output_dir / filename

    if output.exists():
        logging.info(f"Exists: {filename}")
        return output

    doc_count = config["doc_count"]
    field_configs = build_field_configs(config)

    logging.info(
        f"Generating {filename} ({len(field_configs)} fields, {doc_count} docs)"
    )

    with open(output, "w", encoding="utf-8") as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<corpus>\n')

        context = ET.iterparse(source_wiki, events=("end",))
        generated = 0

        for event, elem in context:
            if (elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag) != "page":
                continue

            if generated >= doc_count:
                break

            text_elem = None
            for child in elem.iter():
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag == "text" and child.text:
                    text_elem = child
                    break

            if (
                text_elem is None
                or not text_elem.text
                or text_elem.text.startswith("#REDIRECT")
            ):
                elem.clear()
                continue

            generated += 1
            out.write(f"  <doc>\n    <id>{generated:06d}</id>\n")

            for field in field_configs:
                content = apply_transforms(
                    text_elem.text,
                    field.get("transforms", [{"type": "wikipedia"}]),
                    field["size"],
                    generated,
                    doc_count,
                )
                out.write(f"    <{field['name']}>{content}</{field['name']}>\n")

            out.write("  </doc>\n")

            if generated % 10000 == 0:
                logging.info(f"Generated {generated} docs")

            elem.clear()

        out.write("</corpus>\n")

    logging.info(f"Complete: {filename} ({generated} docs)")
    return output


def generate_queries(output_dir: Path, config: dict, filename: str) -> Path:
    """Generate query CSV based on type."""
    output = output_dir / filename

    if output.exists():
        logging.info(f"Exists: {filename}")
        return output

    query_type = config.get("type", "proximity_phrase")
    num_queries = config["doc_count"]

    logging.info(f"Generating {filename} ({num_queries} queries, type: {query_type})")

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if query_type == "proximity_phrase":
            # Multi-column format for proximity queries
            term_count = config["term_count"]
            writer.writerow([f"term{i}" for i in range(1, term_count + 1)])

            for query_id in range(num_queries):
                terms = [f"phrase{query_id}_term{i}" for i in range(1, term_count + 1)]
                writer.writerow(terms)

        elif query_type in ("prefix", "suffix"):
            # Generate prefix/suffix queries from source dataset
            source = config.get("source", "search_terms.csv")
            source_path = output_dir / source

            if not source_path.exists():
                logging.error(
                    f"Source file {source} not found for {query_type} generation"
                )
                return output

            source_terms = _read_source_terms(source_path)

            # Extract substring based on type
            DEFAULT_SUBSTRING_LEN = 3
            writer.writerow(["term"])
            for i, term in enumerate(source_terms[:num_queries]):
                substring_len = (
                    DEFAULT_SUBSTRING_LEN
                    if len(term) > DEFAULT_SUBSTRING_LEN
                    else len(term)
                )
                extracted = (
                    term[:substring_len]
                    if query_type == "prefix"
                    else term[-substring_len:]
                )
                writer.writerow([extracted])

        elif query_type == "expansion":
            # Generate queries for expansion datasets
            # Queries: term001, term002, ..., termNNN (zero-padded, wildcards added in command)
            writer.writerow(["term"])
            for term_id in range(1, num_queries + 1):
                writer.writerow([f"term{term_id:03d}"])

        elif query_type == "fuzzy":
            # Generate the correctly-spelled base words for fuzzy datasets.
            # Uses the same term_id → base_word mapping as the fuzzy transform,
            # so each query is guaranteed to have matching variants in the dataset.
            min_length = config.get("min_word_length", 5)
            max_length = config.get("max_word_length", 6)

            writer.writerow(["term"])
            for term_id in range(1, num_queries + 1):
                term_rng = random.Random(term_id)
                base_word = _generate_random_word(term_rng, min_length, max_length)
                writer.writerow([base_word])

        elif query_type == "tag_only":
            # Generate tag-only queries for composed TAG filter tests.
            # Rotates through the provided tag list to produce ``doc_count`` queries.
            tags = config.get(
                "tags", ["electronics", "books", "clothing", "food", "sports"]
            )
            if not tags:
                logging.error(f"tag_only query {filename} needs non-empty 'tags' list")
                return output

            writer.writerow(["category"])
            for i in range(num_queries):
                writer.writerow([tags[i % len(tags)]])

        elif query_type == "vector":
            # Structured NPY: (search_term: SN, query_vector: f4[dims]).
            # Terms are 'phrase{i}' so they match generate_structured_npy() ingest
            # (phrase_label / proximity_phrase transform stores 'phrase{qid}' per
            # doc). Query vectors are L2-normalized random Gaussians drawn from
            # a filename-seeded RNG so two invocations produce byte-identical
            # output. A companion CSV of just the search terms is also written.
            dimensions = config.get("dimensions", 256)
            npy_path = output_dir / Path(filename).with_suffix(".npy")

            if not npy_path.exists():
                logging.info(
                    f"Generating {npy_path.name} "
                    f"(structured: search_term + query_vector)"
                )
                rng = _make_rng(npy_path.name)
                dtype = [
                    ("search_term", "S20"),
                    ("query_vector", "f4", (dimensions,)),
                ]
                data = np.zeros(num_queries, dtype=dtype)
                # search_term: deterministic 'phrase{i}' → vectorized encode.
                data["search_term"] = np.array(
                    [f"phrase{i}".encode("ascii") for i in range(num_queries)],
                    dtype=data["search_term"].dtype,
                )
                # query_vector: random Gaussian, L2-normalized in a single pass.
                vectors = rng.standard_normal(
                    (num_queries, dimensions), dtype=np.float32
                )
                data["query_vector"] = _l2_normalize(vectors)
                np.save(npy_path, data)
                logging.info(f"Complete: {npy_path.name}")

            # Companion CSV with just the terms (validate_queries.py fallback).
            writer.writerow(["search_term"])
            for i in range(num_queries):
                writer.writerow([f"phrase{i}"])

    logging.info(f"Complete: {filename} ({num_queries} queries)")
    return output


def main():
    parser = argparse.ArgumentParser(description="Generate FTS test datasets")
    parser.add_argument("--output-dir", type=Path, default=Path("datasets"))
    parser.add_argument(
        "--config", type=Path, help="Config JSON with dataset_generation section"
    )
    parser.add_argument("--files", nargs="+", help="Specific files to generate")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    dataset_configs = {}
    query_configs = {}
    if args.config:
        with open(args.config) as f:
            config_data = json.load(f)[0]
            dataset_configs = config_data.get("dataset_generation", {})
            query_configs = config_data.get("query_generation", {})

    files_to_gen = args.files or list(dataset_configs.keys())

    # Check if Wikipedia is needed for any file
    needs_wiki = any("field_explosion" in f or "negation" in f for f in files_to_gen)

    # Also check if any CSV file needs Wikipedia (hybrid data with wikipedia transforms)
    if not needs_wiki:
        for filename in files_to_gen:
            if filename in dataset_configs and (filename.endswith(".csv") or filename.endswith(".xml")):
                field_configs = build_field_configs(dataset_configs[filename])
                needs_wiki = any(
                    any(
                        t.get("type", "wikipedia") == "wikipedia"
                        for t in field.get("transforms", [])
                    )
                    for field in field_configs
                )
                if needs_wiki:
                    break

    wiki_file = download_wikipedia(args.output_dir) if needs_wiki else None

    for filename in files_to_gen:
        # A scenario may reference `foo.npy` while the dataset_generation key
        # is `foo.csv` (structured NPY is produced by the CSV path when a
        # vector field is present). Fall back to the .csv key in that case.
        config_key = filename
        if filename.endswith(".npy") and filename not in dataset_configs:
            csv_key = str(Path(filename).with_suffix(".csv"))
            if csv_key in dataset_configs:
                config_key = csv_key

        if config_key in dataset_configs:
            if config_key.endswith(".csv"):
                # CSV format - pass wiki_file if needed. Auto-routes to
                # structured NPY internally when any field is a vector.
                generate_csv_dataset(
                    args.output_dir,
                    dataset_configs[config_key],
                    config_key,
                    wiki_file,
                )
            elif wiki_file:
                # XML format - needs Wikipedia
                generate_dataset(
                    args.output_dir,
                    wiki_file,
                    dataset_configs[config_key],
                    config_key,
                )

    # Generate query CSVs
    for query_filename, query_config in query_configs.items():
        generate_queries(args.output_dir, query_config, query_filename)

    logging.info("Dataset setup complete")


if __name__ == "__main__":
    main()
