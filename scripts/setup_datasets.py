#!/usr/bin/env python3
"""Download and generate FTS test datasets."""

import argparse
import csv
import json
import logging
import subprocess
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

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


def download_wikipedia(output_dir: Path) -> Path:
    """Download and extract Wikipedia dataset."""
    compressed = output_dir / "enwiki-latest-pages-articles.xml.bz2"
    extracted = output_dir / "enwiki-latest-pages-articles.xml"

    if extracted.exists():
        return extracted

    if compressed.exists():
        logging.info(f"Extracting {compressed.name}...")
        subprocess.run(["bunzip2", "-k", str(compressed)], check=True)
        return extracted

    url = (
        "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
    )
    logging.info(f"Downloading Wikipedia (~20GB, 30-60 min)...")

    try:
        urllib.request.urlretrieve(url, compressed)
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
            import random

            min_val = t.get("min", 0)
            max_val = t.get("max", 100)
            content = str(random.uniform(min_val, max_val))

        elif ttype == "tag_list":
            # Generate tag combinations
            import random

            tags = t.get("tags", ["tag1", "tag2", "tag3"])
            # Select 1-2 random tags and join with pipe
            num_tags = random.randint(1, min(2, len(tags)))
            selected = random.sample(tags, num_tags)
            content = "|".join(selected)

    return content[:field_size]


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
            if filename in dataset_configs and filename.endswith(".csv"):
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
        if filename in dataset_configs:
            if filename.endswith(".csv"):
                # CSV format - pass wiki_file if needed
                generate_csv_dataset(
                    args.output_dir, dataset_configs[filename], filename, wiki_file
                )
            elif wiki_file:
                # XML format - needs Wikipedia
                generate_dataset(
                    args.output_dir, wiki_file, dataset_configs[filename], filename
                )

    # Generate query CSVs
    for query_filename, query_config in query_configs.items():
        generate_queries(args.output_dir, query_config, query_filename)

    logging.info("Dataset setup complete")


if __name__ == "__main__":
    main()
