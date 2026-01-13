#!/usr/bin/env python3
"""Download and generate FTS test datasets."""

import argparse
import json
import logging
import subprocess
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


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

    return content[:field_size]


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
    if args.config:
        with open(args.config) as f:
            dataset_configs = json.load(f)[0].get("dataset_generation", {})

    files_to_gen = args.files or list(dataset_configs.keys())

    needs_wiki = any("field_explosion" in f or "negation" in f for f in files_to_gen)
    wiki_file = download_wikipedia(args.output_dir) if needs_wiki else None

    for filename in files_to_gen:
        if filename in dataset_configs and wiki_file:
            generate_dataset(
                args.output_dir, wiki_file, dataset_configs[filename], filename
            )

    logging.info("Dataset setup complete")


if __name__ == "__main__":
    main()
