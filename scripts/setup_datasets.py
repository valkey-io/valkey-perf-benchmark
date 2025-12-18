#!/usr/bin/env python3
"""Download and generate FTS test datasets."""

import argparse
import logging
import subprocess
import sys
from pathlib import Path
import urllib.request
import xml.etree.ElementTree as ET


def extract_bz2_file(compressed_file: Path, extracted_file: Path) -> None:
    """Extract a bz2 compressed file, keeping the original."""
    logging.info("Extracting (this may take 15-30 minutes)...")
    subprocess.run(["bunzip2", "-k", str(compressed_file)], check=True)
    logging.info(f"Extraction complete: {extracted_file}")
    logging.info(f"Kept compressed file: {compressed_file}")


def download_wikipedia(output_dir: Path) -> Path:
    """Download and extract Wikipedia dataset."""
    logging.info("=" * 80)
    logging.info("Downloading Wikipedia dataset...")
    logging.info("=" * 80)

    compressed_file = output_dir / "enwiki-latest-pages-articles.xml.bz2"
    extracted_file = output_dir / "enwiki-latest-pages-articles.xml"

    if extracted_file.exists():
        logging.info(f"Wikipedia dataset already exists: {extracted_file}")
        return extracted_file

    # If compressed file exists but not extracted, just extract it
    if compressed_file.exists():
        logging.info(f"Found existing compressed file: {compressed_file}")
        extract_bz2_file(compressed_file, extracted_file)
        return extracted_file

    # Download from Wikimedia dumps
    url = (
        "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
    )

    logging.info(f"Downloading from: {url}")
    logging.info("This is a ~20GB download and may take 30-60 minutes...")
    logging.info(f"Target: {compressed_file}")

    try:
        urllib.request.urlretrieve(url, compressed_file)
        logging.info("Download complete!")

        # Extract (keep compressed file with -k flag)
        extract_bz2_file(compressed_file, extracted_file)

        return extracted_file

    except Exception as e:
        logging.error(f"Failed to download/extract Wikipedia: {e}")
        logging.error("")
        logging.error("Manual download instructions:")
        logging.error("1. Visit: https://dumps.wikimedia.org/enwiki/latest/")
        logging.error("2. Download: enwiki-latest-pages-articles.xml.bz2")
        logging.error(f"3. Place in: {output_dir}/")
        logging.error("4. Extract: bunzip2 enwiki-latest-pages-articles.xml.bz2")
        sys.exit(1)


def generate_field_explosion(
    output_dir: Path,
    source_wiki: Path,
    max_fields: int = 50,
    field_size: int = 1000,
    doc_count: int = 50000,
) -> Path:
    """Generate field explosion dataset with configurable parameters."""
    output_file = output_dir / "field_explosion_50k.xml"

    if output_file.exists():
        logging.info(f"Dataset already exists: {output_file}")
        return output_file

    logging.info(
        f"Generating {output_file.name} ({max_fields} fields, {field_size} chars, {doc_count} docs)"
    )

    with open(output_file, "w", encoding="utf-8") as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<corpus>\n')

        context = ET.iterparse(source_wiki, events=("end",))
        generated = 0

        for event, elem in context:
            tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            if tag_name == "page" and generated < doc_count:
                text_elem = None
                for child in elem.iter():
                    child_tag = (
                        child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    )
                    if child_tag == "text" and child.text:
                        text_elem = child
                        break

                if text_elem is not None and text_elem.text:
                    if text_elem.text.startswith("#REDIRECT"):
                        elem.clear()
                        continue

                    text = text_elem.text[:field_size]
                    generated += 1

                    out.write("  <doc>\n")
                    out.write(f"    <id>{generated:06d}</id>\n")
                    for i in range(1, max_fields + 1):
                        out.write(f"    <field{i}>{text}</field{i}>\n")
                    out.write("  </doc>\n")

                    if generated % 10000 == 0:
                        logging.info(f"Generated {generated} documents...")

                    if generated >= doc_count:
                        break

                elem.clear()

        out.write("</corpus>\n")

    logging.info(f"Generated {generated} documents with {max_fields} fields each")
    return output_file


def main():
    """Main entry point for dataset setup."""
    parser = argparse.ArgumentParser(
        description="Generate search test datasets with configurable parameters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datasets"),
        help="Output directory for datasets (default: datasets/)",
    )

    parser.add_argument(
        "--files",
        nargs="+",
        help="Specific dataset files to generate (if not specified, generates all)",
    )

    parser.add_argument(
        "--max-fields",
        type=int,
        default=50,
        help="Maximum number of fields per document (default: 50)",
    )

    parser.add_argument(
        "--field-size",
        type=int,
        default=1000,
        help="Maximum size of each field in characters (default: 1000)",
    )

    parser.add_argument(
        "--doc-count",
        type=int,
        default=50000,
        help="Number of documents to generate (default: 50000)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Determine which files to generate
    files_to_generate = args.files or ["field_explosion_50k.xml"]

    # Check if any file needs Wikipedia
    needs_wikipedia = any(
        "field_explosion" in f
        or "popular_term" in f
        or "term_repetition" in f
        or "prefix_explosion" in f
        for f in files_to_generate
    )

    wikipedia_file = None
    if needs_wikipedia:
        wikipedia_file = download_wikipedia(args.output_dir)

    # Generate requested datasets
    for filename in files_to_generate:
        if "field_explosion" in filename:
            if wikipedia_file and wikipedia_file.exists():
                generate_field_explosion(
                    args.output_dir,
                    wikipedia_file,
                    args.max_fields,
                    args.field_size,
                    args.doc_count,
                )
            else:
                logging.error(
                    f"Cannot generate {filename} - Wikipedia source not available"
                )
        else:
            logging.warning(f"Unknown dataset type: {filename}")

    logging.info("=" * 80)
    logging.info("Dataset setup complete")
    logging.info("=" * 80)


if __name__ == "__main__":
    main()
