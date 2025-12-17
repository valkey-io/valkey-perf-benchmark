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


def generate_field_explosion(output_dir: Path, source_wiki: Path) -> Path:
    """Generate field explosion dataset with 50 fields per document (Valkey Search max)."""
    logging.info("=" * 80)
    logging.info("Generating field_explosion_50k.xml (50 fields)...")
    logging.info("=" * 80)

    output_file = output_dir / "field_explosion_50k.xml"

    if output_file.exists():
        logging.info(f"Field explosion dataset already exists: {output_file}")
        return output_file

    logging.info("Parsing Wikipedia XML...")
    logging.info("Creating 50K documents with 50 TEXT fields each...")
    logging.info("Note: Valkey Search maximum is 50 fields per index")

    # Parse Wikipedia and create multi-field documents
    with open(output_file, "w", encoding="utf-8") as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<corpus>\n')

        # Parse Wikipedia XML incrementally
        context = ET.iterparse(source_wiki, events=("end",))
        doc_count = 0

        for event, elem in context:
            # Namespace-agnostic: remove namespace from tag
            tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            if tag_name == "page" and doc_count < 50000:
                # Extract text content - namespace-agnostic search
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
                    text = text_elem.text[:1012]

                    doc_count += 1
                    out.write("  <doc>\n")
                    out.write(f"    <id>{doc_count:06d}</id>\n")
                    for i in range(1, 51):
                        out.write(f"    <field{i}>{text}</field{i}>\n")
                    out.write("  </doc>\n")

                    if doc_count % 10000 == 0:
                        logging.info(f"Generated {doc_count} documents...")

                    if doc_count >= 50000:
                        break

                elem.clear()

        out.write("</corpus>\n")

    logging.info(f"Generated {doc_count} documents with 50 fields each")
    logging.info(f"Output: {output_file}")
    return output_file


def main():
    """Main entry point for dataset setup."""
    parser = argparse.ArgumentParser(
        description="Generate FTS test dataset for Group 1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datasets"),
        help="Output directory for datasets (default: datasets/)",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Download Wikipedia and generate field_explosion dataset
    wikipedia_file = download_wikipedia(args.output_dir)

    if wikipedia_file.exists():
        logging.info("")
        generate_field_explosion(args.output_dir, wikipedia_file)

    logging.info("")
    logging.info("=" * 80)
    logging.info("Dataset setup complete!")
    logging.info("=" * 80)
    logging.info("")
    logging.info("Generated datasets:")
    for f in args.output_dir.glob("*.xml"):
        size_mb = f.stat().st_size / (1024 * 1024)
        logging.info(f"  {f.name}: {size_mb:.1f} MB")
    for f in args.output_dir.glob("*.csv"):
        size_kb = f.stat().st_size / 1024
        logging.info(f"  {f.name}: {size_kb:.1f} KB")


if __name__ == "__main__":
    main()
