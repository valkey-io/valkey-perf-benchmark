#!/usr/bin/env python3
"""Download and generate FTS test datasets."""

import argparse
import logging
import subprocess
import sys
from pathlib import Path
import urllib.request
import xml.etree.ElementTree as ET


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
    
    # Download from Wikimedia dumps
    url = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
    
    logging.info(f"Downloading from: {url}")
    logging.info("This is a ~20GB download and may take 30-60 minutes...")
    logging.info(f"Target: {compressed_file}")
    
    try:
        urllib.request.urlretrieve(url, compressed_file)
        logging.info("Download complete!")
        
        # Extract
        logging.info("Extracting (this may take 15-30 minutes)...")
        subprocess.run(
            ["bunzip2", str(compressed_file)],
            check=True
        )
        logging.info(f"Extraction complete: {extracted_file}")
        
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
    """Generate field explosion dataset with 64 fields per document."""
    logging.info("=" * 80)
    logging.info("Generating field_explosion_50k.xml (64 fields)...")
    logging.info("=" * 80)
    
    output_file = output_dir / "field_explosion_50k.xml"
    
    if output_file.exists():
        logging.info(f"Field explosion dataset already exists: {output_file}")
        return output_file
    
    logging.info("Parsing Wikipedia XML...")
    logging.info("Creating 50K documents with 64 TEXT fields each...")
    
    # Parse Wikipedia and create multi-field documents
    with open(output_file, 'w', encoding='utf-8') as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<docs>\n')
        
        # Parse Wikipedia XML incrementally
        context = ET.iterparse(source_wiki, events=('end',))
        doc_count = 0
        
        for event, elem in context:
            if elem.tag.endswith('page') and doc_count < 50000:
                # Extract text content
                text_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.10/}text')
                if text_elem is not None and text_elem.text:
                    text = text_elem.text[:1000]  # Limit text size
                    
                    # Create document with 64 fields
                    out.write('  <doc>\n')
                    for i in range(1, 65):
                        # Distribute text across fields
                        field_text = text[i*15:(i+1)*15] if len(text) > i*15 else text[:15]
                        out.write(f'    <field{i}>{field_text}</field{i}>\n')
                    out.write('  </doc>\n')
                    
                    doc_count += 1
                    if doc_count % 10000 == 0:
                        logging.info(f"Generated {doc_count} documents...")
                
                # Clear element to save memory
                elem.clear()
        
        out.write('</docs>\n')
    
    logging.info(f"Generated {doc_count} documents with 64 fields each")
    logging.info(f"Output: {output_file}")
    return output_file


def generate_popular_term(output_dir: Path, source_wiki: Path) -> Path:
    """Generate popular term dataset (postings explosion)."""
    logging.info("=" * 80)
    logging.info("Generating popular_term_100k.xml...")
    logging.info("=" * 80)
    
    output_file = output_dir / "popular_term_100k.xml"
    
    if output_file.exists():
        logging.info(f"Popular term dataset already exists: {output_file}")
        return output_file
    
    logging.info("Injecting 'search' term every 10th word in 100K documents...")
    
    with open(output_file, 'w', encoding='utf-8') as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<docs>\n')
        
        context = ET.iterparse(source_wiki, events=('end',))
        doc_count = 0
        
        for event, elem in context:
            if elem.tag.endswith('page') and doc_count < 100000:
                text_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.10/}text')
                if text_elem is not None and text_elem.text:
                    words = text_elem.text.split()[:500]  # First 500 words
                    
                    # Inject "search" every 10th word
                    modified_words = []
                    for i, word in enumerate(words):
                        if i % 10 == 0:
                            modified_words.append("search")
                        modified_words.append(word)
                    
                    text = ' '.join(modified_words)
                    out.write(f'  <doc><text>{text}</text></doc>\n')
                    
                    doc_count += 1
                    if doc_count % 25000 == 0:
                        logging.info(f"Generated {doc_count} documents...")
                
                elem.clear()
        
        out.write('</docs>\n')
    
    logging.info(f"Generated {doc_count} documents")
    logging.info(f"Output: {output_file}")
    return output_file


def generate_term_repetition(output_dir: Path) -> Path:
    """Generate term repetition dataset (term frequency explosion)."""
    logging.info("=" * 80)
    logging.info("Generating term_repetition_5k.xml...")
    logging.info("=" * 80)
    
    output_file = output_dir / "term_repetition_5k.xml"
    
    if output_file.exists():
        logging.info(f"Term repetition dataset already exists: {output_file}")
        return output_file
    
    logging.info("Creating 5K documents with 'performance' repeated 20K times...")
    
    with open(output_file, 'w', encoding='utf-8') as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<docs>\n')
        
        for doc_id in range(5000):
            # Create text with "performance" repeated 20,000 times
            text = ' '.join(['performance'] * 20000)
            out.write(f'  <doc><text>{text}</text></doc>\n')
            
            if (doc_id + 1) % 1000 == 0:
                logging.info(f"Generated {doc_id + 1} documents...")
        
        out.write('</docs>\n')
    
    logging.info("Generated 5000 documents")
    logging.info(f"Output: {output_file}")
    return output_file


def generate_prefix_explosion(output_dir: Path) -> Path:
    """Generate prefix explosion dataset (RadixTree breadth stress)."""
    logging.info("=" * 80)
    logging.info("Generating prefix_explosion_500k.xml...")
    logging.info("=" * 80)
    
    output_file = output_dir / "prefix_explosion_500k.xml"
    
    if output_file.exists():
        logging.info(f"Prefix explosion dataset already exists: {output_file}")
        return output_file
    
    logging.info("Creating 500K documents with numbered prefixes...")
    
    with open(output_file, 'w', encoding='utf-8') as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n<docs>\n')
        
        for doc_id in range(500000):
            # Create 20 terms with numbered prefixes per document
            terms = [f"term_{i:06d}" for i in range(doc_id * 20, (doc_id + 1) * 20)]
            text = ' '.join(terms)
            out.write(f'  <doc><text>{text}</text></doc>\n')
            
            if (doc_id + 1) % 100000 == 0:
                logging.info(f"Generated {doc_id + 1} documents...")
        
        out.write('</docs>\n')
    
    logging.info("Generated 500K documents with 10M unique terms")
    logging.info(f"Output: {output_file}")
    return output_file


def main():
    """Main entry point for dataset setup."""
    parser = argparse.ArgumentParser(
        description='Download and generate FTS test datasets',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('datasets'),
        help='Output directory for datasets (default: datasets/)'
    )
    
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip Wikipedia download (use existing file)'
    )
    
    parser.add_argument(
        '--only-pathological',
        action='store_true',
        help='Only generate pathological datasets (skip Wikipedia download)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Download Wikipedia
    wikipedia_file = args.output_dir / "enwiki-latest-pages-articles.xml"
    if not args.skip_download and not args.only_pathological:
        wikipedia_file = download_wikipedia(args.output_dir)
    elif not wikipedia_file.exists():
        logging.error(f"Wikipedia dataset not found: {wikipedia_file}")
        logging.error("Run without --skip-download to download it")
        sys.exit(1)
    
    # Generate pathological datasets
    if wikipedia_file.exists():
        logging.info("")
        generate_field_explosion(args.output_dir, wikipedia_file)
        
        logging.info("")
        generate_popular_term(args.output_dir, wikipedia_file)
    
    logging.info("")
    generate_term_repetition(args.output_dir)
    
    logging.info("")
    generate_prefix_explosion(args.output_dir)
    
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
