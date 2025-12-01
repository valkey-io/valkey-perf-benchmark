# FTS Test Datasets

This directory contains datasets for Full Text Search performance testing.

## 📦 **Dataset Files**

### Small CSV Datasets (Committed to Git)
- `search_terms.csv` - 201 single search terms
- `miss_terms.csv` - 50 non-matching terms
- `proximity_phrases.csv` - 30 three-word phrases
- `extended_proximity.csv` - 30 ten-word phrases
- `wildcard_patterns.csv` - 35 wildcard patterns

### Large XML Datasets (Generated via Script)
- `enwiki-latest-pages-articles.xml` - Wikipedia dump (~105GB)
- `field_explosion_50k.xml` - 50K docs × 64 fields (~3.2GB)
- `popular_term_100k.xml` - Postings explosion stress (~1.7GB)
- `term_repetition_5k.xml` - Term frequency stress (~1.2GB)
- `prefix_explosion_500k.xml` - RadixTree breadth stress (~1.1GB)

## 🚀 **Setup Instructions**

### Automatic Setup (Recommended)
```bash
# Download Wikipedia and generate all pathological datasets
python3 scripts/setup_datasets.py

# Or generate only pathological datasets (if you have Wikipedia already)
python3 scripts/setup_datasets.py --skip-download

# Or generate only pathological (no Wikipedia needed for these)
python3 scripts/setup_datasets.py --only-pathological
```

### Manual Wikipedia Download
If automatic download fails:
```bash
# 1. Download from Wikimedia
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

# 2. Extract (~105GB)
bunzip2 enwiki-latest-pages-articles.xml.bz2

# 3. Move to datasets directory
mv enwiki-latest-pages-articles.xml datasets/

# 4. Generate pathological datasets
python3 scripts/setup_datasets.py --skip-download
```

## 📊 **Dataset Usage**

**Wikipedia-based tests** (Groups 1, 2, 5, 6, 7, 11, 12, I1, I2):
- Require `enwiki-latest-pages-articles.xml`
- Natural language content for baseline testing

**Pathological stress tests** (Groups 3, 4, 16, 17, 18):
- Use generated XML files
- Engineered to stress specific FTS data structures

## 🗑️ **Storage Requirements**

- **Wikipedia download**: ~20GB compressed, ~105GB extracted
- **Generated datasets**: ~7GB total
- **Total**: ~112GB with all datasets

## 🔄 **Regeneration**

To regenerate a specific dataset:
```bash
# Delete the file
rm datasets/field_explosion_50k.xml

# Re-run setup
python3 scripts/setup_datasets.py --skip-download
```

## 📝 **Notes**

- Large XML files are NOT committed to git (see `.gitignore`)
- Small CSV files ARE committed for convenience
- Download Wikipedia once, generate pathological datasets as needed
- Field explosion dataset now has 64 fields (changed from 1000)
