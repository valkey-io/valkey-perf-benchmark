# FTS Test Datasets

## Overview

This directory contains test datasets for FTS (Full-Text Search) performance testing. The framework uses a **hybrid approach**:

- **Small query files**: Committed to git (version-controlled)
- **Large test datasets**: Generated locally (not in git)

## Committed Files (In Git)

These files are **committed to the repository** and available immediately:

### Query Datasets (CSV) - Group 1 Only
- `search_terms.csv` (~2KB) - 194 real search queries for term search tests (1a, 1b, 1f, 1g)
- `proximity_phrases.csv` (~500B) - 31 multi-word proximity phrases (1c, 1d, 1h, 1i)
- `mixed_match.csv` (~1KB) - Mixed queries: 50% hits, 50% misses (1e, 1j)

**Why committed:** 
- Small size (total ~4KB)
- Required for Group 1 multi-field tests
- Version-controlled query evolution

## Generated Files (Not In Git)

These files are **automatically generated** by `scripts/setup_datasets.py`:

### Wikipedia Base Dataset
- `enwiki-latest-pages-articles.xml` (~100GB uncompressed)
- `enwiki-latest-pages-articles.xml.bz2` (~20GB compressed)

**Source:** https://dumps.wikimedia.org/enwiki/latest/

### Synthetic Test Datasets (Generated from Wikipedia)
- `field_explosion_50k.xml` (2.35GB) - 50K documents with 50 fields each


## Manual Generation

You can pre-generate datasets if needed:

```bash
# Generate all datasets
python scripts/setup_datasets.py
```

## Dataset Sizes

| File | Size | Type | Status | Used In |
|------|------|------|--------|---------|
| `search_terms.csv` | ~2KB | Query | ✅ Committed | Group 1a,1b,1f,1g |
| `proximity_phrases.csv` | ~500B | Query | ✅ Committed | Group 1c,1d,1h,1i |
| `mixed_match.csv` | ~1KB | Query | ✅ Committed | Group 1e,1j |
| `field_explosion_50k.xml` | 2.35GB | Test Corpus | 🔧 Generated | Group 1 (all) |
| `enwiki-latest-pages-articles.xml` | ~60GB | Source | 🔧 Generated | Wikipedia base |

## CI/CD Behavior

In GitHub Actions (`.github/workflows/fts_benchmark.yml`):

1. **Check for cached datasets** on self-hosted runner
2. **Skip download** if `field_explosion_50k.xml` exists
3. **Generate** if missing (first run only)
4. **Preserve** datasets between workflow runs

This ensures:
- Fast CI/CD runs (datasets cached on runner)
- Self-healing if datasets deleted
- No large files in git repository

## Storage Recommendations

### Local Development
- Keep generated datasets in `datasets/` directory
- They're gitignored automatically
- Reused across test runs

### Self-Hosted CI Runners
- Preserve `datasets/` directory between workflow runs
- Significantly speeds up subsequent runs
- No re-download needed

### Ephemeral CI Runners (GitHub-hosted)
- Not recommended for FTS tests
- Would re-download 20GB on every run
- Use self-hosted runners with persistent storage
