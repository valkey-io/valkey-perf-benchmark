#!/usr/bin/env python3
"""Simple entry point for running FTS performance tests."""

import argparse
import logging
import sys
from pathlib import Path

from fts_benchmark import run_fts_benchmarks


def main():
    """Main entry point for FTS testing."""
    parser = argparse.ArgumentParser(
        description='Run Valkey FTS Performance Tests',
        epilog='''
Examples:
  # Run against local search-enabled Valkey server
  python run_fts_tests.py --valkey-path /path/to/valkey

  # Run against remote search server  
  python run_fts_tests.py --target-ip 192.168.1.100 --valkey-path /path/to/valkey

  # Run with profiling enabled
  python run_fts_tests.py --valkey-path /path/to/valkey --profiling

  # Run specific test groups only
  python run_fts_tests.py --valkey-path /path/to/valkey --groups 1,2,3
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--target-ip', 
        default='127.0.0.1',
        help='Target Valkey server IP (default: 127.0.0.1)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=6379,
        help='Target Valkey server port (default: 6379)'
    )
    
    parser.add_argument(
        '--valkey-path',
        required=True,
        help='Path to valkey source directory (required)'
    )
    
    parser.add_argument(
        '--valkey-benchmark-path',
        help='Path to valkey-benchmark executable (default: valkey-path/src/valkey-benchmark)'
    )
    
    parser.add_argument(
        '--config',
        default='configs/fts-search-configs.json',
        help='FTS configuration file (default: configs/fts-search-configs.json)'
    )
    
    parser.add_argument(
        '--results-dir',
        type=Path,
        default=Path('results'),
        help='Results output directory (default: results/)'
    )
    
    parser.add_argument(
        '--cores',
        help='CPU cores for taskset (e.g., "0-3" or "0,2,4")'
    )
    
    parser.add_argument(
        '--profiling',
        action='store_true',
        help='Enable performance profiling with flamegraphs'
    )
    
    parser.add_argument(
        '--groups',
        help='Comma-separated list of test groups to run (e.g., "1,2,3" for Groups 1-3 only)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--generate-corpus',
        action='store_true',
        help='Generate pathological test corpus before running tests'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    # Validate paths
    valkey_path = Path(args.valkey_path)
    if not valkey_path.exists():
        logging.error(f"Valkey path does not exist: {valkey_path}")
        sys.exit(1)
    
    # Set default valkey-benchmark path if not provided
    if not args.valkey_benchmark_path:
        valkey_benchmark_path = str(valkey_path / "src" / "valkey-benchmark")
        if not Path(valkey_benchmark_path).exists():
            logging.error(f"valkey-benchmark not found at: {valkey_benchmark_path}")
            logging.error("Please build valkey or provide --valkey-benchmark-path")
            sys.exit(1)
    else:
        valkey_benchmark_path = args.valkey_benchmark_path
    
    # Use pre-generated datasets directly - no runtime generation needed
    logging.info("Using pre-generated datasets from datasets/ directory")
    
    # Validate Wikipedia dataset if needed
    wikipedia_path = Path("datasets/enwiki-latest-pages-articles.xml")
    if not wikipedia_path.exists():
        logging.warning("=" * 80)
        logging.warning("Wikipedia dataset not found at:")
        logging.warning(f"  {wikipedia_path.absolute()}")
        logging.warning("")
        logging.warning("This dataset is required for Groups 1, 2, 5, 6, 7, 11, 12, I1, I2")
        logging.warning("")
        logging.warning("To setup datasets, run:")
        logging.warning("  python3 scripts/setup_datasets.py")
        logging.warning("")
        logging.warning("This will download Wikipedia dump and generate pathological datasets.")
        logging.warning("")
        logging.warning("Alternatively, run tests that don't need Wikipedia:")
        logging.warning("  python3 run_fts_tests.py --groups 3,4,16,17,18 --valkey-path /path/to/valkey")
        logging.warning("=" * 80)
        
        # Only fail if user is trying to run groups that need Wikipedia
        if not args.groups or any(g in [1, 2, 5, 6, 7, 11, 12] for g in [int(x.strip()) for x in args.groups.split(',')]):
            logging.error("\nCannot proceed without Wikipedia dataset for requested test groups")
            sys.exit(1)
    
    # Create results directory
    results_dir = args.results_dir / "fts_tests"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Filter configuration by groups if specified
    config_file = args.config
    if args.groups:
        logging.info(f"Will run only groups: {args.groups}")
        # Parse requested groups
        requested_groups = [int(g.strip()) for g in args.groups.split(',')]
        
        # Load and filter configuration
        import json
        with open(config_file, 'r') as f:
            full_config = json.load(f)
        
        # Filter test groups
        filtered_tests = [
            group for group in full_config[0]["fts_tests"] 
            if group["group"] in requested_groups
        ]
        full_config[0]["fts_tests"] = filtered_tests
        
        # Also filter standalone tests if they have matching test IDs
        standalone_map = {"I1": 12, "I2": 13}  # Map standalone tests to group numbers
        filtered_standalone = [
            test for test in full_config[0]["standalone_ingestion"]
            if standalone_map.get(test["test"], 999) in requested_groups
        ]
        full_config[0]["standalone_ingestion"] = filtered_standalone
        
        # Write filtered config to temp file
        filtered_config_file = "configs/fts-filtered-config.json"
        with open(filtered_config_file, 'w') as f:
            json.dump(full_config, f, indent=2)
        
        config_file = filtered_config_file
        logging.info(f"Filtered to {len(filtered_tests)} test groups + {len(filtered_standalone)} standalone tests")
    
    # Override port in config if specified
    if args.port:
        import json
        with open(config_file, 'r') as f:
            config_data = json.load(f)
        config_data[0]["port"] = args.port
        # Write back to temp file
        temp_config = "configs/fts-port-override.json"
        with open(temp_config, 'w') as f:
            json.dump(config_data, f, indent=2)
        config_file = temp_config
    
    # Run FTS benchmarks
    logging.info("Starting FTS performance testing...")
    logging.info(f"Target server: {args.target_ip}:{args.port}")
    logging.info(f"Valkey path: {valkey_path}")
    logging.info(f"Results directory: {results_dir}")
    logging.info(f"Profiling: {'enabled' if args.profiling else 'disabled'}")
    
    try:
        run_fts_benchmarks(
            target_ip=args.target_ip,
            config_file=config_file,
            results_dir=results_dir,
            valkey_path=str(valkey_path),
            valkey_benchmark_path=valkey_benchmark_path,
            cores=args.cores,
            profiling_enabled=args.profiling
        )
        
        logging.info("FTS performance testing completed successfully")
        logging.info(f"Results available in: {results_dir}")
        
        if args.profiling:
            flamegraph_dir = results_dir / "flamegraphs"  
            if flamegraph_dir.exists():
                logging.info(f"Flamegraphs available in: {flamegraph_dir}")
        
    except Exception as e:
        logging.error(f"FTS testing failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
