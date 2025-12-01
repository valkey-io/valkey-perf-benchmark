"""FTS-specific benchmark execution with dataset support."""

import logging
import time
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any
import json

import valkey
from valkey_benchmark import ClientRunner
from profiler import PerformanceProfiler
from process_metrics import MetricsProcessor


class FTSBenchmarkRunner(ClientRunner):
    """FTS benchmark runner with dataset and index management support."""
    
    def __init__(self, *args, **kwargs):
        # Extract profiling_enabled before calling parent __init__
        profiling_enabled = kwargs.pop('profiling_enabled', False)
        commit_id = kwargs.get('commit_id', 'HEAD')
        
        super().__init__(*args, **kwargs)
        self.current_index = None
        self.profiler = PerformanceProfiler(self.results_dir, profiling_enabled)
        
        # Initialize metrics processor (same as core tests)
        self.metrics_processor = MetricsProcessor(
            commit_id=commit_id,
            cluster_mode=kwargs.get('cluster_mode', False),
            tls_mode=kwargs.get('tls_mode', False),
            commit_time=self._get_commit_time(commit_id),
            architecture=kwargs.get('architecture', 'x86_64')
        )
        self.all_metrics = []
    
    def _get_commit_time(self, commit_id: str) -> str:
        """Get commit timestamp."""
        try:
            result = subprocess.run(
                ["git", "show", "-s", "--format=%cI", commit_id],
                cwd=self.valkey_path,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except Exception:
            from datetime import datetime
            return datetime.now().isoformat()
        
    def run_fts_benchmark_config(self, fts_config: Dict[str, Any]) -> None:
        """Run FTS benchmark tests according to configuration."""
        logging.info("=== Starting FTS Benchmark Testing ===")
        
        # Run compound ingestion+search tests
        for test_group in fts_config.get("fts_tests", []):
            self._run_compound_test_group(test_group)
            
        # Run standalone ingestion tests
        for standalone_test in fts_config.get("standalone_ingestion", []):
            self._run_standalone_ingestion_test(standalone_test)
            
        # Save results using MetricsProcessor (same as core tests)
        if self.all_metrics:
            self.metrics_processor.write_metrics(self.results_dir, self.all_metrics)
            logging.info(f"Wrote {len(self.all_metrics)} metrics to results/fts_tests/metrics.json")
            
        logging.info("=== FTS Benchmark Testing Complete ===")
    
    def _run_compound_test_group(self, test_group: Dict[str, Any]) -> None:
        """Run ingestion + search compound test group."""
        group_id = test_group["group"]
        description = test_group["description"]
        test_type = test_group.get("test_type", "standard")
        
        logging.info(f"=== Group {group_id}: {description} ===")
        
        # Special handling for backfill testing
        if test_type == "backfill":
            self._run_backfill_test_group(test_group)
            return
        
        # Step 1: Create index with appropriate configuration
        self._create_fts_index(test_group["ingestion"])
        
        # Step 2: Run ingestion phase
        logging.info(f"Phase: Ingestion ({description})")
        self._run_ingestion_phase(test_group["ingestion"], group_id)
        
        # Step 3: Run all search phases  
        for search_test in test_group["searches"]:
            sub_id = search_test["sub"]
            search_desc = search_test["description"]
            logging.info(f"Phase: Search {group_id}{sub_id} ({search_desc})")
            self._run_search_phase(search_test, f"{group_id}{sub_id}")
            
        # Step 4: Cleanup
        self._drop_fts_index()
        self._flush_database()
        
        logging.info(f"Completed Group {group_id}")
    
    def _run_backfill_test_group(self, test_group: Dict[str, Any]) -> None:
        """Run backfill testing: data first, then create index."""
        group_id = test_group["group"]
        description = test_group["description"]
        
        logging.info(f"=== Backfill Test Group {group_id}: {description} ===")
        
        # Step 1: Flush database for clean state
        self._flush_database()
        
        # Step 2: Load data WITHOUT index
        logging.info("Phase: Loading data without index")
        self._run_ingestion_phase(test_group["ingestion"], f"{group_id}_preload")
        
        # Step 3: Create index (triggers backfill)
        logging.info("Phase: Creating index (backfill starts)")
        self._create_fts_index(test_group["ingestion"])
        
        # Step 4: Monitor backfill progress
        self._monitor_backfill_progress()
        
        # Step 5: Run concurrent operations during backfill if specified
        for search_test in test_group["searches"]:
            if search_test.get("concurrent_with_backfill"):
                sub_id = search_test["sub"]
                search_desc = search_test["description"]
                logging.info(f"Phase: {group_id}{sub_id} ({search_desc}) - concurrent with backfill")
                
                # Check if this is a search or ingestion test
                if search_test.get("test_phase") == "ingestion":
                    # Run ingestion during backfill
                    self._run_ingestion_phase(search_test, f"{group_id}{sub_id}")
                else:
                    # Run search during backfill
                    self._run_search_phase(search_test, f"{group_id}{sub_id}")
        
        # Step 6: Wait for backfill completion
        logging.info("Waiting for backfill to complete...")
        self._wait_for_backfill_completion()
        
        # Step 7: Cleanup
        self._drop_fts_index()
        self._flush_database()
        
        logging.info(f"Completed Backfill Test Group {group_id}")
    
    def _monitor_backfill_progress(self) -> None:
        """Monitor FTS backfill progress."""
        try:
            with self._client_context() as client:
                info = client.execute_command("FT.INFO", "rd0")
                # Parse FT.INFO response (list of key-value pairs)
                info_dict = {}
                for i in range(0, len(info), 2):
                    key = info[i].decode() if isinstance(info[i], bytes) else str(info[i])
                    value = info[i+1]
                    if isinstance(value, bytes):
                        value = value.decode()
                    info_dict[key] = value
                
                backfill_pct = info_dict.get("backfill_complete_percent", "N/A")
                state = info_dict.get("state", "unknown")
                logging.info(f"Index state: {state}, Backfill progress: {backfill_pct}")
        except Exception as e:
            logging.warning(f"Could not monitor backfill: {e}")
    
    def _wait_for_backfill_completion(self, timeout: int = 600) -> None:
        """Wait for backfill to complete."""
        import time
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                with self._client_context() as client:
                    info = client.execute_command("FT.INFO", "rd0")
                    info_dict = {}
                    for i in range(0, len(info), 2):
                        key = info[i].decode() if isinstance(info[i], bytes) else str(info[i])
                        value = info[i+1]
                        if isinstance(value, bytes):
                            value = value.decode()
                        info_dict[key] = value
                    
                    backfill_pct = info_dict.get("backfill_complete_percent", "0")
                    state = info_dict.get("state", "unknown")
                    
                    if state == "ready" or float(backfill_pct) >= 1.0:
                        logging.info("Backfill completed successfully")
                        return
                    
                    logging.info(f"Backfill in progress: {backfill_pct} (state: {state})")
                    time.sleep(5)
            except Exception as e:
                logging.warning(f"Error checking backfill status: {e}")
                time.sleep(5)
        
        logging.warning(f"Backfill did not complete within {timeout} seconds")
    
    def _run_standalone_ingestion_test(self, test_config: Dict[str, Any]) -> None:
        """Run standalone ingestion performance test."""
        test_id = test_config["test"]
        description = test_config["description"]
        
        logging.info(f"=== Standalone {test_id}: {description} ===")
        
        # Create basic index for ingestion testing
        self._create_basic_fts_index()
        
        # Run ingestion test with profiling
        self._run_ingestion_phase(test_config, test_id)
        
        # Cleanup
        self._drop_fts_index()
        self._flush_database()
        
        logging.info(f"Completed Standalone {test_id}")
    
    def _create_fts_index(self, ingestion_config: Dict[str, Any]) -> None:
        """Create FTS index with specified configuration."""
        config = ingestion_config.get("config", "Default")
        
        # Build FT.CREATE command based on configuration
        create_cmd = ["FT.CREATE", "rd0", "ON", "HASH", "PREFIX", "1", "rd0-", "SCHEMA"]
        
        # Add fields based on field_range if present
        if "field_range" in ingestion_config:
            # Multi-field scenario - add all fields from range
            field_range = ingestion_config["field_range"]
            start, end = map(int, field_range.split("-"))
            logging.info(f"Creating index with {end - start + 1} TEXT fields")
            for i in range(start, end + 1):
                create_cmd.extend([f"field{i}", "TEXT"])
            create_cmd.extend(["title", "TEXT"])
        else:
            # Single field scenario - use title, body, and url fields
            create_cmd.extend(["title", "TEXT", "body", "TEXT", "url", "TEXT"])
        
        # Add configuration options
        if config == "WITHOFFSETS":
            create_cmd.append("WITHOFFSETS")
        elif config == "WITHSUFFIXTRIE": 
            create_cmd.append("WITHSUFFIXTRIE")
        elif config == "WITHOFFSETS+SUFFIXTRIE":
            create_cmd.extend(["WITHOFFSETS", "WITHSUFFIXTRIE"])
            
        logging.info(f"Creating FTS index with config: {config}")
        # Flush database for clean state instead of trying to drop index
        self._flush_database()
        
        self._execute_fts_command(create_cmd)
        self.current_index = "rd0"
    
    def _create_basic_fts_index(self) -> None:
        """Create basic FTS index for standalone ingestion tests."""
        create_cmd = ["FT.CREATE", "rd0", "ON", "HASH", "PREFIX", "1", "rd0-", "SCHEMA", "title", "TEXT", "body", "TEXT", "url", "TEXT"]
        self._execute_fts_command(create_cmd)
        self.current_index = "rd0"
    
    def _drop_fts_index(self) -> None:
        """Drop the current FTS index."""
        if self.current_index:
            logging.info(f"Dropping FTS index: {self.current_index} (may take time for large indexes)")
            # Use extended timeout for large index drops
            try:
                client = valkey.Valkey(
                    host=self.target_ip,
                    port=self.config.get("port", 6379),
                    decode_responses=True,
                    socket_timeout=120,  # 2 minute timeout for large index drops
                    socket_connect_timeout=10,
                )
                result = client.execute_command("FT.DROPINDEX", self.current_index)
                logging.info(f"Index dropped successfully: {result}")
                client.close()
                self.current_index = None
            except Exception as e:
                logging.error(f"Failed to drop index {self.current_index}: {e}")
                raise
    
    def _execute_fts_command(self, cmd: List[str]) -> Any:
        """Execute FTS command via Valkey client with extended timeout."""
        try:
            # Use extended timeout for FTS commands (index creation can be slow)
            client = valkey.Valkey(
                host=self.target_ip,
                port=self.config.get("port", 6379),
                decode_responses=True,
                socket_timeout=120,  # 2 minute timeout for FTS operations
                socket_connect_timeout=10,
            )
            result = client.execute_command(*cmd)
            client.close()
            return result
        except Exception as e:
            logging.error(f"FTS command failed: {' '.join(cmd)}, Error: {e}")
            raise
    
    def _flush_database(self) -> None:
        """Flush database between test groups."""
        logging.info("Flushing database for clean state (may take 30+ seconds for large datasets)")
        try:
            # Create client with extended timeout for large flushes
            client = valkey.Valkey(
                host=self.target_ip,
                port=self.config.get("port", 6379),
                decode_responses=True,
                socket_timeout=120,  # 2 minute timeout for large flushes
                socket_connect_timeout=10,
            )
            # Execute FLUSHALL synchronously
            result = client.execute_command("FLUSHALL", "SYNC")
            logging.info(f"Database flushed successfully: {result}")
            client.close()
        except Exception as e:
            logging.error(f"Database flush failed: {e}")
            raise
    
    def _run_ingestion_phase(self, ingestion_config: Dict[str, Any], test_id: str) -> None:
        """Run ingestion phase with dataset support."""
        dataset = ingestion_config["dataset"]
        xml_root = ingestion_config.get("xml_root_element", "page")
        maxdocs = ingestion_config.get("maxdocs", 100000)
        command = ingestion_config["command"]
        
        # Generate dynamic command for multi-field scenarios
        if "field_range" in ingestion_config:
            command = self._build_multi_field_command(ingestion_config)
            logging.info(f"Generated dynamic multi-field command with {ingestion_config['field_range']}")
        
        # Start profiling if enabled (Note: causes ~30-50x slowdown)
        self.profiler.start_profiling(f"ingestion_{test_id}")
        
        # Build valkey-benchmark command for ingestion
        bench_cmd = self._build_fts_benchmark_command(
            dataset=dataset,
            xml_root_element=xml_root,
            maxdocs=maxdocs,
            command=command,
            test_phase="ingestion",
            concurrent_clients=ingestion_config.get("concurrent_clients", 1),
            batch_size=ingestion_config.get("batch_size", 1)
        )
        
        # Execute ingestion
        logging.info("Running ingestion benchmark")
        logging.info(f"Command being executed: {' '.join(bench_cmd)}")
        proc = self._run(bench_cmd, cwd=self.valkey_path, capture_output=True, timeout=None)
        
        # Stop profiling and generate analysis
        self.profiler.stop_profiling(f"ingestion_{test_id}")
        
        if proc:
            logging.info(f"Ingestion output:\n{proc.stdout}")
            if proc.stderr:
                logging.warning(f"Ingestion stderr:\n{proc.stderr}")
            
            # Parse and store results using MetricsProcessor
            metrics = self.metrics_processor.create_metrics(
                benchmark_csv_data=proc.stdout,
                command=command,
                data_size=100,  # FTS doesn't have data_size, use default
                pipeline=ingestion_config.get("batch_size", 1),
                clients=ingestion_config.get("concurrent_clients", 1),
                requests=maxdocs
            )
            if metrics:
                # Add FTS-specific fields
                metrics["test_id"] = test_id
                metrics["test_phase"] = "ingestion"
                metrics["dataset"] = dataset
                if "field_range" in ingestion_config:
                    metrics["field_count"] = len(ingestion_config["field_range"].split("-"))
                self.all_metrics.append(metrics)
    
    def _run_search_phase(self, search_config: Dict[str, Any], test_id: str) -> None:
        """Run search phase with dataset support."""
        dataset = search_config["dataset"]
        command = search_config["command"]
        
        # Start profiling if enabled (Note: causes ~30-50x slowdown)
        self.profiler.start_profiling(f"search_{test_id}")
        
        # Build valkey-benchmark command for search - no dataset needed
        bench_cmd = self._build_fts_benchmark_command(
            dataset=dataset if dataset else None,
            command=command,
            test_phase="search",
            concurrent_clients=search_config.get("concurrent_clients", 1)
        )
        
        # Execute search
        logging.info("Running search benchmark")
        logging.info(f"Command being executed: {' '.join(bench_cmd)}")
        proc = self._run(bench_cmd, cwd=self.valkey_path, capture_output=True, timeout=None)
        
        # Stop profiling and generate analysis
        self.profiler.stop_profiling(f"search_{test_id}")
        
        if proc:
            logging.info(f"Search output:\n{proc.stdout}")
            if proc.stderr:
                logging.warning(f"Search stderr:\n{proc.stderr}")
            
            # Parse and store results using MetricsProcessor
            metrics = self.metrics_processor.create_metrics(
                benchmark_csv_data=proc.stdout,
                command=command,
                data_size=100,  # FTS doesn't have data_size, use default
                pipeline=1,
                clients=search_config.get("concurrent_clients", 1),
                duration=self.config.get("duration", 60)
            )
            if metrics:
                # Add FTS-specific fields
                metrics["test_id"] = test_id
                metrics["test_phase"] = "search"
                metrics["dataset"] = dataset
                self.all_metrics.append(metrics)
    
    def _build_multi_field_command(self, ingestion_config: Dict[str, Any]) -> str:
        """Build HSET command for multi-field scenarios."""
        field_range = ingestion_config["field_range"]
        start, end = map(int, field_range.split("-"))
        
        # Build HSET command with all fields
        fields = []
        for i in range(start, end + 1):
            fields.append(f'field{i} "__field:field{i}__"')
        
        # Add title field
        fields.append('title "__field:field1__"')
        
        # Construct full command
        command = f'HSET doc:__rand_int__ {" ".join(fields)}'
        return command
    
    def _build_fts_benchmark_command(
        self, 
        dataset: str,
        command: str,
        test_phase: str,
        xml_root_element: str = "page",
        maxdocs: int = 100000,
        requests: int = None,
        concurrent_clients: int = 1,
        batch_size: int = 1
    ) -> List[str]:
        """Build valkey-benchmark command with FTS dataset support."""
        cmd = []
        
        # Add CPU pinning if specified
        if self.cores:
            cmd += ["taskset", "-c", self.cores]
        
        cmd.append(self.valkey_benchmark_path)
        
        # Connection settings
        if self.tls_mode:
            cmd += ["--tls"]
            cmd += ["--cert", "./tests/tls/valkey.crt"]
            cmd += ["--key", "./tests/tls/valkey.key"] 
            cmd += ["--cacert", "./tests/tls/ca.crt"]
        
        cmd += ["-h", self.target_ip]
        cmd += ["-p", str(self.config.get("port", 6379))]
        
        # Dataset configuration - for both ingestion and search phases
        if dataset:
            # Resolve relative paths
            if not Path(dataset).is_absolute():
                # If relative path, make it relative to framework directory
                dataset_path = Path.cwd() / dataset
                cmd += ["--dataset", str(dataset_path)]
            else:
                cmd += ["--dataset", dataset]
            if xml_root_element and test_phase == "ingestion":
                cmd += ["--xml-root-element", xml_root_element]
            if maxdocs and test_phase == "ingestion":
                cmd += ["--maxdocs", str(maxdocs)]
        
        # Test parameters
        if test_phase == "ingestion":
            # For ingestion: use exact insertion count based on maxdocs
            cmd += ["-n", str(maxdocs)]
        elif requests:
            # For search: use specific request count
            cmd += ["-n", str(requests)]
        else:
            # For search: use duration-based testing
            cmd += ["--duration", str(self.config.get("duration", 60))]
        
        cmd += ["-c", str(concurrent_clients)]
        cmd += ["-P", str(batch_size)]
        cmd += ["-r", "1000000"]  # Large key space for unique keys
        cmd += ["--sequential"]
        cmd += ["--csv"]
        
        # Add the custom command
        cmd += ["--", command]
        
        return cmd


def load_fts_config(config_file: str) -> Dict[str, Any]:
    """Load FTS test configuration."""
    with open(config_file, 'r') as f:
        configs = json.load(f)
    return configs[0]  # Return first config


def run_fts_benchmarks(
    target_ip: str,
    config_file: str,
    results_dir: Path,
    valkey_path: str,
    valkey_benchmark_path: str = None,
    cores: str = None,
    commit_id: str = "HEAD",
    profiling_enabled: bool = False
) -> None:
    """Run FTS benchmarks with specified configuration."""
    
    # Load FTS configuration
    fts_config = load_fts_config(config_file)
    
    # Enable profiling from config or parameter
    profiling = profiling_enabled or fts_config.get("profiling", {}).get("enabled", False)
    
    # Create FTS benchmark runner
    runner = FTSBenchmarkRunner(
        commit_id=commit_id,
        config=fts_config,  # Pass FTS config as base config
        cluster_mode=fts_config.get("cluster_mode", False),
        tls_mode=fts_config.get("tls_mode", False),
        target_ip=target_ip,
        results_dir=results_dir,
        valkey_path=valkey_path,
        cores=cores,
        valkey_benchmark_path=valkey_benchmark_path,
        runs=1,
        server_launcher=None,  # FTS tests run against external server
        architecture="x86_64",
        profiling_enabled=profiling
    )
    
    # Wait for server to be ready
    runner.wait_for_server_ready()
    
    # Run FTS benchmark suite
    runner.run_fts_benchmark_config(fts_config)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run FTS benchmarks')
    parser.add_argument('--target-ip', default='127.0.0.1', help='Target Valkey server IP')
    parser.add_argument('--config', default='configs/fts-search-configs.json', help='FTS config file')
    parser.add_argument('--results-dir', type=Path, default=Path('results'), help='Results directory')
    parser.add_argument('--valkey-path', required=True, help='Path to valkey directory')
    parser.add_argument('--valkey-benchmark-path', help='Path to valkey-benchmark executable')
    parser.add_argument('--cores', help='CPU cores for taskset')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    # Create results directory
    results_dir = args.results_dir / "fts_tests" 
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Run FTS benchmarks
    run_fts_benchmarks(
        target_ip=args.target_ip,
        config_file=args.config,
        results_dir=results_dir,
        valkey_path=args.valkey_path,
        valkey_benchmark_path=args.valkey_benchmark_path,
        cores=args.cores
    )
