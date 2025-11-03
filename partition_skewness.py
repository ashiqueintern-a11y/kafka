#!/usr/bin/env python3
"""
Kafka Partition Skewness Detection and Rebalancing Script with OpenTSDB Integration

Enhanced Version v4.3.0:
- Auto-discovery of Zookeeper from /etc/kafka/conf/server.properties
- Auto-discovery of Kafka bin directory from kafka-env.sh and other methods
- Auto-discovery of disk mount path from kafka-log-dirs.sh
- Replica count-based skewness detection
- Batch-wise partition assignment
- Two-phase leader election with verification
- Comprehensive replica rebalancing with TOLERANCE
- Prevents over-rebalancing and broker draining
- Reorder-only mode for leader balancing

Optimized for Kafka 2.8.2 with PhonePe infrastructure
- Zookeeper-based broker discovery
- OpenTSDB metrics collection (CPU, disk)
- Intelligent health-aware rebalancing
- Cruise Control-inspired algorithm

Version: 4.3.0 (Kafka 2.8.2 Compatible - Auto-Discovery)
"""

import subprocess
import json
import logging
import argparse
import sys
import os
import re
import time
import yaml
import requests
import glob
import pathlib
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

# Configure logging
LOG_DIR = "/var/log/kafka-tools"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = f"{LOG_DIR}/partition_balancer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class BrokerMetrics:
    """Container for broker health metrics."""
    
    def __init__(self, broker_id: int):
        self.broker_id = broker_id
        self.hostname = ""
        self.cpu_usage = 0.0
        self.disk_usage_bytes = 0
        self.disk_total_bytes = 0
        self.disk_usage_percent = 0.0
        self.network_in_bytes_per_sec = 0.0
        self.network_out_bytes_per_sec = 0.0
        self.partition_count = 0
        self.leader_count = 0
        self.replica_count = 0
        
    def get_health_score(self) -> float:
        """Calculate broker health score (0-100, lower is healthier)."""
        cpu_weight = 0.5
        disk_weight = 0.5
        score = (self.cpu_usage * cpu_weight) + (self.disk_usage_percent * disk_weight)
        return score
    
    def can_accept_replica(self, partition_size_bytes: int, disk_threshold: float = 80.0) -> bool:
        """Check if broker can accept a new replica without exceeding disk threshold."""
        if self.disk_total_bytes == 0:
            return True
        projected_usage_bytes = self.disk_usage_bytes + partition_size_bytes
        projected_usage_pct = (projected_usage_bytes / self.disk_total_bytes) * 100
        return projected_usage_pct <= disk_threshold


class PartitionMetrics:
    """Container for partition metrics."""
    
    def __init__(self, topic: str, partition: int):
        self.topic = topic
        self.partition = partition
        self.size_bytes = 0
        self.log_start_offset = 0
        self.log_end_offset = 0
        self.leader = -1
        self.replicas = []
        self.isr = []
        
    def get_message_count(self) -> int:
        """Get approximate message count."""
        return self.log_end_offset - self.log_start_offset


class KafkaPartitionBalancer:
    """
    Main class for detecting and balancing Kafka partition skewness.
    Optimized for Kafka 2.8.2 with OpenTSDB integration.
    Enhanced with replica count skewness detection and batch-wise assignment.
    """
    
    def __init__(self, config_file: str):
        """Initialize the Kafka Partition Balancer."""
        self.config = self._load_config(config_file)
        self.reassignment_json_file = f"/tmp/partition_reassignment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.broker_metrics: Dict[int, BrokerMetrics] = {}
        self.partition_metrics: Dict[Tuple[str, int], PartitionMetrics] = {}
        self.broker_info: Dict[int, Dict] = {}
        
        # --- NEW AUTO-DISCOVERY ---
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 0: AUTO-DISCOVERY")
        logger.info("=" * 80)
        
        # Auto-detect Kafka bin directory
        self.kafka_bin_dir = self._discover_kafka_bin_dir()
        
        # Auto-discover Zookeeper connection string
        self.zookeeper_connect = self._discover_zookeeper_connect()
        
        # Bootstrap server is still needed from config for log-dirs check
        self.bootstrap_server = self.config['kafka']['bootstrap_servers']
        if not self.bootstrap_server:
            logger.error("CRITICAL: 'kafka.bootstrap_servers' must be set in config file!")
            sys.exit(1)
            
        # Auto-discover disk mount path
        self.disk_mount_path = self._discover_disk_mount_path()
        
        logger.info("--- End Auto-Discovery ---")

        
        # Extract *remaining* config values
        self.opentsdb_url = self.config['opentsdb']['url']
        self.cpu_metric = self.config['opentsdb']['metrics']['cpu']
        self.disk_metric = self.config['opentsdb']['metrics']['disk']
        
        self.skew_threshold = self.config['thresholds']['skew_percent']
        self.disk_threshold = self.config['thresholds']['disk_max_percent']
        self.max_disk_usage_diff = self.config['thresholds']['disk_max_diff_percent']
        
        # NEW: Replica skewness threshold
        self.replica_skew_threshold = self.config['thresholds'].get('replica_skew_percent', 10.0)
        
        # NEW: Batch-wise assignment configuration
        self.batch_size = self.config['reassignment'].get('batch_size', 50)
        self.batch_wait_time = self.config['reassignment'].get('batch_wait_seconds', 60)
        
        # NEW: Leader election retry configuration
        self.leader_election_max_retries = self.config['advanced'].get('leader_election_max_retries', 2)
        self.leader_election_wait_time = self.config['advanced'].get('leader_election_wait_seconds', 30)
        
        logger.info("=" * 80)
        logger.info("Kafka Partition Balancer v4.3.0 (Auto-Discovery Mode)")
        logger.info(f"Bootstrap Server (Config): {self.bootstrap_server}")
        logger.info(f"Zookeeper (Discovered): {self.zookeeper_connect}")
        logger.info(f"OpenTSDB URL (Config): {self.opentsdb_url}")
        logger.info(f"Kafka Bin (Discovered): {self.kafka_bin_dir}")
        logger.info(f"Disk Mount (Discovered): {self.disk_mount_path}")
        logger.info(f"Skew Threshold: {self.skew_threshold}%")
        logger.info(f"Replica Skew Threshold: {self.replica_skew_threshold}%")
        logger.info(f"Disk Threshold: {self.disk_threshold}%")
        logger.info(f"Batch Size: {self.batch_size} partitions")
        logger.info(f"Log File: {LOG_FILE}")
        logger.info("=" * 80)
    
    def _load_config(self, config_file: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from: {config_file}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_file}")
            sys.exit(1)
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML: {str(e)}")
            sys.exit(1)

    def _discover_zookeeper_connect(self) -> str:
        """Auto-discover Zookeeper connection string from server.properties."""
        logger.info("Discovering Zookeeper connection string...")
        
        # Standard path for server.properties
        prop_file_path = "/etc/kafka/conf/server.properties"
        
        try:
            with open(prop_file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Find the zookeeper.connect line, ignore commented lines
                    if line.startswith("zookeeper.connect=") and not line.startswith("#"):
                        zk_connect = line.split('=', 1)[1].strip()
                        if zk_connect:
                            logger.info(f"  Found Zookeeper: {zk_connect}")
                            return zk_connect
                        
        except FileNotFoundError:
            logger.error(f"CRITICAL: Kafka properties file not found at: {prop_file_path}")
            logger.error("  Cannot auto-discover Zookeeper. Please check file path.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"CRITICAL: Error reading {prop_file_path}: {str(e)}")
            sys.exit(1)
            
        logger.error(f"CRITICAL: Could not find 'zookeeper.connect' in {prop_file_path}")
        sys.exit(1)

    def _discover_kafka_bin_dir(self) -> str:
        """Auto-detect Kafka bin directory."""
        logger.info("Auto-detecting Kafka bin directory...")
        
        # Method 1: Parse kafka-env.sh (HDP/ODP style)
        try:
            logger.info("  Attempt 1: Parsing kafka-env.sh...")
            # Look for HDP/ODP style env files, e.g., /etc/kafka/3.2.2.0-1/0/kafka-env.sh
            env_files = glob.glob('/etc/kafka/*/0/kafka-env.sh')
            if env_files:
                env_file_path = env_files[0] # Use the first one found
                logger.info(f"    Reading: {env_file_path}")
                with open(env_file_path, 'r') as f:
                    for line in f:
                        # Look for a path like /usr/odp/current/kafka-broker/config
                        match = re.search(r'[:=](/[^:]+/kafka-broker)/config', line)
                        if match:
                            bin_dir = match.group(1) + '/bin'
                            if self._verify_kafka_tools(bin_dir):
                                logger.info(f"    Found via kafka-env.sh: {bin_dir}")
                                return bin_dir
        except Exception as e:
            logger.warning(f"    Failed to parse kafka-env.sh: {str(e)}")

        # Method 2: Try 'which' command
        try:
            logger.info("  Attempt 2: Using 'which' command...")
            result = subprocess.run(['which', 'kafka-topics.sh'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                bin_dir = os.path.dirname(result.stdout.strip())
                if self._verify_kafka_tools(bin_dir):
                    logger.info(f"    Found via 'which': {bin_dir}")
                    return bin_dir
        except Exception as e:
            logger.warning(f"    'which' command failed: {str(e)}")
        
        # Method 3: Check KAFKA_HOME
        logger.info("  Attempt 3: Checking KAFKA_HOME...")
        kafka_home = os.environ.get('KAFKA_HOME')
        if kafka_home:
            bin_dir = os.path.join(kafka_home, 'bin')
            if self._verify_kafka_tools(bin_dir):
                logger.info(f"    Found via KAFKA_HOME: {bin_dir}")
                return bin_dir
        
        # Method 4: Common paths (add HDP/ODP paths first)
        logger.info("  Attempt 4: Checking common paths...")
        for bin_dir in [
            '/usr/odp/current/kafka-broker/bin', # HDP/ODP current
            '/usr/odp/3.2.2.0-1/kafka/bin',      # HDP/ODP specific version
            '/opt/kafka/bin', 
            '/usr/local/kafka/bin',
            '/usr/bin'                           # Standard bin
        ]:
            if self._verify_kafka_tools(bin_dir):
                logger.info(f"    Found at: {bin_dir}")
                return bin_dir
        
        logger.error("CRITICAL: Could not find Kafka bin directory.")
        logger.error("  Please ensure Kafka tools (kafka-topics.sh, etc.) are in your PATH")
        logger.error("  or set KAFKA_HOME environment variable.")
        sys.exit(1)
        
    def _discover_disk_mount_path(self) -> str:
        """Auto-discover the primary disk mount path from kafka-log-dirs.sh."""
        logger.info("Discovering disk mount path via kafka-log-dirs.sh...")
        
        command = [
            f"{self.kafka_bin_dir}/kafka-log-dirs.sh",
            "--describe",
            "--bootstrap-server", self.bootstrap_server
        ]
        
        success, stdout, stderr = self.run_kafka_command(command)
        
        if not success:
            logger.error("Failed to run kafka-log-dirs.sh. Cannot discover mount path.")
            logger.error(f"Error: {stderr}")
            logger.warning("Falling back to default disk mount path: /data")
            return "/data"
            
        json_line = None
        for line in stdout.split('\n'):
            line = line.strip()
            if line.startswith('{"version":1,"brokers":'):
                json_line = line
                break
        
        if not json_line:
            logger.error("Could not parse kafka-log-dirs.sh output. No JSON found.")
            logger.warning("Falling back to default disk mount path: /data")
            return "/data"

        try:
            data = json.loads(json_line)
            if not data.get("brokers"):
                raise ValueError("No brokers in JSON")
            
            # Get first broker
            first_broker = data["brokers"][0]
            if not first_broker.get("logDirs"):
                raise ValueError("No logDirs for first broker")
            
            # Get first log dir
            first_log_dir = first_broker["logDirs"][0]["logDir"]
            
            # Infer mount path
            # e.g., /data/kafka-logs -> /data
            # e.g., /data1/logs -> /data1
            p = pathlib.Path(first_log_dir)
            if len(p.parts) > 1:
                mount_path = "/" + p.parts[1]
                logger.info(f"  Discovered log dir: {first_log_dir}, inferred mount path: {mount_path}")
                return mount_path
            elif len(p.parts) == 1 and p.parts[0] == "/":
                logger.warning(f"  Discovered log dir is root '/'. This is unusual.")
                return "/"
            else:
                raise ValueError(f"Could not parse path: {first_log_dir}")

        except Exception as e:
            logger.error(f"  Error parsing log-dirs JSON: {str(e)}")
            logger.warning("  Falling back to default disk mount path: /data")
            return "/data"
    
    def _verify_kafka_tools(self, bin_dir: str) -> bool:
        """Verify essential Kafka CLI tools exist."""
        required_tools = ['kafka-topics.sh', 'zookeeper-shell.sh', 'kafka-reassign-partitions.sh', 'kafka-log-dirs.sh']
        for tool in required_tools:
            if not os.path.exists(os.path.join(bin_dir, tool)):
                logger.debug(f"Verify failed: {tool} not in {bin_dir}")
                return False
        return True
    
    def run_kafka_command(self, command: List[str]) -> Tuple[bool, str, str]:
        """Execute a Kafka CLI command."""
        try:
            logger.debug(f"Executing: {' '.join(command)}")
            result = subprocess.run(command, capture_output=True, text=True, timeout=300)
            
            # Log raw output for debugging Zookeeper commands
            if 'zookeeper-shell' in ' '.join(command):
                logger.debug(f"Zookeeper raw stdout: {result.stdout[:500]}")
            
            return (result.returncode == 0, result.stdout, result.stderr)
        except subprocess.TimeoutExpired:
            return (False, "", "Command timeout")
        except Exception as e:
            return (False, "", str(e))
    
    def discover_brokers_from_zookeeper(self) -> Dict[int, Dict]:
        """
        Discover all broker information from Zookeeper.
        Handles Kafka 2.8.2 Zookeeper output format with connection messages.
        """
        logger.info("\n[DISCOVERY] Discovering Brokers from Zookeeper...")
        
        try:
            # Get list of broker IDs
            command = [
                f"{self.kafka_bin_dir}/zookeeper-shell.sh",
                self.zookeeper_connect,
                "ls", "/brokers/ids"
            ]
            
            success, stdout, stderr = self.run_kafka_command(command)
            
            if not success:
                logger.error("Failed to list brokers from Zookeeper")
                logger.error(f"Error: {stderr}")
                return {}
            
            # Parse broker IDs, filtering Zookeeper connection messages
            broker_ids = []
            for line in stdout.split('\n'):
                line = line.strip()
                # Skip connection messages
                if not line or 'Connecting' in line or 'Welcome' in line or \
                   'JLine' in line or 'WATCHER' in line or 'WatchedEvent' in line:
                    continue
                
                # Look for list: [1001, 1002, 1003, 1004]
                if line.startswith('[') and line.endswith(']'):
                    try:
                        broker_ids = json.loads(line)
                        logger.info(f"Raw broker IDs: {line}")
                        break
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse: {line}, error: {e}")
            
            if not broker_ids:
                logger.error("No broker IDs found")
                return {}
            
            logger.info(f"Found {len(broker_ids)} broker(s): {broker_ids}")
            
            # Get detailed info for each broker
            broker_info = {}
            for broker_id in broker_ids:
                logger.info(f"Fetching details for broker {broker_id}...")
                
                command = [
                    f"{self.kafka_bin_dir}/zookeeper-shell.sh",
                    self.zookeeper_connect,
                    "get", f"/brokers/ids/{broker_id}"
                ]
                
                success, stdout, stderr = self.run_kafka_command(command)
                
                if success:
                    for line in stdout.split('\n'):
                        line = line.strip()
                        
                        # Skip connection messages
                        if not line or 'Connecting' in line or 'Welcome' in line or \
                           'JLine' in line or 'WATCHER' in line or 'WatchedEvent' in line:
                            continue
                        
                        # Look for JSON: {"listener_security_protocol_map":...
                        if line.startswith('{') and line.endswith('}'):
                            try:
                                info = json.loads(line)
                                broker_info[broker_id] = info
                                logger.info(f"  Broker {broker_id}: {info['host']}:{info['port']}")
                                break
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse broker {broker_id} info, error: {e}")
                else:
                    logger.warning(f"Failed to get info for broker {broker_id}")
            
            if not broker_info:
                logger.error("Failed to retrieve any broker information")
                return {}
            
            logger.info(f"\n Successfully discovered {len(broker_info)} broker(s)")
            
            self.broker_info = broker_info
            return broker_info
            
        except Exception as e:
            logger.error(f"Error discovering brokers: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return {}
    
    def check_cluster_health(self) -> bool:
        """
        Check Kafka cluster health.
        Handles Kafka 2.8.2 broker API versions output format.
        """
        logger.info("\n[PRECHECK 1/4] Checking Cluster Health...")
        
        command = [f"{self.kafka_bin_dir}/kafka-broker-api-versions.sh", "--bootstrap-server", self.bootstrap_server]
        success, stdout, stderr = self.run_kafka_command(command)
        
        if not success:
            logger.error(" Cluster health check FAILED")
            logger.error(f"Error: {stderr}")
            return False
        
        # Parse Kafka 2.8.2 format: hostname:port (id: XXXX rack: null) -> (
        broker_count = 0
        brokers_found = []
        
        for line in stdout.split('\n'):
            if '(id:' in line and 'rack:' in line:
                broker_count += 1
                try:
                    hostname = line.split(':')[0].strip()
                    id_match = re.search(r'\(id:\s*(\d+)', line)
                    if id_match:
                        broker_id = id_match.group(1)
                        brokers_found.append(f"{broker_id}:{hostname}")
                except Exception as e:
                    logger.debug(f"Failed to parse line: {line}, error: {e}")
        
        if broker_count == 0:
            logger.error(" No brokers found")
            logger.debug(f"Output: {stdout[:500]}")
            return False
        
        logger.info(f" Cluster is reachable. Found {broker_count} broker(s)")
        for broker_info in brokers_found:
            logger.info(f"    - Broker {broker_info}")
        
        return True
    
    def check_under_replicated_partitions(self) -> bool:
        """Check for under-replicated partitions."""
        logger.info("\n[PRECHECK 2/4] Checking for Under-Replicated Partitions...")
        
        command = [
            f"{self.kafka_bin_dir}/kafka-topics.sh",
            "--bootstrap-server", self.bootstrap_server,
            "--describe",
            "--under-replicated-partitions"
        ]
        
        success, stdout, stderr = self.run_kafka_command(command)
        
        if not success:
            logger.error(" Failed to check under-replicated partitions")
            logger.error(f"Error: {stderr}")
            return False
        
        if stdout.strip() and "Topic:" in stdout:
            logger.warning(" Under-replicated partitions found")
            return False
        
        logger.info(" No under-replicated partitions found")
        return True
    
    def check_controller_status(self) -> bool:
        """Check if Kafka controller is active."""
        logger.info("\n[PRECHECK 3/4] Checking Kafka Controller Status...")
        
        command = [f"{self.kafka_bin_dir}/kafka-topics.sh", "--bootstrap-server", self.bootstrap_server, "--list"]
        success, stdout, stderr = self.run_kafka_command(command)
        
        if not success:
            logger.error(" Controller check FAILED")
            logger.error(f"Error: {stderr}")
            return False
        
        logger.info(" Kafka controller is active")
        return True
    
    def check_ongoing_reassignments(self) -> bool:
        """
        Check for ongoing partition reassignments.
        Handles Kafka 2.8.2 output: "No partition reassignments found."
        """
        logger.info("\n[PRECHECK 4/4] Checking for Ongoing Reassignments...")
        
        command = [f"{self.kafka_bin_dir}/kafka-reassign-partitions.sh", "--bootstrap-server", self.bootstrap_server, "--list"]
        success, stdout, stderr = self.run_kafka_command(command)
        
        if not success:
            logger.error(f" Command failed. Stderr: {stderr}")
            return False
        
        logger.debug(f"Output: {stdout}")
        
        # Kafka 2.8.2: "No partition reassignments found."
        if "no partition reassignment" in stdout.lower():
            logger.info(" No ongoing reassignments")
            return True
        
        logger.warning("  Ongoing reassignments detected:")
        logger.warning(f"Output: {stdout}")
        return False
    
    def get_cluster_metadata(self) -> Dict:
        """
        Retrieve cluster metadata.
        Handles Kafka 2.8.2 format with TopicId and tab-separated fields.
        """
        logger.info("\n[ANALYSIS] Retrieving Cluster Metadata...")
        
        command = [f"{self.kafka_bin_dir}/kafka-topics.sh", "--bootstrap-server", self.bootstrap_server, "--describe"]
        success, stdout, stderr = self.run_kafka_command(command)
        
        if not success:
            logger.error("Failed to retrieve metadata")
            logger.error(f"Error: {stderr}")
            return {}
        
        metadata = {
            "brokers": set(),
            "topics": {},
            "partition_count_per_broker": defaultdict(int),
            "replica_count_per_broker": defaultdict(int),
            "leader_count_per_broker": defaultdict(int)
        }
        
        current_topic = None
        
        for line in stdout.split('\n'):
            original_line = line
            line = line.strip()
            
            if not line:
                continue
            
            # Kafka 2.8.2 format:
            # Topic: secound	TopicId: ...	PartitionCount: 3	ReplicationFactor: 3
            # 	Topic: secound	Partition: 0	Leader: 1001	Replicas: 1001,1002,1003	Isr: ...
            
            # Topic header (no leading tab, has TopicId)
            if not original_line.startswith('\t') and line.startswith("Topic:") and "TopicId:" in line:
                parts = line.split('\t')
                topic_name = None
                partition_count = 0
                replication_factor = 0
                
                for part in parts:
                    part = part.strip()
                    if part.startswith("Topic:"):
                        topic_name = part.replace("Topic:", "").strip()
                    elif part.startswith("PartitionCount:"):
                        try:
                            partition_count = int(part.replace("PartitionCount:", "").strip())
                        except ValueError:
                            pass
                    elif part.startswith("ReplicationFactor:"):
                        try:
                            replication_factor = int(part.replace("ReplicationFactor:", "").strip())
                        except ValueError:
                            pass
                
                if topic_name:
                    current_topic = topic_name
                    metadata["topics"][current_topic] = {
                        "partitions": [],
                        "partition_count": partition_count,
                        "replication_factor": replication_factor
                    }
                    logger.debug(f"Topic: {current_topic} (Partitions: {partition_count}, RF: {replication_factor})")
            
            # Partition line (has Partition:, Leader:, Replicas:)
            elif "Partition:" in line and "Leader:" in line and "Replicas:" in line:
                parts = line.split('\t')
                partition_info = {}
                topic_from_line = None
                
                for part in parts:
                    part = part.strip()
                    if not part:
                        continue
                    
                    if part.startswith("Topic:"):
                        topic_from_line = part.replace("Topic:", "").strip()
                    elif part.startswith("Partition:"):
                        try:
                            partition_info["partition"] = int(part.replace("Partition:", "").strip())
                        except ValueError:
                            continue
                    elif part.startswith("Leader:"):
                        try:
                            leader = int(part.replace("Leader:", "").strip())
                            partition_info["leader"] = leader
                            metadata["leader_count_per_broker"][leader] += 1
                            metadata["brokers"].add(leader)
                        except ValueError:
                            partition_info["leader"] = -1
                    elif part.startswith("Replicas:"):
                        replica_str = part.replace("Replicas:", "").strip()
                        try:
                            replicas = [int(r.strip()) for r in replica_str.split(',') if r.strip()]
                            partition_info["replicas"] = replicas
                            for replica in replicas:
                                metadata["replica_count_per_broker"][replica] += 1
                                metadata["partition_count_per_broker"][replica] += 1
                                metadata["brokers"].add(replica)
                        except ValueError:
                            partition_info["replicas"] = []
                    elif part.startswith("Isr:"):
                        isr_str = part.replace("Isr:", "").strip()
                        try:
                            partition_info["isr"] = [int(r.strip()) for r in isr_str.split(',') if r.strip()]
                        except ValueError:
                            partition_info["isr"] = []
                
                topic_to_use = topic_from_line if topic_from_line else current_topic
                
                if topic_to_use and partition_info:
                    if topic_to_use not in metadata["topics"]:
                        metadata["topics"][topic_to_use] = {
                            "partitions": [],
                            "partition_count": 0,
                            "replication_factor": 0
                        }
                    
                    metadata["topics"][topic_to_use]["partitions"].append(partition_info)
                    logger.debug(f"Partition {partition_info.get('partition')} of {topic_to_use}: Leader={partition_info.get('leader')}")
        
        metadata["brokers"] = sorted(list(metadata["brokers"]))
        
        logger.info(f"Found {len(metadata['brokers'])} broker(s): {metadata['brokers']}")
        logger.info(f"Found {len(metadata['topics'])} topic(s)")
        
        if metadata["brokers"]:
            logger.info("\nPartition Distribution:")
            for broker_id in metadata["brokers"]:
                logger.info(f"  Broker {broker_id}: {metadata['leader_count_per_broker'][broker_id]} leaders, {metadata['replica_count_per_broker'][broker_id]} replicas")
        else:
            logger.error(" No brokers found in metadata")
            logger.debug(f"Sample output:\n{stdout[:2000]}")
        
        return metadata
    
    def query_opentsdb_metric(self, metric: str, hostname: str, tags: Dict[str, str] = None,
                              start: str = "24h-ago", aggregator: str = "avg") -> Optional[float]:
        """Query OpenTSDB for a specific metric."""
        try:
            query_tags = {"node_host": hostname}
            if tags:
                query_tags.update(tags)
            
            payload = {
                "start": start,
                "queries": [{
                    "metric": metric,
                    "aggregator": aggregator,
                    "tags": query_tags
                }]
            }
            
            logger.debug(f"Querying OpenTSDB: {metric} for {hostname} with tags {query_tags}")
            
            response = requests.post(
                f"{self.opentsdb_url}/api/query",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data and len(data) > 0 and 'dps' in data[0]:
                    dps = data[0]['dps']
                    if dps:
                        values = list(dps.values())
                        if aggregator == "min":
                            return min(values)
                        elif aggregator == "max":
                            return max(values)
                        else:
                            return sum(values) / len(values)
            else:
                logger.warning(f"OpenTSDB query failed: {response.status_code}")
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to query OpenTSDB: {str(e)}")
        except Exception as e:
            logger.warning(f"Error processing OpenTSDB response: {str(e)}")
        
        return None
    
    def collect_broker_metrics(self, broker_list: List[int]) -> Dict[int, BrokerMetrics]:
        """Collect broker health metrics from OpenTSDB."""
        logger.info("\n[METRICS] Collecting Broker Health Metrics from OpenTSDB...")
        
        metrics = {}
        
        for broker_id in broker_list:
            broker_metric = BrokerMetrics(broker_id)
            
            if broker_id not in self.broker_info:
                logger.warning(f"No Zookeeper info for broker {broker_id}, skipping")
                continue
            
            hostname = self.broker_info[broker_id]['host']
            broker_metric.hostname = hostname
            
            logger.info(f"\nCollecting metrics for Broker {broker_id} ({hostname})...")
            
            # CPU usage (24h min idle)
            cpu_idle = self.query_opentsdb_metric(
                metric=self.cpu_metric,
                hostname=hostname,
                tags={"cpu": "cpu-total"},
                start="24h-ago",
                aggregator="min"
            )
            
            if cpu_idle is not None:
                broker_metric.cpu_usage = 100.0 - cpu_idle
            else:
                logger.warning(f"Failed to get CPU for {hostname}, using default")
                broker_metric.cpu_usage = 30.0
            
            # Disk usage
            disk_used_pct = self.query_opentsdb_metric(
                metric=self.disk_metric,
                hostname=hostname,
                tags={"path": self.disk_mount_path}, # Use auto-discovered mount path
                start="24h-ago",
                aggregator="avg"
            )
            
            if disk_used_pct is not None:
                broker_metric.disk_usage_percent = disk_used_pct
                estimated_total_bytes = 1024 * 1024 * 1024 * 1024  # 1TB
                broker_metric.disk_total_bytes = estimated_total_bytes
                broker_metric.disk_usage_bytes = int(estimated_total_bytes * (disk_used_pct / 100.0))
            else:
                logger.warning(f"Failed to get disk for {hostname}, using default")
                broker_metric.disk_usage_percent = 50.0
                broker_metric.disk_total_bytes = 1024 * 1024 * 1024 * 1024
                broker_metric.disk_usage_bytes = broker_metric.disk_total_bytes // 2
            
            metrics[broker_id] = broker_metric
            
            logger.info(f"  CPU: {broker_metric.cpu_usage:.2f}%")
            logger.info(f"  Disk: {broker_metric.disk_usage_percent:.2f}%")
            logger.info(f"  Health Score: {broker_metric.get_health_score():.2f}")
        
        self.broker_metrics = metrics
        return metrics
    
    def collect_partition_metrics(self, metadata: Dict) -> Dict[Tuple[str, int], PartitionMetrics]:
        """Collect partition-level metrics."""
        logger.info("\n[METRICS] Collecting Partition Metrics...")
        
        partition_metrics = {}
        
        for topic_name, topic_data in metadata["topics"].items():
            for partition_info in topic_data["partitions"]:
                partition_id = partition_info["partition"]
                key = (topic_name, partition_id)
                
                pm = PartitionMetrics(topic_name, partition_id)
                pm.leader = partition_info.get("leader", -1)
                pm.replicas = partition_info.get("replicas", [])
                pm.isr = partition_info.get("isr", [])
                pm.size_bytes = 1024 * 1024 * 100  # 100MB estimate
                
                partition_metrics[key] = pm
        
        self.partition_metrics = partition_metrics
        return partition_metrics
    
    def _format_bytes(self, bytes_val: float) -> str:
        """Format bytes to human-readable string."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
    
    def analyze_skewness_with_metrics(self, metadata: Dict) -> Dict:
        """
        ENHANCED: Analyze partition distribution with OpenTSDB metrics.
        Now includes BOTH disk-based AND replica count-based skewness detection.
        """
        logger.info("\n[ANALYSIS] Analyzing Partition Skewness with OpenTSDB Metrics...")
        
        analysis = {
            "has_skewness": False,
            "skewness_types": [],
            "recommendations": [],
            "metrics": {},
            "critical_brokers": [],
            "eligible_brokers": [],
            "leader_skew_detected": False,
            "replica_skew_detected": False
        }
        
        brokers = metadata["brokers"]
        if not brokers:
            logger.error("No brokers found in metadata")
            return analysis
        
        broker_metrics = self.collect_broker_metrics(brokers)
        partition_metrics = self.collect_partition_metrics(metadata)
        
        for broker_id in brokers:
            if broker_id in broker_metrics:
                broker_metrics[broker_id].leader_count = metadata["leader_count_per_broker"][broker_id]
                broker_metrics[broker_id].replica_count = metadata["replica_count_per_broker"][broker_id]
                broker_metrics[broker_id].partition_count = metadata["partition_count_per_broker"][broker_id]
        
        # Analyze Leader Skewness
        logger.info("\n--- Leader Distribution Analysis ---")
        leader_skewness = self._analyze_leader_skewness(broker_metrics, brokers)
        
        if leader_skewness:
            analysis["has_skewness"] = True
            analysis["leader_skew_detected"] = True
            analysis["skewness_types"].append("Leader Distribution")
            analysis["recommendations"].append("Execute preferred leader election (with verification)")
        
        # ENHANCED: Analyze Replica Skewness (Disk + Replica Count Aware)
        logger.info("\n--- Replica Distribution Analysis (Disk + Count Aware) ---")
        replica_skewness, eligible_brokers = self._analyze_replica_skewness_enhanced(
            broker_metrics, partition_metrics, brokers, metadata
        )
        
        # CRITICAL: Always set eligible_brokers (needed for leader reordering even if replicas are balanced)
        analysis["eligible_brokers"] = eligible_brokers
        
        if replica_skewness:
            analysis["has_skewness"] = True
            analysis["replica_skew_detected"] = True
            analysis["skewness_types"].append("Replica Distribution (Disk + Count)")
            analysis["recommendations"].append("Execute batch-wise partition reassignment with throttling")
        
        for broker_id, metrics in broker_metrics.items():
            if metrics.disk_usage_percent > self.disk_threshold:
                analysis["critical_brokers"].append(broker_id)
                logger.warning(f"  Broker {broker_id} exceeds disk threshold: {metrics.disk_usage_percent:.2f}%")
        
        analysis["metrics"] = {
            "brokers": {bid: {
                "hostname": bm.hostname,
                "cpu": bm.cpu_usage,
                "disk_pct": bm.disk_usage_percent,
                "health_score": bm.get_health_score(),
                "leaders": bm.leader_count,
                "replicas": bm.replica_count
            } for bid, bm in broker_metrics.items()}
        }
        
        return analysis
    
    def _analyze_leader_skewness(self, broker_metrics: Dict[int, BrokerMetrics], brokers: List[int]) -> bool:
        """Analyze leader distribution."""
        total_leaders = sum(bm.leader_count for bm in broker_metrics.values())
        expected_leaders = total_leaders / len(brokers) if brokers else 0
        
        skewness_detected = False
        
        for broker_id in brokers:
            if broker_id not in broker_metrics:
                continue
            
            bm = broker_metrics[broker_id]
            deviation = abs(bm.leader_count - expected_leaders) / expected_leaders * 100 if expected_leaders > 0 else 0
            
            logger.info(f"Broker {broker_id}:")
            logger.info(f"  Leaders: {bm.leader_count} (expected: {expected_leaders:.2f}, deviation: {deviation:.2f}%)")
            logger.info(f"  Health Score: {bm.get_health_score():.2f}")
            
            if deviation > self.skew_threshold:
                skewness_detected = True
                logger.warning(f"    LEADER SKEWNESS DETECTED")
        
        if not skewness_detected:
            logger.info(" Leader distribution is balanced")
        
        return skewness_detected
    
    def _analyze_replica_skewness_enhanced(self, broker_metrics: Dict[int, BrokerMetrics], 
                                         partition_metrics: Dict[Tuple[str, int], PartitionMetrics],
                                         brokers: List[int],
                                         metadata: Dict) -> Tuple[bool, List[int]]:
        """
        ENHANCED: Analyze replica distribution considering BOTH disk usage AND replica count.
        """
        skewness_detected = False
        disk_usages = []
        replica_counts = []
        eligible_brokers = []
        
        for broker_id in brokers:
            if broker_id not in broker_metrics:
                continue
            
            bm = broker_metrics[broker_id]
            
            logger.info(f"Broker {broker_id}:")
            logger.info(f"  Disk Usage: {bm.disk_usage_percent:.2f}%")
            logger.info(f"  CPU Usage: {bm.cpu_usage:.2f}%")
            logger.info(f"  Replicas: {bm.replica_count}")
            
            disk_usages.append(bm.disk_usage_percent)
            replica_counts.append(bm.replica_count)
            
            if bm.disk_usage_percent > self.disk_threshold:
                skewness_detected = True
                logger.warning(f"    CRITICAL: Disk exceeds {self.disk_threshold}% threshold")
            else:
                eligible_brokers.append(broker_id)
                logger.info(f"    ELIGIBLE for partition reassignment")
        
        # Check disk skewness
        if disk_usages:
            max_disk = max(disk_usages)
            min_disk = min(disk_usages)
            disk_diff = max_disk - min_disk
            
            logger.info(f"\n Disk Range: {min_disk:.2f}% - {max_disk:.2f}% (diff: {disk_diff:.2f}%)")
            
            if disk_diff > self.max_disk_usage_diff:
                skewness_detected = True
                logger.warning(f"  DISK SKEWNESS: Difference ({disk_diff:.2f}%) exceeds threshold ({self.max_disk_usage_diff}%)")
        
        # NEW: Check replica count skewness
        if replica_counts:
            total_replicas = sum(replica_counts)
            expected_replicas = total_replicas / len(brokers) if brokers else 0
            
            logger.info(f"\n Replica Count Analysis:")
            logger.info(f"Total Replicas: {total_replicas}")
            logger.info(f"Expected per Broker: {expected_replicas:.2f}")
            
            max_replica_count = max(replica_counts)
            min_replica_count = min(replica_counts)
            replica_diff = max_replica_count - min_replica_count
            
            logger.info(f"Range: {min_replica_count} - {max_replica_count} (diff: {replica_diff})")
            
            # Check if any broker deviates beyond threshold
            for broker_id in brokers:
                if broker_id not in broker_metrics:
                    continue
                
                bm = broker_metrics[broker_id]
                deviation = abs(bm.replica_count - expected_replicas) / expected_replicas * 100 if expected_replicas > 0 else 0
                
                if deviation > self.replica_skew_threshold:
                    skewness_detected = True
                    logger.warning(f"  REPLICA COUNT SKEWNESS: Broker {broker_id} has {bm.replica_count} replicas (deviation: {deviation:.2f}%)")
        
        if not skewness_detected:
            logger.info(" Replica distribution is balanced (both disk and count)")
        
        logger.info(f"\n Eligible Brokers for Reassignment: {eligible_brokers}")
        
        return skewness_detected, eligible_brokers
    
    def generate_intelligent_reassignment_plan(self, metadata: Dict, analysis: Dict) -> Optional[Dict]:
        """
        ENHANCED: Generate disk-aware and replica-count-aware reassignment plan.
        Distributes replicas evenly considering both disk usage and replica count.
        Uses TOLERANCE to avoid over-rebalancing and draining brokers.
        """
        logger.info("\n[REBALANCING] Generating Intelligent Reassignment Plan...")
        
        eligible_brokers = analysis.get('eligible_brokers', [])
        
        if not eligible_brokers:
            logger.error(" No eligible brokers available")
            return None
        
        logger.info(f"Using {len(eligible_brokers)} eligible broker(s): {eligible_brokers}")
        
        # Sort by combined health score (disk + CPU) and current replica count
        sorted_brokers = sorted(eligible_brokers, key=lambda b: (
            self.broker_metrics[b].get_health_score() * 0.7 + 
            (self.broker_metrics[b].replica_count / 100.0) * 0.3  # Factor in current load
        ))
        
        logger.info("\nBroker Health & Load Ranking:")
        for broker_id in sorted_brokers:
            bm = self.broker_metrics[broker_id]
            logger.info(f"  Broker {broker_id}: Health {bm.get_health_score():.2f}, "
                        f"Disk {bm.disk_usage_percent:.2f}%, Replicas {bm.replica_count}, "
                        f"Leaders {bm.leader_count}")
        
        reassignment = {"version": 1, "partitions": []}
        broker_assignment_count = defaultdict(int)
        broker_leader_count = defaultdict(int)
        
        # Calculate target replica count per broker
        total_replicas = sum(bm.replica_count for bm in self.broker_metrics.values())
        target_replicas_per_broker = total_replicas / len(eligible_brokers)
        
        # CRITICAL: Set tolerance (15% deviation is acceptable)
        tolerance_percent = 15.0  # Allow 15% deviation from target
        tolerance = target_replicas_per_broker * (tolerance_percent / 100.0)
        min_acceptable = target_replicas_per_broker - tolerance
        max_acceptable = target_replicas_per_broker + tolerance
        
        logger.info(f"\n Rebalancing Targets:")
        logger.info(f"Total Replicas: {total_replicas}")
        logger.info(f"Target per Broker: {target_replicas_per_broker:.1f}")
        logger.info(f"Acceptable Range: {min_acceptable:.1f} - {max_acceptable:.1f} ({tolerance_percent}% tolerance)")
        
        # Track current assignments (start with actual current state)
        for broker_id in eligible_brokers:
            broker_assignment_count[broker_id] = self.broker_metrics[broker_id].replica_count
            broker_leader_count[broker_id] = self.broker_metrics[broker_id].leader_count
        
        # Identify brokers that need action
        overloaded_brokers = {b: count for b, count in broker_assignment_count.items() if count > max_acceptable}
        underloaded_brokers = {b: count for b, count in broker_assignment_count.items() if count < min_acceptable}
        
        logger.info(f"\n Brokers Needing Rebalancing:")
        if overloaded_brokers:
            for broker_id, count in overloaded_brokers.items():
                excess = count - target_replicas_per_broker
                logger.warning(f"  Overloaded: Broker {broker_id} has {count} replicas (excess: {excess:+.1f})")
        if underloaded_brokers:
            for broker_id, count in underloaded_brokers.items():
                deficit = target_replicas_per_broker - count
                logger.warning(f"  Underloaded: Broker {broker_id} has {count} replicas (deficit: {deficit:+.1f})")
        if not overloaded_brokers and not underloaded_brokers:
            logger.info("    All brokers within acceptable range")
            return None
        
        # Process each partition and decide if it needs to move
        for topic_name, topic_data in metadata["topics"].items():
            replication_factor = topic_data["replication_factor"]
            
            for partition_info in topic_data["partitions"]:
                partition_id = partition_info["partition"]
                current_replicas = partition_info["replicas"]
                current_leader = partition_info.get("leader", -1)
                
                partition_key = (topic_name, partition_id)
                partition_size = self.partition_metrics.get(partition_key, PartitionMetrics(topic_name, partition_id)).size_bytes
                
                # Check if ANY current replica is on an overloaded broker
                has_overloaded_replica = any(broker_id in overloaded_brokers for broker_id in current_replicas)
                
                # Only reassign if we have overloaded replicas
                if not has_overloaded_replica:
                    continue
                
                new_replicas = []
                
                # STEP 1: Choose replica brokers intelligently
                # Priority: underloaded > within range > slightly overloaded
                for i in range(replication_factor):
                    best_broker = None
                    best_score = float('inf')
                    
                    for broker_id in eligible_brokers:
                        if broker_id in new_replicas:
                            continue
                        
                        bm = self.broker_metrics[broker_id]
                        
                        if not bm.can_accept_replica(partition_size, self.disk_threshold):
                            continue
                        
                        current_count = broker_assignment_count[broker_id]
                        
                        # Calculate score based on current state and tolerance
                        if current_count < min_acceptable:
                            # Underloaded - highest priority
                            deviation = abs(current_count - target_replicas_per_broker)
                            score = deviation * 0.3  # Low penalty (prefer these)
                        elif current_count <= max_acceptable:
                            # Within range - medium priority
                            deviation = abs(current_count - target_replicas_per_broker)
                            score = deviation * 1.0  # Medium penalty
                        else:
                            # Overloaded - lowest priority (avoid these)
                            deviation = abs(current_count - target_replicas_per_broker)
                            score = deviation * 3.0  # High penalty (avoid these)
                        
                        # Add health consideration
                        score += bm.get_health_score() * 0.1
                        
                        if score < best_score:
                            best_score = score
                            best_broker = broker_id
                    
                    if best_broker is not None:
                        new_replicas.append(best_broker)
                        broker_assignment_count[best_broker] += 1
                
                # STEP 2: Order replicas to balance LEADER distribution
                if len(new_replicas) == replication_factor:
                    # Find broker with lowest leader count
                    leader_candidate = min(new_replicas, key=lambda b: (
                        broker_leader_count[b],
                        self.broker_metrics[b].get_health_score()
                    ))
                    
                    # Reorder: put leader candidate first
                    new_replicas.remove(leader_candidate)
                    new_replicas.insert(0, leader_candidate)
                    
                    # Update tracking (decrement old replicas)
                    for old_broker in current_replicas:
                        if old_broker not in new_replicas:
                            broker_assignment_count[old_broker] -= 1
                    
                    # Track leader
                    if current_leader in broker_leader_count:
                        broker_leader_count[current_leader] -= 1
                    broker_leader_count[leader_candidate] += 1
                    
                    # Add if DIFFERENT from current
                    if set(new_replicas) != set(current_replicas) or new_replicas != current_replicas:
                        reassignment["partitions"].append({
                            "topic": topic_name,
                            "partition": partition_id,
                            "replicas": new_replicas,
                            "log_dirs": ["any"] * len(new_replicas)
                        })
        
        if not reassignment["partitions"]:
            logger.info("No reassignments needed (all brokers within tolerance)")
            return None
        
        logger.info(f"\n Generated plan for {len(reassignment['partitions'])} partition(s)")
        
        logger.info("\n--- Projected Distribution ---")
        for broker_id in eligible_brokers:
            current_replicas = self.broker_metrics[broker_id].replica_count
            projected_replicas = broker_assignment_count[broker_id]
            change_replicas = projected_replicas - current_replicas
            target_diff = projected_replicas - target_replicas_per_broker
            
            current_leaders = self.broker_metrics[broker_id].leader_count
            projected_leaders = broker_leader_count[broker_id]
            change_leaders = projected_leaders - current_leaders
            
            status = " Within range" if min_acceptable <= projected_replicas <= max_acceptable else "⚠️  Needs more work"
            
            logger.info(f"Broker {broker_id}: "
                        f"Replicas {current_replicas} → {projected_replicas} ({change_replicas:+d}, "
                        f"target: {target_replicas_per_broker:.1f}, diff: {target_diff:+.1f}) {status}, "
                        f"Leaders {current_leaders} → {projected_leaders} ({change_leaders:+d})")
        
        return reassignment
    
    def generate_reorder_only_plan_for_leaders(self, metadata: Dict, analysis: Dict) -> Optional[Dict]:
        """
        Generate a plan that ONLY reorders replicas to balance leaders.
        Does NOT move replicas to different brokers.
        Used when replicas are within tolerance but leaders are skewed.
        """
        logger.info("\n[REORDER-ONLY] Generating leader rebalancing plan...")
        
        eligible_brokers = analysis.get('eligible_brokers', [])
        if not eligible_brokers:
            return None
        
        reassignment = {"version": 1, "partitions": []}
        broker_leader_count = defaultdict(int)
        
        # Initialize with current leader counts
        for broker_id in eligible_brokers:
            broker_leader_count[broker_id] = self.broker_metrics[broker_id].leader_count
        
        # Calculate target leaders per broker
        total_leaders = sum(bm.leader_count for bm in self.broker_metrics.values())
        target_leaders_per_broker = total_leaders / len(eligible_brokers)
        
        logger.info(f"Total Leaders: {total_leaders}")
        logger.info(f"Target per Broker: {target_leaders_per_broker:.1f}")
        logger.info(f"\nCurrent Leader Distribution:")
        for broker_id in eligible_brokers:
            current = broker_leader_count[broker_id]
            diff = current - target_leaders_per_broker
            logger.info(f"  Broker {broker_id}: {current} leaders (diff: {diff:+.1f})")
        
        # Process each partition
        for topic_name, topic_data in metadata["topics"].items():
            for partition_info in topic_data["partitions"]:
                partition_id = partition_info["partition"]
                current_replicas = partition_info["replicas"]
                current_leader = partition_info.get("leader", -1)
                
                if not current_replicas:
                    continue
                
                # Find the broker with fewest leaders from current replica set
                best_leader = min(current_replicas, key=lambda b: (
                    broker_leader_count.get(b, 999),  # Prefer low leader count
                    self.broker_metrics.get(b, BrokerMetrics(b)).get_health_score()  # Then health
                ))
                
                # Reorder: put best leader candidate first
                new_replicas = current_replicas.copy()
                if best_leader != new_replicas[0]:
                    new_replicas.remove(best_leader)
                    new_replicas.insert(0, best_leader)
                    
                    # Update tracking
                    if current_leader in broker_leader_count:
                        broker_leader_count[current_leader] -= 1
                    broker_leader_count[best_leader] += 1
                    
                    # Add to plan
                    reassignment["partitions"].append({
                        "topic": topic_name,
                        "partition": partition_id,
                        "replicas": new_replicas,
                        "log_dirs": ["any"] * len(new_replicas)
                    })
        
        if not reassignment["partitions"]:
            return None
        
        logger.info(f"\n Reorder-only plan affects {len(reassignment['partitions'])} partitions")
        logger.info("\n--- Projected Leader Distribution ---")
        for broker_id in eligible_brokers:
            current = self.broker_metrics[broker_id].leader_count
            projected = broker_leader_count[broker_id]
            change = projected - current
            diff = projected - target_leaders_per_broker
            logger.info(f"Broker {broker_id}: {current} → {projected} ({change:+d}, target: {target_leaders_per_broker:.1f}, diff: {diff:+.1f})")
        
        return reassignment
    
    def execute_preferred_leader_election_with_verification(self, dry_run: bool = True, 
                                                            metadata: Dict = None) -> bool:
        """
        ENHANCED: Execute preferred leader election with verification.
        If first attempt doesn't resolve skewness, tries comprehensive reassignment.
        """
        logger.info("\n[REBALANCING] Executing Preferred Leader Election (Phase 1)...")
        
        if dry_run:
            logger.info("🔍 DRY RUN MODE: Simulating leader election")
            return True
        
        for attempt in range(1, self.leader_election_max_retries + 1):
            logger.info(f"\n{'='*80}")
            logger.info(f"Leader Election Attempt {attempt}/{self.leader_election_max_retries}")
            logger.info(f"{'='*80}")
            
            # Execute preferred leader election
            command = [
                f"{self.kafka_bin_dir}/kafka-leader-election.sh",
                "--bootstrap-server", self.bootstrap_server,
                "--election-type", "PREFERRED",
                "--all-topic-partitions"
            ]
            
            logger.warning(f"⚠️  Executing preferred leader election (attempt {attempt})...")
            success, stdout, stderr = self.run_kafka_command(command)
            
            if not success:
                logger.error(f"❌ Leader election failed on attempt {attempt}")
                logger.error(stderr)
                if attempt < self.leader_election_max_retries:
                    logger.info(f"Waiting {self.leader_election_wait_time}s before retry...")
                    time.sleep(self.leader_election_wait_time)
                    continue
                else:
                    return False
            
            logger.info(f" Leader election completed (attempt {attempt})")
            
            # Wait for stabilization
            logger.info(f"Waiting {self.leader_election_wait_time}s for cluster stabilization...")
            time.sleep(self.leader_election_wait_time)
            
            # Verify: Check if leader skewness is resolved
            logger.info("\n[VERIFICATION] Checking if leader skewness is resolved...")
            
            # Refresh metadata
            new_metadata = self.get_cluster_metadata()
            if not new_metadata or not new_metadata["brokers"]:
                logger.error("Failed to retrieve metadata for verification")
                if attempt < self.leader_election_max_retries:
                    continue
                else:
                    return False
            
            # Recalculate broker metrics with new leader counts
            for broker_id in new_metadata["brokers"]:
                if broker_id in self.broker_metrics:
                    self.broker_metrics[broker_id].leader_count = new_metadata["leader_count_per_broker"][broker_id]
            
            # Check if skewness is resolved
            leader_skewness = self._analyze_leader_skewness(self.broker_metrics, new_metadata["brokers"])
            
            if not leader_skewness:
                logger.info(f"\n SUCCESS: Leader skewness RESOLVED after attempt {attempt}!")
                return True
            else:
                logger.warning(f"\n  Leader skewness still detected after attempt {attempt}")
                
                if attempt < self.leader_election_max_retries:
                    logger.info("Retrying preferred leader election...")
                else:
                    logger.warning("\n  Preferred leader election did not resolve skewness")
                    logger.info("Moving to Phase 2: Comprehensive Cluster-Wide Reassignment")
                    return False
        
        return False
    
    def execute_comprehensive_rebalancing(self, metadata: Dict, analysis: Dict, 
                                          dry_run: bool = True) -> bool:
        """
        ENHANCED: Execute comprehensive cluster-wide rebalancing.
        This is Phase 2 - used when preferred leader election doesn't resolve skewness.
        Performs full replica shuffle with batch-wise assignment.
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 2: COMPREHENSIVE CLUSTER-WIDE REBALANCING")
        logger.info("=" * 80)
        logger.info("This phase will:")
        logger.info("  1. Generate cluster-wide replica shuffle plan")
        logger.info("  2. Execute with throttling and batch-wise assignment")
        logger.info("  3. Run preferred leader election afterward")
        logger.info("  4. Verify final state")
        
        # Generate comprehensive reassignment plan
        reassignment_plan = self.generate_intelligent_reassignment_plan(metadata, analysis)
        
        if not reassignment_plan:
            logger.info("No replica reassignments needed (replicas within tolerance)")
            
            # CRITICAL: Check if leaders are skewed even though replicas are OK
            leader_skew_detected = analysis.get("has_skewness", False) and "Leader Distribution" in analysis.get("skewness_types", [])
            
            if leader_skew_detected:
                logger.warning("  CRITICAL: Leaders are skewed even though replicas are balanced!")
                logger.info("This means partitions have the WRONG preferred leader.")
                logger.info("Solution: Generate REORDER-ONLY plan (same brokers, different order)")
                
                # Generate a plan that ONLY reorders replicas for leader balance
                # Don't enforce tolerance - allow any broker to be used
                reorder_plan = self.generate_reorder_only_plan_for_leaders(metadata, analysis)
                
                if reorder_plan:
                    logger.info(f"\n Generated reorder-only plan for {len(reorder_plan['partitions'])} partitions")
                    logger.info("This will change replica ORDER to balance leaders (no data movement)")
                    
                    if not dry_run:
                        success = self.execute_reassignment_batch_wise(reorder_plan, dry_run=dry_run)
                        
                        if success:
                            logger.info("\n[POST-REORDER] Running preferred leader election...")
                            time.sleep(30)
                            
                            election_success = self.execute_preferred_leader_election_with_verification(
                                dry_run=False, metadata=metadata
                            )
                            
                            if election_success:
                                logger.info(" Leader rebalancing successful")
                            return True
                else:
                    logger.warning("  Could not generate reorder-only plan")
                    return False
            else:
                logger.info(" Both replicas and leaders are balanced - nothing to do")
                return True
        
        # Execute with batch-wise assignment
        success = self.execute_reassignment_batch_wise(reassignment_plan, dry_run=dry_run)
        
        if not success:
            logger.error(" Batch-wise reassignment failed")
            return False
        
        if dry_run:
            logger.info(" DRY RUN MODE: Would execute preferred leader election after reassignment")
            return True
        
        # After reassignment completes, run preferred leader election
        logger.info("\n[POST-REASSIGNMENT] Running preferred leader election...")
        time.sleep(30)  # Wait for stabilization
        
        election_success = self.execute_preferred_leader_election_with_verification(
            dry_run=False, metadata=metadata
        )
        
        if election_success:
            logger.info(" Post-reassignment leader election successful")
        else:
            logger.warning("  Post-reassignment leader election had issues")
        
        return True
    
    def execute_reassignment_batch_wise(self, reassignment_plan: Dict, dry_run: bool = True,
                                        throttle_bytes_per_sec: int = None) -> bool:
        """
        ENHANCED: Execute partition reassignment in batches with throttling.
        Processes partitions in configurable batch sizes to reduce cluster impact.
        """
        logger.info("\n[REBALANCING] Executing Batch-Wise Partition Reassignment...")
        
        if throttle_bytes_per_sec is None:
            throttle_bytes_per_sec = self.config['reassignment']['throttle_bytes_per_sec']
        
        logger.info(f"Throttle: {self._format_bytes(throttle_bytes_per_sec)}/s")
        logger.info(f"Batch Size: {self.batch_size} partitions")
        logger.info(f"Batch Wait Time: {self.batch_wait_time}s")
        
        total_partitions = len(reassignment_plan["partitions"])
        batches = [reassignment_plan["partitions"][i:i + self.batch_size] 
                   for i in range(0, total_partitions, self.batch_size)]
        
        logger.info(f"\nTotal Partitions: {total_partitions}")
        logger.info(f"Number of Batches: {len(batches)}")
        
        if dry_run:
            logger.info("\n🔍 DRY RUN MODE: Batch plan preview")
            for batch_num, batch in enumerate(batches, 1):
                logger.info(f"\nBatch {batch_num}/{len(batches)}: {len(batch)} partitions")
                for i, partition in enumerate(batch[:5], 1):  # Show first 5
                    logger.info(f"  {i}. {partition['topic']}-{partition['partition']}: {partition['replicas']}")
                if len(batch) > 5:
                    logger.info(f"  ... and {len(batch) - 5} more")
            return True
        
        # Set global throttle once
        logger.info("\n[SETUP] Setting replication throttle...")
        throttle_command = [
            f"{self.kafka_bin_dir}/kafka-configs.sh",
            "--bootstrap-server", self.bootstrap_server,
            "--alter",
            "--add-config", f"leader.replication.throttled.rate={throttle_bytes_per_sec},follower.replication.throttled.rate={throttle_bytes_per_sec}",
            "--entity-type", "brokers",
            "--entity-default"
        ]
        
        success, stdout, stderr = self.run_kafka_command(throttle_command)
        if success:
            logger.info(" Throttle set globally")
        else:
            logger.warning(f"  Failed to set throttle: {stderr}")
        
        # Execute batches
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"\n{'='*80}")
            logger.info(f"Executing Batch {batch_num}/{len(batches)}")
            logger.info(f"{'='*80}")
            logger.info(f"Partitions in this batch: {len(batch)}")
            
            # Create batch-specific reassignment plan
            batch_plan = {
                "version": 1,
                "partitions": batch
            }
            
            batch_file = f"{self.reassignment_json_file}.batch{batch_num}"
            
            try:
                with open(batch_file, 'w') as f:
                    json.dump(batch_plan, f, indent=2)
                logger.info(f"Batch plan written to: {batch_file}")
            except Exception as e:
                logger.error(f"Failed to write batch file: {str(e)}")
                continue
            
            # Execute batch
            command = [
                f"{self.kafka_bin_dir}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.bootstrap_server,
                "--reassignment-json-file", batch_file,
                "--execute"
            ]
            
            logger.warning(f"⚠️  Executing batch {batch_num}...")
            success, stdout, stderr = self.run_kafka_command(command)
            
            if not success:
                logger.error(f" Batch {batch_num} failed")
                logger.error(stderr)
                self._remove_throttle()
                return False
            
            logger.info(f" Batch {batch_num} initiated")
            
            # Monitor batch completion
            logger.info(f"Monitoring batch {batch_num} progress...")
            batch_completed = self._monitor_batch_completion(batch_file, timeout_seconds=1800)
            
            if not batch_completed:
                logger.warning(f"  Batch {batch_num} monitoring timeout")
            else:
                logger.info(f" Batch {batch_num} completed")
            
            # Wait before next batch (except for last batch)
            if batch_num < len(batches):
                logger.info(f"Waiting {self.batch_wait_time}s before next batch...")
                time.sleep(self.batch_wait_time)
        
        logger.info("\n All batches completed")
        
        # Remove throttle
        self._remove_throttle()
        
        return True
    
    def _monitor_batch_completion(self, batch_file: str, timeout_seconds: int = 1800) -> bool:
        """Monitor completion of a batch reassignment."""
        start_time = time.time()
        check_interval = 30
        
        while time.time() - start_time < timeout_seconds:
            command = [
                f"{self.kafka_bin_dir}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.bootstrap_server,
                "--reassignment-json-file", batch_file,
                "--verify"
            ]
            
            success, stdout, stderr = self.run_kafka_command(command)
            
            if success:
                in_progress = stdout.count("in progress")
                completed = stdout.count("successfully completed")
                
                logger.info(f"  Progress: {completed} completed, {in_progress} in progress")
                
                if in_progress == 0:
                    logger.info("    Batch completed")
                    return True
            
            time.sleep(check_interval)
        
        return False
    
    def execute_preferred_leader_election(self, dry_run: bool = True) -> bool:
        """
        Simple preferred leader election (legacy method for compatibility).
        For new workflow, use execute_preferred_leader_election_with_verification().
        """
        logger.info("\n[REBALANCING] Executing Preferred Leader Election...")
        
        if dry_run:
            logger.info("🔍 DRY RUN MODE: Simulating election")
            return True
        
        command = [
            f"{self.kafka_bin_dir}/kafka-leader-election.sh",
            "--bootstrap-server", self.bootstrap_server,
            "--election-type", "PREFERRED",
            "--all-topic-partitions"
        ]
        
        logger.warning("  Executing leader election...")
        success, stdout, stderr = self.run_kafka_command(command)
        
        if success:
            logger.info(" Completed")
            return True
        else:
            logger.error(" Failed")
            logger.error(stderr)
            return False
    
    def execute_reassignment_with_throttling(self, reassignment_plan: Dict, dry_run: bool = True,
                                            throttle_bytes_per_sec: int = None) -> bool:
        """
        Legacy single-pass reassignment method (for compatibility).
        For new workflow, use execute_reassignment_batch_wise().
        """
        logger.info("\n[REBALANCING] Executing Partition Reassignment...")
        
        if throttle_bytes_per_sec is None:
            throttle_bytes_per_sec = self.config['reassignment']['throttle_bytes_per_sec']
        
        logger.info(f"Throttle: {self._format_bytes(throttle_bytes_per_sec)}/s")
        
        try:
            with open(self.reassignment_json_file, 'w') as f:
                json.dump(reassignment_plan, f, indent=2)
            logger.info(f"Plan written to: {self.reassignment_json_file}")
        except Exception as e:
            logger.error(f"Failed to write file: {str(e)}")
            return False
        
        if dry_run:
            logger.info("🔍 DRY RUN MODE: Plan generated but not executed")
            logger.info("\n--- Preview (first 10) ---")
            for partition in reassignment_plan["partitions"][:10]:
                logger.info(f"{partition['topic']}-{partition['partition']}: {partition['replicas']}")
            if len(reassignment_plan["partitions"]) > 10:
                logger.info(f"... and {len(reassignment_plan['partitions']) - 10} more")
            return True
        
        # Set throttle
        throttle_command = [
            f"{self.kafka_bin_dir}/kafka-configs.sh",
            "--bootstrap-server", self.bootstrap_server,
            "--alter",
            "--add-config", f"leader.replication.throttled.rate={throttle_bytes_per_sec},follower.replication.throttled.rate={throttle_bytes_per_sec}",
            "--entity-type", "brokers",
            "--entity-default"
        ]
        
        success, stdout, stderr = self.run_kafka_command(throttle_command)
        if success:
            logger.info(" Throttle set")
        
        # Execute
        command = [
            f"{self.kafka_bin_dir}/kafka-reassign-partitions.sh",
            "--bootstrap-server", self.bootstrap_server,
            "--reassignment-json-file", self.reassignment_json_file,
            "--execute"
        ]
        
        logger.warning("  Executing...")
        success, stdout, stderr = self.run_kafka_command(command)
        
        if success:
            logger.info(" Reassignment initiated")
            return True
        else:
            logger.error(" Failed")
            logger.error(stderr)
            return False
    
    def monitor_reassignment_progress(self, timeout_seconds: int = 3600) -> bool:
        """Monitor reassignment progress."""
        logger.info("\n[MONITORING] Monitoring Progress...")
        
        start_time = time.time()
        check_interval = 30
        
        while time.time() - start_time < timeout_seconds:
            command = [
                f"{self.kafka_bin_dir}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.bootstrap_server,
                "--reassignment-json-file", self.reassignment_json_file,
                "--verify"
            ]
            
            success, stdout, stderr = self.run_kafka_command(command)
            
            if success:
                in_progress = stdout.count("in progress")
                completed = stdout.count("successfully completed")
                
                logger.info(f"Progress: {completed} completed, {in_progress} in progress")
                
                if in_progress == 0:
                    logger.info(" All completed")
                    self._remove_throttle()
                    return True
            
            time.sleep(check_interval)
        
        logger.warning(" Timeout reached")
        return False
    
    def _remove_throttle(self):
        """Remove replication throttle."""
        logger.info("Removing throttle...")
        command = [
            f"{self.kafka_bin_dir}/kafka-configs.sh",
            "--bootstrap-server", self.bootstrap_server,
            "--alter",
            "--delete-config", "leader.replication.throttled.rate,follower.replication.throttled.rate",
            "--entity-type", "brokers",
            "--entity-default"
        ]
        success, stdout, stderr = self.run_kafka_command(command)
        if success:
            logger.info(" Throttle removed")
    
    def verify_post_balance_stability(self, metadata: Dict) -> bool:
        """Verify cluster stability after rebalancing."""
        logger.info("\n[VERIFICATION] Verifying Stability...")
        
        time.sleep(30)
        
        new_metadata = self.get_cluster_metadata()
        
        if not self.check_under_replicated_partitions():
            logger.warning("  Under-replicated partitions detected")
            return False
        
        # Check if skewness is truly resolved
        logger.info("\n[FINAL CHECK] Analyzing final cluster state...")
        final_analysis = self.analyze_skewness_with_metrics(new_metadata)
        
        if final_analysis["has_skewness"]:
            logger.warning("  Some skewness still present:")
            logger.warning(f"Types: {', '.join(final_analysis['skewness_types'])}")
            return False
        
        logger.info(" VERIFICATION PASSED - Cluster is balanced")
        return True
    
    def run_comprehensive_check(self, dry_run: bool = True, execute: bool = False,
                                monitor: bool = True, verify: bool = True) -> bool:
        """
        ENHANCED: Run comprehensive check and rebalancing workflow.
        Now includes two-phase leader election and batch-wise reassignment.
        """
        # Phase 0 (Auto-discovery) is done in __init__
        
        # Phase 1: Discovery & Pre-checks
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 1: BROKER DISCOVERY & PRE-FLIGHT CHECKS")
        logger.info("=" * 80)
        
        broker_info = self.discover_brokers_from_zookeeper()
        if not broker_info:
            logger.error("Failed to discover brokers")
            return False
        
        prechecks = [
            ("Cluster Health", self.check_cluster_health),
            ("Under-Replicated Partitions", self.check_under_replicated_partitions),
            ("Controller Status", self.check_controller_status),
            ("Ongoing Reassignments", self.check_ongoing_reassignments)
        ]
        
        for check_name, check_func in prechecks:
            if not check_func():
                logger.error(f"\n Pre-check failed: {check_name}")
                return False
        
        logger.info("\n All pre-checks passed!")
        
        # Phase 2: Metrics & Analysis
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 2: OPENTSDB METRICS COLLECTION & ENHANCED ANALYSIS")
        logger.info("=" * 80)
        
        metadata = self.get_cluster_metadata()
        if not metadata or not metadata["brokers"]:
            logger.error("Failed to retrieve metadata")
            return False
        
        analysis = self.analyze_skewness_with_metrics(metadata)
        
        # Phase 3: Intelligent Rebalancing
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 3: INTELLIGENT REBALANCING (TWO-PHASE APPROACH)")
        logger.info("=" * 80)
        
        if not analysis["has_skewness"]:
            logger.info("\n CLUSTER STATUS: HEALTHY & BALANCED")
            return True
        
        logger.warning("\n  CLUSTER STATUS: SKEWNESS DETECTED")
        logger.warning(f"Types: {', '.join(analysis['skewness_types'])}")
        
        if analysis["critical_brokers"]:
            logger.warning(f"Critical Brokers: {analysis['critical_brokers']}")
        
        logger.info("\nRecommendations:")
        for i, rec in enumerate(analysis["recommendations"], 1):
            logger.info(f"  {i}. {rec}")
        
        comprehensive_rebalancing_executed = False
        
        # Handle Leader Skewness (Two-Phase Approach)
        if analysis["leader_skew_detected"]:
            logger.info("\n" + "=" * 80)
            logger.info("LEADER SKEWNESS HANDLING")
            logger.info("=" * 80)
            
            leader_resolved = self.execute_preferred_leader_election_with_verification(
                dry_run=not execute, metadata=metadata
            )
            
            if not leader_resolved and execute:
                logger.warning("\n⚠️  Phase 1 (Preferred Leader Election) did not fully resolve skewness")
                logger.info("Proceeding to Phase 2 (Comprehensive Rebalancing)...")
                
                # This will handle BOTH leader and replica skewness comprehensively
                self.execute_comprehensive_rebalancing(metadata, analysis, dry_run=not execute)
                comprehensive_rebalancing_executed = True
                logger.info("\n Comprehensive rebalancing completed (handles both leader + replica skewness)")
        
        # Handle Replica Skewness (Batch-Wise Approach)
        # CRITICAL: Skip if comprehensive rebalancing already ran (it handles replicas too!)
        if analysis["replica_skew_detected"] and not comprehensive_rebalancing_executed:
            logger.info("\n" + "=" * 80)
            logger.info("REPLICA SKEWNESS HANDLING (BATCH-WISE)")
            logger.info("=" * 80)
            
            reassignment_plan = self.generate_intelligent_reassignment_plan(metadata, analysis)
            
            if reassignment_plan:
                self.execute_reassignment_batch_wise(reassignment_plan, dry_run=not execute)
                
                if execute and monitor:
                    # Individual batch monitoring is done within execute_reassignment_batch_wise
                    logger.info("\n[POST-EXECUTION] All batches processed")
            
        elif comprehensive_rebalancing_executed:
            logger.info("\n" + "=" * 80)
            logger.info("REPLICA SKEWNESS HANDLING")
            logger.info("=" * 80)
            logger.info(" Skipped - already handled by comprehensive rebalancing")
        
        # Phase 4: Verification
        if execute and verify:
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 4: POST-BALANCE VERIFICATION")
            logger.info("=" * 80)
            verification_success = self.verify_post_balance_stability(metadata)
            
            if not verification_success:
                logger.warning("  Verification found remaining issues")
                return False
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        
        if execute:
            logger.info(" Rebalancing operations executed")
            logger.info("    - Two-phase leader election with verification")
            logger.info("    - Batch-wise replica reassignment")
            logger.info("    - Replica count and disk usage balanced")
        else:
            logger.info(" Dry-run completed - no changes made")
            logger.info("    Run with --execute flag to apply changes")
        
        logger.info(f"\nLog file: {LOG_FILE}")
        logger.info(f"Reassignment file: {self.reassignment_json_file}")
        
        return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Kafka Partition Skewness Detection and Rebalancing Tool (v4.3 Auto-Discovery)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run with config file (auto-discovers paths)
  python3 kafka_partition_balancer.py --config production.yaml --dry-run
 
  # Execute rebalancing with monitoring and verification
  python3 kafka_partition_balancer.py --config production.yaml --execute --monitor --verify
 
  # Execute without verification
  python3 kafka_partition_balancer.py --config production.yaml --execute --monitor
        """
    )
    
    parser.add_argument('--config', required=True, help='Path to YAML configuration file (still needed for OpenTSDB, Kafka bootstrap, and thresholds)')
    parser.add_argument('--dry-run', action='store_true', default=True, help='Dry-run mode (default)')
    parser.add_argument('--execute', action='store_true', help='Execute rebalancing')
    parser.add_argument('--monitor', action='store_true', default=True, help='Monitor progress')
    parser.add_argument('--verify', action='store_true', default=True, help='Verify after rebalancing')
    
    args = parser.parse_args()
    
    balancer = KafkaPartitionBalancer(config_file=args.config)
    
    try:
        success = balancer.run_comprehensive_check(
            dry_run=not args.execute,
            execute=args.execute,
            monitor=args.monitor,
            verify=args.verify
        )
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("\n\nOperation interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\nUnexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

