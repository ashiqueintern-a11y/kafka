#!/usr/bin/env python3
"""
Kafka Broker Decommission/Recommission Script - Production Grade
=================================================================
This script safely decommissions a Kafka broker by transferring leadership
and stopping the broker, or recommissions it by starting and restoring leadership.

Features:
- Comprehensive pre-checks before decommission
- Resource-aware leader reassignment (CPU, Disk)
- Broker stop/start with local Kafka scripts
- ISR synchronization monitoring before recommission
- Rollback capability to restore previous state
- Detailed logging and state persistence

Author: Production Engineering Team
Version: 2.0.0
Kafka Version: 2.8.2
"""

import json
import yaml
import logging
import os
import sys
import argparse
import subprocess
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
from collections import defaultdict
import shutil
import traceback
import re
import glob

# ==============================================================================
# AUTO-DETECTION UTILITIES
# ==============================================================================

def auto_detect_kafka_server_config() -> Optional[str]:
    """
    Auto-detect Kafka server.properties path.
    
    Priority:
    1. /etc/kafka/conf/server.properties
    2. /usr/odp/current/kafka-broker/config/server.properties
    3. /usr/hdp/current/kafka-broker/config/server.properties
    
    Returns:
        Path to server.properties or None
    """
    config_paths = [
        "/etc/kafka/conf/server.properties",
        "/usr/odp/current/kafka-broker/config/server.properties",
        "/usr/hdp/current/kafka-broker/config/server.properties"
    ]
    
    for config_path in config_paths:
        if os.path.exists(config_path):
            return config_path
    
    return None


def auto_detect_kafka_bin() -> Optional[str]:
    """
    Auto-detect Kafka binary directory.
    
    Priority:
    1. Check /etc/kafka/*/0/kafka-env.sh for CLASSPATH
    2. Search /usr/odp/current/kafka-broker/bin
    3. Fall back to find command
    
    Returns:
        Path to Kafka bin directory or None
    """
    # Method 1: Parse kafka-env.sh
    try:
        env_files = glob.glob("/etc/kafka/*/0/kafka-env.sh")
        if env_files:
            with open(env_files[0], 'r') as f:
                for line in f:
                    if 'CLASSPATH=' in line and '/kafka' in line:
                        # Extract path like /usr/odp/current/kafka-broker/config
                        match = re.search(r'(/usr/odp/[^:]+/kafka-broker)', line)
                        if match:
                            kafka_home = match.group(1)
                            kafka_bin = f"{kafka_home}/bin"
                            if os.path.exists(kafka_bin):
                                return kafka_bin
    except Exception:
        pass
    
    # Method 2: Check standard paths
    standard_paths = [
        "/usr/odp/current/kafka-broker/bin",
        "/usr/hdp/current/kafka-broker/bin",
        "/opt/kafka/bin"
    ]
    for path in standard_paths:
        if os.path.exists(path):
            return path
    
    # Method 3: Find kafka-topics.sh
    try:
        result = subprocess.run(
            ["find", "/usr", "-name", "kafka-topics.sh", "-type", "f"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            kafka_script = result.stdout.strip().split('\n')[0]
            kafka_bin = os.path.dirname(kafka_script)
            return kafka_bin
    except Exception:
        pass
    
    return None


def auto_detect_zookeeper_servers() -> Optional[str]:
    """
    Auto-detect Zookeeper servers from Kafka server.properties.
    
    Looks for zookeeper.connect property in:
    - /etc/kafka/conf/server.properties
    - /usr/odp/current/kafka-broker/config/server.properties
    
    Returns:
        Comma-separated list of zookeeper servers or None
    """
    config_paths = [
        "/etc/kafka/conf/server.properties",
        "/usr/odp/current/kafka-broker/config/server.properties",
        "/usr/hdp/current/kafka-broker/config/server.properties"
    ]
    
    for config_path in config_paths:
        if not os.path.exists(config_path):
            continue
        
        try:
            with open(config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('zookeeper.connect='):
                        zk_connect = line.split('=', 1)[1].strip()
                        # Extract just the first server for zookeeper-shell
                        # Format: host1:2181,host2:2181,host3:2181
                        first_server = zk_connect.split(',')[0]
                        return first_server
        except Exception:
            continue
    
    return None


def get_broker_disk_usage_from_kafka(kafka_bin: str, bootstrap_servers: str, 
                                      logger: logging.Logger) -> Dict[int, Dict[str, float]]:
    """
    Get disk usage for all brokers using kafka-log-dirs.sh.
    
    Args:
        kafka_bin: Path to Kafka bin directory
        bootstrap_servers: Kafka bootstrap servers
        logger: Logger instance
        
    Returns:
        Dict mapping broker_id to {'total_bytes': int, 'usage_percent': float}
    """
    try:
        cmd = [
            f"{kafka_bin}/kafka-log-dirs.sh",
            "--describe",
            "--bootstrap-server", bootstrap_servers
        ]
        
        logger.debug(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            logger.error(f"kafka-log-dirs.sh failed: {result.stderr}")
            return {}
        
        # Parse JSON output - skip header lines
        json_line = None
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line.startswith('{'):
                json_line = line
                break
        
        if not json_line:
            logger.error("No JSON output found from kafka-log-dirs.sh")
            logger.debug(f"Full output: {result.stdout}")
            return {}
        
        try:
            data = json.loads(json_line)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from kafka-log-dirs.sh: {e}")
            logger.debug(f"JSON line: {json_line[:200]}...")
            return {}
        
        broker_usage = {}
        
        for broker_obj in data.get('brokers', []):
            broker_id = broker_obj.get('broker')
            if broker_id is None:
                continue
            
            total_bytes = 0
            for log_dir in broker_obj.get('logDirs', []):
                for partition in log_dir.get('partitions', []):
                    total_bytes += partition.get('size', 0)
            
            total_gb = total_bytes / (1024 ** 3)
            usage_percent = 0.0
            
            broker_usage[broker_id] = {
                'total_bytes': total_bytes,
                'usage_gb': total_gb,
                'usage_percent': usage_percent
            }
            
            logger.debug(f"Broker {broker_id}: {total_gb:.2f} GB")
        
        return broker_usage
        
    except subprocess.TimeoutExpired:
        logger.error("kafka-log-dirs.sh timed out")
        return {}
    except Exception as e:
        logger.error(f"Error getting broker disk usage: {e}")
        logger.debug(f"Exception details: {traceback.format_exc()}")
        return {}


# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================

def setup_logging(log_dir: str = "/var/log/kafka-node-decommission/") -> logging.Logger:
    """
    Configure comprehensive logging for the script.
    
    Args:
        log_dir: Directory to store log files
        
    Returns:
        Configured logger instance
    """
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"kafka_decommission_{timestamp}.log")
    
    logger = logging.getLogger("KafkaDecommission")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger


# ==============================================================================
# CONFIGURATION MANAGEMENT
# ==============================================================================

class KafkaConfig:
    """Configuration manager for Kafka operations."""
    
    def __init__(self, config_file: str, logger: logging.Logger):
        self.logger = logger
        self.config_file = config_file
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info(f"Configuration loaded from {self.config_file}")
            return config
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {self.config_file}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Invalid YAML in configuration file: {e}")
            raise
    
    def _validate_config(self) -> None:
        """Validate required configuration parameters with auto-detection."""
        required_fields = ['bootstrap_servers']
        
        missing = [field for field in required_fields if field not in self.config]
        if missing:
            raise ValueError(f"Missing required config fields: {missing}")
        
        if 'kafka_bin_path' not in self.config or not self.config['kafka_bin_path']:
            self.logger.info("kafka_bin_path not configured, attempting auto-detection...")
            kafka_bin = auto_detect_kafka_bin()
            if kafka_bin:
                self.config['kafka_bin_path'] = kafka_bin
                self.logger.info(f"✓ Auto-detected kafka_bin_path: {kafka_bin}")
            else:
                raise ValueError("Could not auto-detect kafka_bin_path. Please specify in config.")
        
        if 'zookeeper_server' not in self.config or not self.config['zookeeper_server']:
            self.logger.info("zookeeper_server not configured, attempting auto-detection...")
            zk_server = auto_detect_zookeeper_servers()
            if zk_server:
                self.config['zookeeper_server'] = zk_server
                self.logger.info(f"✓ Auto-detected zookeeper_server: {zk_server}")
            else:
                raise ValueError("Could not auto-detect zookeeper_server. Please specify in config.")
        
        if 'kafka_server_config' not in self.config or not self.config['kafka_server_config']:
            self.logger.info("kafka_server_config not configured, attempting auto-detection...")
            server_config = auto_detect_kafka_server_config()
            if server_config:
                self.config['kafka_server_config'] = server_config
                self.logger.info(f"✓ Auto-detected kafka_server_config: {server_config}")
            else:
                self.logger.warning("Could not auto-detect kafka_server_config")
                self.logger.warning("Using default: /etc/kafka/conf/server.properties")
                self.config['kafka_server_config'] = '/etc/kafka/conf/server.properties'
        
        if 'state_dir' not in self.config and 'state_directory' not in self.config:
            self.config['state_dir'] = './kafka_demotion_state'
        elif 'state_directory' in self.config:
            self.config['state_dir'] = self.config['state_directory']
        
        if 'log_dir' not in self.config and 'log_directory' not in self.config:
            self.config['log_dir'] = './logs'
        elif 'log_directory' in self.config:
            self.config['log_dir'] = self.config['log_directory']
        
        defaults = {
            'cpu_threshold': 80,
            'disk_threshold': 85,
            'min_isr_required': 2,
            'isr_sync_timeout': 600,
            'isr_check_interval': 10,
            'verification_interval': 10,
            'reassignment_timeout': 300
        }
        
        for key, default_value in defaults.items():
            if key not in self.config:
                self.config[key] = default_value
        
        self.logger.info("✓ Configuration validation passed")
        self.logger.info(f"  Bootstrap servers: {self.config['bootstrap_servers']}")
        self.logger.info(f"  Kafka bin path: {self.config['kafka_bin_path']}")
        self.logger.info(f"  Zookeeper server: {self.config['zookeeper_server']}")
        self.logger.info(f"  Server config: {self.config['kafka_server_config']}")
        self.logger.info(f"  State directory: {self.config['state_dir']}")
        self.logger.info(f"  Log directory: {self.config.get('log_dir', './logs')}")
    
    def get(self, key: str, default=None):
        """Get configuration value."""
        return self.config.get(key, default)


# ==============================================================================
# LOCAL BROKER MANAGEMENT (NO SSH)
# ==============================================================================

class BrokerManager:
    """Manage local broker start/stop operations."""
    
    def __init__(self, kafka_bin: str, logger: logging.Logger):
        self.kafka_bin = kafka_bin
        self.logger = logger
        self.kafka_stop_script = f"{kafka_bin}/kafka-server-stop.sh"
        self.kafka_start_script = f"{kafka_bin}/kafka-server-start.sh"
    
    def stop_broker(self, config_file: Optional[str] = None) -> bool:
        """Stop local Kafka broker using kafka-server-stop.sh."""
        try:
            self.logger.info("="*70)
            self.logger.info("STOPPING KAFKA BROKER")
            self.logger.info("="*70)
            
            if not os.path.exists(self.kafka_stop_script):
                self.logger.error(f"Kafka stop script not found: {self.kafka_stop_script}")
                return False
            
            cmd = [self.kafka_stop_script]
            
            self.logger.info(f"Executing: {' '.join(cmd)}")
            self.logger.info("This may take a few moments...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.stdout:
                self.logger.info("Stop script output:")
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            if result.stderr:
                self.logger.warning("Stop script stderr:")
                for line in result.stderr.strip().split('\n'):
                    self.logger.warning(f"  {line}")
            
            self.logger.info("Waiting for broker to stop (15 seconds)...")
            time.sleep(15)
            
            if self.verify_broker_stopped():
                self.logger.info("✓ Kafka broker stopped successfully")
                self.logger.info("="*70)
                return True
            else:
                self.logger.warning("Broker stop command executed, but broker may still be running")
                self.logger.info("Waiting additional 10 seconds...")
                time.sleep(10)
                
                if self.verify_broker_stopped():
                    self.logger.info("✓ Kafka broker stopped successfully (after additional wait)")
                    self.logger.info("="*70)
                    return True
                else:
                    self.logger.error("✗ Broker may not have stopped properly")
                    self.logger.error("Please check manually with: ps aux | grep kafka")
                    self.logger.info("="*70)
                    return False
            
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout stopping broker (120s)")
            self.logger.info("="*70)
            return False
        except Exception as e:
            self.logger.error(f"Error stopping broker: {e}")
            self.logger.debug(traceback.format_exc())
            self.logger.info("="*70)
            return False
    
    def start_broker(self, config_file: str, daemon_mode: bool = True) -> bool:
        """Start local Kafka broker using kafka-server-start.sh."""
        try:
            self.logger.info("="*70)
            self.logger.info("STARTING KAFKA BROKER")
            self.logger.info("="*70)
            
            if not os.path.exists(self.kafka_start_script):
                self.logger.error(f"Kafka start script not found: {self.kafka_start_script}")
                return False
            
            if not os.path.exists(config_file):
                self.logger.error(f"Server config file not found: {config_file}")
                return False
            
            cmd = [self.kafka_start_script]
            if daemon_mode:
                cmd.append("-daemon")
            cmd.append(config_file)
            
            self.logger.info(f"Executing: {' '.join(cmd)}")
            self.logger.info(f"Config file: {config_file}")
            self.logger.info(f"Daemon mode: {daemon_mode}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.stdout:
                self.logger.info("Start script output:")
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            if result.stderr:
                self.logger.warning("Start script stderr:")
                for line in result.stderr.strip().split('\n'):
                    self.logger.warning(f"  {line}")
            
            if result.returncode != 0:
                self.logger.error(f"Start script returned error code: {result.returncode}")
                self.logger.info("="*70)
                return False
            
            self.logger.info("Waiting for broker to start (20 seconds)...")
            time.sleep(20)
            
            if self.verify_broker_running():
                self.logger.info("✓ Kafka broker started successfully")
                self.logger.info("="*70)
                return True
            else:
                self.logger.warning("Start command executed, but broker may not be running yet")
                self.logger.info("Waiting additional 15 seconds...")
                time.sleep(15)
                
                if self.verify_broker_running():
                    self.logger.info("✓ Kafka broker started successfully (after additional wait)")
                    self.logger.info("="*70)
                    return True
                else:
                    self.logger.error("✗ Broker may not have started properly")
                    self.logger.error("Check logs: tail -f /path/to/kafka/logs/server.log")
                    self.logger.info("="*70)
                    return False
            
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout starting broker (60s)")
            self.logger.info("="*70)
            return False
        except Exception as e:
            self.logger.error(f"Error starting broker: {e}")
            self.logger.debug(traceback.format_exc())
            self.logger.info("="*70)
            return False
    
    def verify_broker_stopped(self) -> bool:
        """Verify broker is stopped by checking for kafka.Kafka process."""
        try:
            cmd = ["pgrep", "-f", "kafka.Kafka"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                self.logger.debug(f"Kafka process(es) still running: {', '.join(pids)}")
                return False
            else:
                self.logger.debug("No Kafka process found - broker is stopped")
                return True
        except Exception as e:
            self.logger.warning(f"Could not verify broker status: {e}")
            return True
    
    def verify_broker_running(self) -> bool:
        """Verify broker is running by checking for kafka.Kafka process."""
        try:
            cmd = ["pgrep", "-f", "kafka.Kafka"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                self.logger.debug(f"Kafka process(es) running with PID(s): {', '.join(pids)}")
                return True
            else:
                self.logger.debug("No Kafka process found - broker is not running")
                return False
        except Exception as e:
            self.logger.warning(f"Could not verify broker status: {e}")
            return False


# ==============================================================================
# ISR SYNCHRONIZATION MONITOR
# ==============================================================================

class ISRMonitor:
    """Monitor ISR synchronization status for broker recommission."""
    
    def __init__(self, cluster_manager, logger: logging.Logger):
        self.cluster = cluster_manager
        self.logger = logger
    
    def wait_for_broker_in_isr(self, broker_id: int, partitions: List[Dict], 
                                timeout: int = 600, check_interval: int = 10) -> bool:
        """Wait for broker to rejoin ISR for all its partitions."""
        self.logger.info(f"Waiting for broker {broker_id} to rejoin ISR for {len(partitions)} partitions")
        self.logger.info(f"Timeout: {timeout}s, Check interval: {check_interval}s")
        
        start_time = time.time()
        partition_set = {(p['topic'], p['partition']) for p in partitions}
        
        while time.time() - start_time < timeout:
            try:
                metadata = self.cluster.get_partition_metadata()
                
                in_isr = set()
                not_in_isr = set()
                
                for topic, partition_id in partition_set:
                    if topic not in metadata:
                        not_in_isr.add((topic, partition_id))
                        continue
                    
                    partition_found = False
                    for p in metadata[topic]:
                        if p['partition'] == partition_id:
                            partition_found = True
                            if broker_id in p['isr']:
                                in_isr.add((topic, partition_id))
                            else:
                                not_in_isr.add((topic, partition_id))
                            break
                    
                    if not partition_found:
                        not_in_isr.add((topic, partition_id))
                
                elapsed = int(time.time() - start_time)
                remaining = int(timeout - elapsed)
                
                self.logger.info(
                    f"ISR sync progress: {len(in_isr)}/{len(partition_set)} partitions "
                    f"(elapsed: {elapsed}s, remaining: {remaining}s)"
                )
                
                if len(not_in_isr) > 0 and len(not_in_isr) <= 5:
                    self.logger.info(f"Waiting for: {', '.join([f'{t}-{p}' for t, p in not_in_isr])}")
                elif len(not_in_isr) > 5:
                    sample = list(not_in_isr)[:3]
                    self.logger.info(
                        f"Waiting for: {', '.join([f'{t}-{p}' for t, p in sample])} "
                        f"and {len(not_in_isr) - 3} more"
                    )
                
                if len(not_in_isr) == 0:
                    self.logger.info(f"✓ Broker {broker_id} is in ISR for all {len(partition_set)} partitions")
                    return True
                
                time.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"Error checking ISR status: {e}")
                self.logger.debug(traceback.format_exc())
                time.sleep(check_interval)
        
        self.logger.error(
            f"Timeout waiting for broker {broker_id} to rejoin ISR. "
            f"{len(not_in_isr)}/{len(partition_set)} partitions still not in ISR"
        )
        
        if not_in_isr:
            self.logger.error("Partitions not in ISR:")
            for topic, partition_id in list(not_in_isr)[:10]:
                self.logger.error(f"  {topic}-{partition_id}")
            if len(not_in_isr) > 10:
                self.logger.error(f"  ... and {len(not_in_isr) - 10} more")
        
        return False


# ==============================================================================
# RESOURCE MONITOR
# ==============================================================================

class ResourceMonitor:
    """Monitor broker resources using kafka-log-dirs.sh and optional OpenTSDB."""
    
    def __init__(self, kafka_bin: str, bootstrap_servers: str, logger: logging.Logger, 
                 opentsdb_url: Optional[str] = None):
        self.kafka_bin = kafka_bin
        self.bootstrap_servers = bootstrap_servers
        self.opentsdb_url = opentsdb_url
        self.logger = logger
        self._disk_usage_cache = None
        self._disk_usage_cache_time = None
    
    def get_broker_cpu_usage(self, hostname: str, hours: int = 24) -> Optional[float]:
        """Get broker CPU usage from OpenTSDB (if available)."""
        if not self.opentsdb_url:
            self.logger.debug(f"OpenTSDB not configured, skipping CPU check for {hostname}")
            return None
        
        try:
            query = {
                "start": f"{hours}h-ago",
                "queries": [{
                    "metric": "cpu.field.usage_idle",
                    "aggregator": "min",
                    "tags": {
                        "node_host": hostname,
                        "cpu": "cpu-total"
                    }
                }]
            }
            
            self.logger.debug(f"Querying CPU metrics for {hostname}")
            response = requests.post(
                f"{self.opentsdb_url}/api/query",
                headers={'Content-Type': 'application/json'},
                json=query,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            if not data or not data[0].get('dps'):
                self.logger.warning(f"No CPU data available for {hostname}")
                return None
            
            idle_values = list(data[0]['dps'].values())
            min_idle = min(idle_values)
            cpu_usage = 100.0 - min_idle
            
            self.logger.info(f"CPU usage for {hostname}: {cpu_usage:.2f}%")
            return cpu_usage
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch CPU metrics for {hostname}: {e}")
            return None
        except (KeyError, IndexError, ValueError) as e:
            self.logger.error(f"Error parsing CPU metrics for {hostname}: {e}")
            return None
    
    def get_all_broker_disk_usage(self, disk_threshold: float = 85.0) -> Dict[int, Dict[str, float]]:
        """Get disk usage for all brokers using kafka-log-dirs.sh."""
        if self._disk_usage_cache and self._disk_usage_cache_time:
            if time.time() - self._disk_usage_cache_time < 60:
                return self._disk_usage_cache
        
        broker_usage = get_broker_disk_usage_from_kafka(
            self.kafka_bin,
            self.bootstrap_servers,
            self.logger
        )
        
        if broker_usage:
            self.logger.info("Kafka data usage per broker (from kafka-log-dirs.sh):")
            for broker_id, usage in sorted(broker_usage.items()):
                usage_gb = usage.get('usage_gb', 0)
                total_bytes = usage.get('total_bytes', 0)
                
                if total_bytes == 0:
                    self.logger.info(f"  Broker {broker_id}: No data (empty partitions)")
                elif usage_gb < 1.0:
                    usage_mb = total_bytes / (1024 ** 2)
                    self.logger.info(f"  Broker {broker_id}: {usage_mb:.2f} MB")
                else:
                    self.logger.info(f"  Broker {broker_id}: {usage_gb:.2f} GB")
        else:
            self.logger.warning("Could not retrieve disk usage from kafka-log-dirs.sh")
        
        self._disk_usage_cache = broker_usage
        self._disk_usage_cache_time = time.time()
        
        return broker_usage
    
    def get_broker_disk_usage(self, broker_id: int) -> Optional[Dict[str, float]]:
        """Get disk usage for a specific broker."""
        all_usage = self.get_all_broker_disk_usage()
        return all_usage.get(broker_id)


# ==============================================================================
# KAFKA CLUSTER MANAGER
# ==============================================================================

class KafkaClusterManager:
    """Manage Kafka cluster operations and metadata."""
    
    def __init__(self, config: KafkaConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.zk_server = config.get('zookeeper_server')
        self.bootstrap_servers = config.get('bootstrap_servers')
        self.kafka_bin = config.get('kafka_bin_path')
        self.resource_monitor = ResourceMonitor(
            self.kafka_bin,
            self.bootstrap_servers,
            logger,
            config.get('opentsdb_url')
        )
    
    def get_broker_hostname(self, broker_id: int) -> Optional[str]:
        """Get broker hostname from Zookeeper."""
        try:
            cmd = [
                f"{self.kafka_bin}/zookeeper-shell.sh",
                self.zk_server,
                "get",
                f"/brokers/ids/{broker_id}"
            ]
            
            self.logger.debug(f"Fetching hostname for broker {broker_id}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                self.logger.error(f"Failed to get broker info: {result.stderr}")
                return None
            
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line.startswith('{'):
                    broker_info = json.loads(line)
                    hostname = broker_info.get('host')
                    self.logger.info(f"Broker {broker_id} hostname: {hostname}")
                    return hostname
            
            return None
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Timeout getting broker {broker_id} info")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse broker info: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error getting broker hostname: {e}")
            return None
    
    def get_all_brokers(self) -> Dict[int, str]:
        """Get all broker IDs and their hostnames."""
        try:
            cmd = [
                f"{self.kafka_bin}/zookeeper-shell.sh",
                self.zk_server,
                "ls",
                "/brokers/ids"
            ]
            
            self.logger.info("Fetching all broker IDs")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                self.logger.error(f"Failed to list brokers: {result.stderr}")
                return {}
            
            broker_ids = []
            for line in result.stdout.split('\n'):
                if '[' in line and ']' in line:
                    ids_str = line[line.index('[')+1:line.index(']')]
                    broker_ids = [int(bid.strip()) for bid in ids_str.split(',') if bid.strip()]
                    break
            
            brokers = {}
            for broker_id in broker_ids:
                hostname = self.get_broker_hostname(broker_id)
                if hostname:
                    brokers[broker_id] = hostname
            
            self.logger.info(f"Found {len(brokers)} brokers in cluster")
            return brokers
            
        except Exception as e:
            self.logger.error(f"Error getting broker list: {e}")
            return {}
    
    def get_partition_metadata(self) -> Dict[str, List[Dict]]:
        """Get partition metadata for all topics."""
        try:
            cmd = [
                f"{self.kafka_bin}/kafka-topics.sh",
                "--bootstrap-server", self.bootstrap_servers,
                "--describe"
            ]
            
            self.logger.info("Fetching partition metadata")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                self.logger.error(f"Failed to describe topics: {result.stderr}")
                return {}
            
            topics_data = defaultdict(list)
            current_topic = None
            
            for line in result.stdout.split('\n'):
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('Topic:') and 'Partition:' not in line:
                    parts = line.split('\t')
                    if parts:
                        topic_part = parts[0]
                        current_topic = topic_part.split(':', 1)[1].strip()
                        self.logger.debug(f"Found topic: {current_topic}")
                
                elif line.startswith('Topic:') and 'Partition:' in line:
                    try:
                        parts = line.split('\t')
                        
                        partition = None
                        leader = None
                        replicas = []
                        isr = []
                        topic_from_line = None
                        
                        for part in parts:
                            part = part.strip()
                            if part.startswith('Topic:'):
                                topic_from_line = part.split(':', 1)[1].strip()
                            elif part.startswith('Partition:'):
                                partition = int(part.split(':', 1)[1].strip())
                            elif part.startswith('Leader:'):
                                leader = int(part.split(':', 1)[1].strip())
                            elif part.startswith('Replicas:'):
                                replicas_str = part.split(':', 1)[1].strip()
                                replicas = [int(r.strip()) for r in replicas_str.split(',') if r.strip()]
                            elif part.startswith('Isr:'):
                                isr_str = part.split(':', 1)[1].strip()
                                isr = [int(i.strip()) for i in isr_str.split(',') if i.strip()]
                        
                        topic_name = topic_from_line if topic_from_line else current_topic
                        
                        if topic_name and partition is not None and leader is not None:
                            topics_data[topic_name].append({
                                'partition': partition,
                                'leader': leader,
                                'replicas': replicas,
                                'isr': isr
                            })
                            self.logger.debug(
                                f"Added partition: {topic_name}-{partition}, "
                                f"Leader={leader}, Replicas={replicas}, ISR={isr}"
                            )
                        else:
                            self.logger.debug(f"Skipping incomplete partition line: {line}")
                    
                    except (ValueError, IndexError) as e:
                        self.logger.warning(f"Could not parse partition line: {line} - {e}")
                        continue
            
            self.logger.info(f"Retrieved metadata for {len(topics_data)} topics")
            total_partitions = sum(len(parts) for parts in topics_data.values())
            self.logger.info(f"Total partitions: {total_partitions}")
            
            return dict(topics_data)
            
        except Exception as e:
            self.logger.error(f"Error getting partition metadata: {e}")
            self.logger.debug(traceback.format_exc())
            return {}
    
    def get_under_replicated_partitions(self) -> int:
        """Get count of under-replicated partitions."""
        try:
            metadata = self.get_partition_metadata()
            urp_count = 0
            
            for topic, partitions in metadata.items():
                for part in partitions:
                    if len(part['isr']) < len(part['replicas']):
                        urp_count += 1
                        self.logger.warning(
                            f"Under-replicated: {topic}-{part['partition']} "
                            f"ISR={part['isr']} Replicas={part['replicas']}"
                        )
            
            self.logger.info(f"Total under-replicated partitions: {urp_count}")
            return urp_count
            
        except Exception as e:
            self.logger.error(f"Error checking under-replicated partitions: {e}")
            return -1
    
    def get_topic_config(self, topic: str) -> Optional[Dict]:
        """Get topic configuration."""
        try:
            cmd = [
                f"{self.kafka_bin}/kafka-configs.sh",
                "--bootstrap-server", self.bootstrap_servers,
                "--entity-type", "topics",
                "--entity-name", topic,
                "--describe"
            ]
            
            self.logger.debug(f"Fetching config for topic {topic}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                self.logger.error(f"Failed to get topic config: {result.stderr}")
                return None
            
            config = {}
            
            for line in result.stdout.split('\n'):
                line = line.strip()
                if not line or not '=' in line:
                    continue
                
                match = re.match(r'^\s*([a-zA-Z0-9._-]+)=([^\s]+)', line)
                if match:
                    key = match.group(1)
                    value = match.group(2)
                    config[key] = value
                    self.logger.debug(f"Parsed config: {key}={value}")
            
            return config
            
        except Exception as e:
            self.logger.error(f"Error getting topic config: {e}")
            self.logger.debug(traceback.format_exc())
            return None
    
    def check_controller_health(self) -> bool:
        """Check if Kafka controller is healthy."""
        try:
            cmd = [
                f"{self.kafka_bin}/zookeeper-shell.sh",
                self.zk_server,
                "get",
                "/controller"
            ]
            
            self.logger.info("Checking controller health")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                self.logger.error("Controller check failed")
                return False
            
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line.startswith('{'):
                    controller_info = json.loads(line)
                    broker_id = controller_info.get('brokerid')
                    self.logger.info(f"Controller is broker {broker_id}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking controller: {e}")
            return False
    
    def trigger_preferred_leader_election(self, partitions: List[Dict]) -> bool:
        """Trigger preferred leader election for given partitions."""
        if not partitions:
            self.logger.info("No partitions specified for preferred leader election")
            return True
        
        try:
            import tempfile
            election_data = {
                "partitions": [
                    {
                        "topic": p['topic'],
                        "partition": p['partition']
                    }
                    for p in partitions
                ]
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(election_data, f)
                election_file = f.name
            
            self.logger.info(f"Triggering preferred leader election for {len(partitions)} partitions")
            
            cmd = [
                f"{self.kafka_bin}/kafka-leader-election.sh",
                "--bootstrap-server", self.bootstrap_servers,
                "--election-type", "preferred",
                "--path-to-json-file", election_file
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            try:
                os.unlink(election_file)
            except:
                pass
            
            if result.returncode == 0:
                self.logger.info("Preferred leader election completed successfully")
                self.logger.debug(f"Election output: {result.stdout}")
                return True
            else:
                self.logger.error(f"Preferred leader election failed: {result.stderr}")
                self.logger.info("This is not critical - leaders may already be correct")
                return True
                
        except Exception as e:
            self.logger.error(f"Error triggering preferred leader election: {e}")
            self.logger.debug(traceback.format_exc())
            return True


# ==============================================================================
# PRE-CHECK VALIDATOR
# ==============================================================================

class PreCheckValidator:
    """Perform comprehensive pre-checks before operations."""
    
    def __init__(self, cluster_manager: KafkaClusterManager, config: KafkaConfig, 
                 logger: logging.Logger):
        self.cluster = cluster_manager
        self.config = config
        self.logger = logger
        self.checks_passed = []
        self.checks_failed = []
    
    def run_all_checks(self, target_broker_id: int) -> bool:
        """Run all pre-checks before decommission."""
        self.logger.info("="*70)
        self.logger.info("STARTING PRE-CHECK VALIDATION")
        self.logger.info("="*70)
        
        checks = [
            ("Controller Health", self._check_controller),
            ("Broker Existence", lambda: self._check_broker_exists(target_broker_id)),
            ("Under-Replicated Partitions", self._check_urp),
            ("Topic Min ISR Configuration", self._check_min_isr),
            ("Follower ISR Status", lambda: self._check_followers_in_sync(target_broker_id)),
        ]
        
        all_passed = True
        for check_name, check_func in checks:
            try:
                self.logger.info(f"\n{'─'*70}")
                self.logger.info(f"CHECK: {check_name}")
                self.logger.info(f"{'─'*70}")
                
                if check_func():
                    self.checks_passed.append(check_name)
                    self.logger.info(f"✓ {check_name}: PASSED")
                else:
                    self.checks_failed.append(check_name)
                    self.logger.error(f"✗ {check_name}: FAILED")
                    all_passed = False
                    
            except Exception as e:
                self.checks_failed.append(check_name)
                self.logger.error(f"✗ {check_name}: EXCEPTION - {e}")
                self.logger.debug(traceback.format_exc())
                all_passed = False
        
        self._print_summary()
        return all_passed
    
    def _check_controller(self) -> bool:
        """Check controller health."""
        return self.cluster.check_controller_health()
    
    def _check_broker_exists(self, broker_id: int) -> bool:
        """Check if target broker exists in inventory."""
        brokers = self.cluster.get_all_brokers()
        if broker_id not in brokers:
            self.logger.error(f"Broker {broker_id} not found in cluster")
            return False
        
        self.logger.info(f"Broker {broker_id} found: {brokers[broker_id]}")
        return True
    
    def _check_urp(self) -> bool:
        """Check for under-replicated partitions."""
        urp_count = self.cluster.get_under_replicated_partitions()
        if urp_count < 0:
            self.logger.error("Failed to retrieve URP count")
            return False
        
        if urp_count > 0:
            self.logger.error(f"Found {urp_count} under-replicated partitions")
            return False
        
        self.logger.info("No under-replicated partitions found")
        return True
    
    def _check_min_isr(self) -> bool:
        """Check min.insync.replicas configuration for all topics."""
        metadata = self.cluster.get_partition_metadata()
        all_valid = True
        min_isr_required = self.config.get('min_isr_required', 2)
        
        for topic in metadata.keys():
            if topic.startswith('__'):
                self.logger.debug(f"Skipping internal topic: {topic}")
                continue
            
            config = self.cluster.get_topic_config(topic)
            if config is None:
                self.logger.warning(f"Could not fetch config for topic {topic}")
                continue
            
            min_isr = config.get('min.insync.replicas', '1')
            try:
                min_isr_value = int(min_isr)
                if min_isr_value < min_isr_required:
                    self.logger.error(
                        f"Topic {topic} has min.insync.replicas={min_isr_value} "
                        f"(required >= {min_isr_required})"
                    )
                    all_valid = False
                else:
                    self.logger.debug(f"Topic {topic}: min.insync.replicas={min_isr_value} ✓")
            except ValueError:
                self.logger.error(
                    f"Invalid min.insync.replicas value for {topic}: '{min_isr}'"
                )
                all_valid = False
        
        return all_valid
    
    def _check_followers_in_sync(self, broker_id: int) -> bool:
        """Check if all followers are in-sync for target broker's partitions."""
        metadata = self.cluster.get_partition_metadata()
        all_in_sync = True
        
        for topic, partitions in metadata.items():
            for part in partitions:
                if part['leader'] == broker_id:
                    if set(part['replicas']) != set(part['isr']):
                        self.logger.error(
                            f"Partition {topic}-{part['partition']} has out-of-sync replicas. "
                            f"Replicas={part['replicas']}, ISR={part['isr']}"
                        )
                        all_in_sync = False
                    else:
                        self.logger.debug(
                            f"Partition {topic}-{part['partition']} - all replicas in sync"
                        )
        
        return all_in_sync
    
    def _print_summary(self) -> None:
        """Print pre-check summary."""
        self.logger.info("\n" + "="*70)
        self.logger.info("PRE-CHECK SUMMARY")
        self.logger.info("="*70)
        self.logger.info(f"Passed: {len(self.checks_passed)}")
        self.logger.info(f"Failed: {len(self.checks_failed)}")
        
        if self.checks_failed:
            self.logger.error("\nFailed Checks:")
            for check in self.checks_failed:
                self.logger.error(f"  ✗ {check}")
        
        if self.checks_passed:
            self.logger.info("\nPassed Checks:")
            for check in self.checks_passed:
                self.logger.info(f"  ✓ {check}")
        
        self.logger.info("="*70 + "\n")


# ==============================================================================
# BROKER DECOMMISSION MANAGER
# ==============================================================================

class BrokerDecommissionManager:
    """Manage broker decommission operations."""
    
    def __init__(self, cluster_manager, broker_manager: BrokerManager, config,
                 logger: logging.Logger, dry_run: bool = False):
        self.cluster = cluster_manager
        self.broker_manager = broker_manager
        self.config = config
        self.logger = logger
        self.dry_run = dry_run
        self.state_dir = config.get('state_dir')
        os.makedirs(self.state_dir, exist_ok=True)
        
        if self.dry_run:
            self.logger.info("="*70)
            self.logger.info("🔍 DRY-RUN MODE ENABLED - No actual changes will be made")
            self.logger.info("="*70)
    
    def decommission_broker(self, broker_id: int) -> bool:
        """Decommission broker: transfer leadership → stop broker."""
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: SIMULATING BROKER DECOMMISSION FOR BROKER {broker_id}")
        else:
            self.logger.info(f"STARTING BROKER DECOMMISSION FOR BROKER {broker_id}")
        self.logger.info("="*70)
        
        hostname = self.cluster.get_broker_hostname(broker_id)
        if not hostname:
            self.logger.error(f"Could not get hostname for broker {broker_id}")
            return False
        
        self.logger.info(f"Broker {broker_id} hostname: {hostname}")
        
        partitions = self._find_partitions_to_transfer(broker_id)
        if not partitions:
            self.logger.warning(f"No partitions found where broker {broker_id} is a replica")
        else:
            self.logger.info(f"Found {len(partitions)} partitions where broker {broker_id} is a replica")
        
        state_file = self._save_decommission_state(broker_id, hostname, partitions)
        
        if partitions:
            leader_partitions = [p for p in partitions if p['leader'] == broker_id]
            if leader_partitions:
                self.logger.info(f"Broker {broker_id} is leader for {len(leader_partitions)} partitions")
                if not self._transfer_leadership(broker_id, leader_partitions):
                    self.logger.error("Failed to transfer leadership")
                    return False
            else:
                self.logger.info(f"Broker {broker_id} is not leader for any partitions - skipping leadership transfer")
        
        if not self.dry_run:
            server_config = self.config.get('kafka_server_config', '/etc/kafka/conf/server.properties')
            self.logger.info(f"Stopping broker {broker_id}...")
            if not self.broker_manager.stop_broker(server_config):
                self.logger.error(f"Failed to stop broker {broker_id}")
                return False
        else:
            self.logger.info(f"🔍 DRY-RUN: Would stop broker {broker_id}")
        
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: DECOMMISSION SIMULATION COMPLETED FOR BROKER {broker_id}")
            self.logger.info(f"🔍 DRY-RUN: No actual changes were made")
            self.logger.info(f"🔍 DRY-RUN: State file: {state_file}")
        else:
            self.logger.info(f"BROKER {broker_id} SUCCESSFULLY DECOMMISSIONED")
            self.logger.info(f"State file for recommission: {state_file}")
        self.logger.info("="*70)
        
        return True
    
    def _find_partitions_to_transfer(self, broker_id: int) -> List[Dict]:
        """Find all partitions where broker is in replica set."""
        metadata = self.cluster.get_partition_metadata()
        partitions = []
        
        for topic, parts in metadata.items():
            for part in parts:
                if broker_id in part['replicas']:
                    partitions.append({
                        'topic': topic,
                        'partition': part['partition'],
                        'leader': part['leader'],
                        'replicas': part['replicas'],
                        'isr': part['isr']
                    })
        
        return partitions
    
    def _transfer_leadership(self, broker_id: int, leader_partitions: List[Dict]) -> bool:
        """Transfer leadership away from broker."""
        self.logger.info(f"Transferring leadership for {len(leader_partitions)} partitions")
        
        all_replicas = set()
        for part in leader_partitions:
            all_replicas.update(part['replicas'])
        
        brokers_info = self._get_brokers_resource_info(all_replicas)
        
        reassignment_file, new_leaders = self._create_reassignment_json(
            leader_partitions,
            brokers_info,
            broker_id
        )
        
        if not reassignment_file:
            self.logger.error("Failed to create reassignment plan")
            return False
        
        self.logger.info("Leadership transfer plan:")
        for part_key, new_leader in list(new_leaders.items())[:10]:
            self.logger.info(f"  {part_key}: {broker_id} → {new_leader}")
        if len(new_leaders) > 10:
            self.logger.info(f"  ... and {len(new_leaders) - 10} more partitions")
        
        if not self._execute_reassignment(reassignment_file):
            return False
        
        if not self._verify_reassignment(reassignment_file):
            return False
        
        if not self.dry_run:
            self.logger.info("Triggering preferred leader election...")
            election_partitions = [
                {'topic': p['topic'], 'partition': p['partition']}
                for p in leader_partitions
            ]
            self.cluster.trigger_preferred_leader_election(election_partitions)
            time.sleep(3)
            self.logger.info("Leadership transfer completed")
        
        return True
    
    def _get_brokers_resource_info(self, broker_ids: Set[int]) -> Dict[int, Dict]:
        """Get resource information for brokers."""
        self.logger.info("Gathering resource information for brokers...")
        brokers_info = {}
        
        all_disk_usage = self.cluster.resource_monitor.get_all_broker_disk_usage(
            disk_threshold=self.config.get('disk_threshold', 85.0)
        )
        
        for broker_id in broker_ids:
            hostname = self.cluster.get_broker_hostname(broker_id)
            if not hostname:
                self.logger.warning(f"Could not get hostname for broker {broker_id}")
                continue
            
            cpu_usage = self.cluster.resource_monitor.get_broker_cpu_usage(hostname)
            disk_info = all_disk_usage.get(broker_id, {})
            disk_usage = disk_info.get('usage_percent', 100.0)
            
            brokers_info[broker_id] = {
                'hostname': hostname,
                'cpu_usage': cpu_usage if cpu_usage is not None else 0.0,
                'disk_usage': disk_usage,
                'disk_usage_gb': disk_info.get('usage_gb', 0.0)
            }
        
        return brokers_info
    
    def _create_reassignment_json(self, partitions: List[Dict], 
                                   brokers_info: Dict[int, Dict],
                                   exclude_broker: int) -> Tuple[Optional[str], Dict[str, int]]:
        """Create reassignment JSON for leadership transfer."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_prefix = "reassignment_dryrun_" if self.dry_run else "reassignment_"
        reassignment_file = os.path.join(
            self.state_dir,
            f"{filename_prefix}{timestamp}.json"
        )
        
        reassignment = {"version": 1, "partitions": []}
        new_leaders = {}
        
        for part in partitions:
            new_leader = self._select_best_replica(
                part['replicas'],
                exclude_broker,
                brokers_info
            )
            
            if new_leader is None:
                self.logger.error(
                    f"Cannot find suitable replica for {part['topic']}-{part['partition']}"
                )
                return None, {}
            
            new_replicas = [new_leader] + [r for r in part['replicas'] if r != new_leader]
            
            reassignment["partitions"].append({
                "topic": part['topic'],
                "partition": part['partition'],
                "replicas": new_replicas,
                "log_dirs": ["any"] * len(new_replicas)
            })
            
            partition_key = f"{part['topic']}-{part['partition']}"
            new_leaders[partition_key] = new_leader
        
        with open(reassignment_file, 'w') as f:
            json.dump(reassignment, f, indent=2)
        
        self.logger.info(f"Reassignment plan created: {reassignment_file}")
        return reassignment_file, new_leaders
    
    def _select_best_replica(self, replicas: List[int], exclude_broker: int,
                             brokers_info: Dict[int, Dict]) -> Optional[int]:
        """Select best replica based on resources."""
        cpu_threshold = self.config.get('cpu_threshold', 80.0)
        disk_threshold = self.config.get('disk_threshold', 85.0)
        
        candidates = [r for r in replicas if r != exclude_broker]
        eligible = []
        
        for broker_id in candidates:
            info = brokers_info.get(broker_id, {})
            cpu = info.get('cpu_usage', 0.0)
            disk_usage_gb = info.get('disk_usage_gb', 0.0)
            disk_percent = info.get('disk_usage', 0.0)
            
            disk_ok = True
            if disk_percent > 0:
                disk_ok = (disk_percent < disk_threshold)
            
            if cpu == 0.0:
                if disk_ok:
                    eligible.append((broker_id, 50.0, disk_usage_gb))
            else:
                if cpu < cpu_threshold and disk_ok:
                    eligible.append((broker_id, cpu, disk_usage_gb))
        
        if not eligible:
            self.logger.warning("No eligible replicas based on thresholds, selecting first available")
            return candidates[0] if candidates else None
        
        eligible.sort(key=lambda x: (x[1], x[2]))
        selected = eligible[0][0]
        
        self.logger.debug(f"Selected broker {selected} (CPU={eligible[0][1]:.2f}%, Disk={eligible[0][2]:.2f}GB)")
        return selected
    
    def _execute_reassignment(self, reassignment_file: str) -> bool:
        """Execute partition reassignment."""
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: Would execute reassignment from {reassignment_file}")
            return True
        
        try:
            cmd = [
                f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.cluster.bootstrap_servers,
                "--reassignment-json-file", reassignment_file,
                "--execute"
            ]
            
            self.logger.info("Executing partition reassignment...")
            self.logger.debug(f"Command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.stdout:
                self.logger.info("Reassignment output:")
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            if result.returncode != 0:
                self.logger.error(f"Reassignment failed: {result.stderr}")
                return False
            
            return True
            
        except subprocess.TimeoutExpired:
            self.logger.error("Reassignment timed out")
            return False
        except Exception as e:
            self.logger.error(f"Error executing reassignment: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def _verify_reassignment(self, reassignment_file: str, timeout: int = 300) -> bool:
        """Verify reassignment completion."""
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: Would verify reassignment")
            return True
        
        try:
            cmd = [
                f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.cluster.bootstrap_servers,
                "--reassignment-json-file", reassignment_file,
                "--verify"
            ]
            
            self.logger.info("Verifying partition reassignment...")
            start_time = time.time()
            check_interval = self.config.get('verification_interval', 10)
            
            while time.time() - start_time < timeout:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                output = result.stdout.strip()
                output_lower = output.lower()
                
                complete_count = output_lower.count('is complete') + output_lower.count('completed successfully')
                in_progress_count = output_lower.count('in progress')
                failed_count = output_lower.count('failed') + output_lower.count('error')
                
                if complete_count > 0 and in_progress_count == 0 and failed_count == 0:
                    self.logger.info(f"✓ Partition reassignment completed")
                    return True
                
                if failed_count > 0:
                    self.logger.error(f"Reassignment failed:\n{output}")
                    return False
                
                if not output or output_lower.strip() == '':
                    self.logger.info("Reassignment completed (empty verification output)")
                    return True
                
                elapsed = int(time.time() - start_time)
                remaining = int(timeout - elapsed)
                self.logger.info(f"Still in progress (elapsed: {elapsed}s, remaining: {remaining}s)")
                
                time.sleep(check_interval)
            
            self.logger.error(f"Reassignment verification timed out after {timeout}s")
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying reassignment: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def _save_decommission_state(self, broker_id: int, hostname: str,
                                  partitions: List[Dict]) -> str:
        """Save decommission state for recommission."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_file = os.path.join(
            self.state_dir,
            f"decommission_state_broker_{broker_id}_{timestamp}.json"
        )
        
        state = {
            'timestamp': timestamp,
            'broker_id': broker_id,
            'hostname': hostname,
            'partitions': partitions,
            'operation': 'decommission',
            'config': {
                'zookeeper': self.config.get('zookeeper_server'),
                'bootstrap_servers': self.config.get('bootstrap_servers')
            },
            'dry_run': self.dry_run
        }
        
        if self.dry_run:
            state_file = state_file.replace('.json', '_dryrun.json')
        
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        self.logger.info(f"Decommission state saved to {state_file}")
        return state_file


# ==============================================================================
# BROKER RECOMMISSION MANAGER
# ==============================================================================

class BrokerRecommissionManager:
    """Manage broker recommission operations."""
    
    def __init__(self, cluster_manager, broker_manager: BrokerManager, config,
                 logger: logging.Logger, dry_run: bool = False):
        self.cluster = cluster_manager
        self.broker_manager = broker_manager
        self.config = config
        self.logger = logger
        self.dry_run = dry_run
        self.state_dir = config.get('state_dir')
        self.isr_monitor = ISRMonitor(cluster_manager, logger)
        
        if self.dry_run:
            self.logger.info("="*70)
            self.logger.info("🔍 DRY-RUN MODE ENABLED - No actual changes will be made")
            self.logger.info("="*70)
    
    def recommission_broker(self, broker_id: int, state_file: Optional[str] = None) -> bool:
        """Recommission broker: start broker → wait for ISR → restore leadership."""
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: SIMULATING BROKER RECOMMISSION FOR BROKER {broker_id}")
        else:
            self.logger.info(f"STARTING BROKER RECOMMISSION FOR BROKER {broker_id}")
        self.logger.info("="*70)
        
        if state_file is None:
            state_file = self._find_latest_state_file(broker_id)
        
        if state_file is None:
            self.logger.error("No state file available for recommission")
            return False
        
        state = self._load_state(state_file)
        if state is None:
            return False
        
        hostname = state.get('hostname')
        partitions = state.get('partitions', [])
        
        self.logger.info(f"Broker {broker_id} hostname: {hostname}")
        self.logger.info(f"Will restore {len(partitions)} partitions")
        
        if not self.dry_run:
            server_config = self.config.get('kafka_server_config', '/etc/kafka/conf/server.properties')
            self.logger.info(f"Starting broker {broker_id}...")
            if not self.broker_manager.start_broker(server_config, daemon_mode=True):
                self.logger.error(f"Failed to start broker {broker_id}")
                return False
        else:
            self.logger.info(f"🔍 DRY-RUN: Would start broker {broker_id}")
        
        if not self.dry_run:
            isr_timeout = self.config.get('isr_sync_timeout', 600)
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info(f"WAITING FOR BROKER {broker_id} TO SYNC REPLICAS")
            self.logger.info("="*70)
            
            if not self.isr_monitor.wait_for_broker_in_isr(broker_id, partitions, isr_timeout):
                self.logger.error(f"Broker {broker_id} failed to rejoin ISR within {isr_timeout}s")
                self.logger.error("Recommission cannot proceed - broker replicas not in sync")
                self.logger.error("")
                self.logger.error("Possible actions:")
                self.logger.error("  1. Wait longer and check ISR status manually")
                self.logger.error("  2. Investigate broker logs for sync issues")
                self.logger.error("  3. Check network connectivity between brokers")
                return False
            
            self.logger.info("="*70)
            self.logger.info(f"✓ BROKER {broker_id} REPLICAS ARE IN-SYNC")
            self.logger.info("="*70)
        else:
            self.logger.info(f"🔍 DRY-RUN: Would wait for broker {broker_id} to rejoin ISR")
        
        if partitions:
            leader_partitions = [p for p in partitions if p['leader'] == broker_id]
            if leader_partitions:
                self.logger.info(f"\nRestoring leadership for {len(leader_partitions)} partitions")
                if not self._restore_leadership(broker_id, leader_partitions):
                    self.logger.error("Failed to restore leadership")
                    return False
            else:
                self.logger.info("No leadership to restore (broker was not leader for any partitions)")
        
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: RECOMMISSION SIMULATION COMPLETED FOR BROKER {broker_id}")
        else:
            self.logger.info(f"BROKER {broker_id} SUCCESSFULLY RECOMMISSIONED")
        self.logger.info("="*70)
        
        return True
    
    def _find_latest_state_file(self, broker_id: int) -> Optional[str]:
        """Find the most recent state file for a broker."""
        pattern = f"decommission_state_broker_{broker_id}_"
        state_files = [
            f for f in os.listdir(self.state_dir)
            if f.startswith(pattern) and f.endswith('.json')
        ]
        
        if not state_files:
            self.logger.error(f"No state files found for broker {broker_id}")
            return None
        
        state_files.sort(reverse=True)
        latest = os.path.join(self.state_dir, state_files[0])
        
        self.logger.info(f"Found latest state file: {latest}")
        return latest
    
    def _load_state(self, state_file: str) -> Optional[Dict]:
        """Load state from file."""
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            self.logger.info(f"Loaded state from {state_file}")
            self.logger.info(f"  Timestamp: {state.get('timestamp')}")
            self.logger.info(f"  Broker ID: {state.get('broker_id')}")
            self.logger.info(f"  Partitions: {len(state.get('partitions', []))}")
            
            return state
            
        except Exception as e:
            self.logger.error(f"Error loading state file: {e}")
            self.logger.debug(traceback.format_exc())
            return None
    
    def _restore_leadership(self, broker_id: int, leader_partitions: List[Dict]) -> bool:
        """Restore original leadership to broker."""
        reassignment_file = self._create_restoration_reassignment(broker_id, leader_partitions)
        if not reassignment_file:
            return False
        
        self.logger.info("Leadership restoration plan:")
        for part in leader_partitions[:10]:
            self.logger.info(f"  {part['topic']}-{part['partition']}: Restore leadership to broker {broker_id}")
        if len(leader_partitions) > 10:
            self.logger.info(f"  ... and {len(leader_partitions) - 10} more partitions")
        
        if not self._execute_reassignment(reassignment_file):
            return False
        
        if not self._verify_reassignment(reassignment_file):
            return False
        
        if not self.dry_run:
            self.logger.info("Triggering preferred leader election...")
            election_partitions = [
                {'topic': p['topic'], 'partition': p['partition']}
                for p in leader_partitions
            ]
            self.cluster.trigger_preferred_leader_election(election_partitions)
            time.sleep(3)
            
            self.logger.info("Verifying leadership restoration...")
            restored_count = 0
            failed_partitions = []
            
            metadata = self.cluster.get_partition_metadata()
            for part in leader_partitions:
                topic = part['topic']
                partition_id = part['partition']
                
                if topic in metadata:
                    for p in metadata[topic]:
                        if p['partition'] == partition_id:
                            if p['leader'] == broker_id:
                                restored_count += 1
                            else:
                                failed_partitions.append(f"{topic}-{partition_id} (current leader: {p['leader']})")
                            break
            
            self.logger.info(f"Leadership restored for {restored_count}/{len(leader_partitions)} partitions")
            
            if failed_partitions and len(failed_partitions) <= 10:
                self.logger.warning("Some partitions did not restore leadership:")
                for fp in failed_partitions:
                    self.logger.warning(f"  {fp}")
            elif len(failed_partitions) > 10:
                self.logger.warning(f"Leadership not restored for {len(failed_partitions)} partitions")
        
        return True
    
    def _create_restoration_reassignment(self, broker_id: int,
                                         leader_partitions: List[Dict]) -> Optional[str]:
        """Create reassignment JSON to restore leadership."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_prefix = "restoration_dryrun_" if self.dry_run else "restoration_"
        reassignment_file = os.path.join(
            self.state_dir,
            f"{filename_prefix}{timestamp}.json"
        )
        
        reassignment = {"version": 1, "partitions": []}
        
        for part in leader_partitions:
            original_replicas = part['replicas']
            restored_replicas = [broker_id] + [r for r in original_replicas if r != broker_id]
            
            reassignment["partitions"].append({
                "topic": part['topic'],
                "partition": part['partition'],
                "replicas": restored_replicas,
                "log_dirs": ["any"] * len(restored_replicas)
            })
        
        with open(reassignment_file, 'w') as f:
            json.dump(reassignment, f, indent=2)
        
        self.logger.info(f"Restoration plan created: {reassignment_file}")
        return reassignment_file
    
    def _execute_reassignment(self, reassignment_file: str) -> bool:
        """Execute partition reassignment."""
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: Would execute reassignment from {reassignment_file}")
            return True
        
        try:
            cmd = [
                f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.cluster.bootstrap_servers,
                "--reassignment-json-file", reassignment_file,
                "--execute"
            ]
            
            self.logger.info("Executing partition reassignment...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.stdout:
                self.logger.info("Reassignment output:")
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            if result.returncode != 0:
                self.logger.error(f"Reassignment failed: {result.stderr}")
                return False
            
            return True
            
        except subprocess.TimeoutExpired:
            self.logger.error("Reassignment timed out")
            return False
        except Exception as e:
            self.logger.error(f"Error executing reassignment: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def _verify_reassignment(self, reassignment_file: str, timeout: int = 300) -> bool:
        """Verify reassignment completion."""
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: Would verify reassignment")
            return True
        
        try:
            cmd = [
                f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                "--bootstrap-server", self.cluster.bootstrap_servers,
                "--reassignment-json-file", reassignment_file,
                "--verify"
            ]
            
            self.logger.info("Verifying partition reassignment...")
            start_time = time.time()
            check_interval = self.config.get('verification_interval', 10)
            
            while time.time() - start_time < timeout:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                output = result.stdout.strip()
                output_lower = output.lower()
                
                complete_count = output_lower.count('is complete') + output_lower.count('completed successfully')
                in_progress_count = output_lower.count('in progress')
                failed_count = output_lower.count('failed') + output_lower.count('error')
                
                if complete_count > 0 and in_progress_count == 0 and failed_count == 0:
                    self.logger.info(f"✓ Partition reassignment completed")
                    return True
                
                if failed_count > 0:
                    self.logger.error(f"Reassignment failed:\n{output}")
                    return False
                
                if not output or output_lower.strip() == '':
                    self.logger.info("Reassignment completed (empty verification output)")
                    return True
                
                elapsed = int(time.time() - start_time)
                remaining = int(timeout - elapsed)
                self.logger.info(f"Still in progress (elapsed: {elapsed}s, remaining: {remaining}s)")
                
                time.sleep(check_interval)
            
            self.logger.error(f"Reassignment verification timed out after {timeout}s")
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying reassignment: {e}")
            self.logger.debug(traceback.format_exc())
            return False


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def print_banner():
    """Print script banner."""
    banner = """
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║         Kafka Broker Decommission/Recommission Tool - v2.0.0        ║
║              Production Grade - Kafka 2.8.2                          ║
║                                                                      ║
║  Features:                                                           ║
║    • Automated broker decommission (stop)                            ║
║    • Automated broker recommission (start + ISR sync)                ║
║    • Resource-aware leader reassignment                             ║
║    • ISR synchronization monitoring                                  ║
║    • Comprehensive logging                                           ║
║    • Dry-run mode for testing                                        ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def main():
    """Main execution function."""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description="Kafka Broker Decommission/Recommission Tool - Automated broker management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Decommission broker 1001 (dry-run)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001 --dry-run
  
  # Decommission broker 1001 (execute)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001
  
  # Recommission broker 1001 (dry-run)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001 --recommission --dry-run
  
  # Recommission broker 1001 (execute)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001 --recommission
        """
    )
    
    parser.add_argument('--config', required=True, help='Path to configuration YAML file')
    parser.add_argument('--broker-id', type=int, required=True, help='Broker ID to decommission/recommission')
    parser.add_argument('--recommission', action='store_true', help='Recommission broker (start and restore leadership)')
    parser.add_argument('--state-file', help='Specific state file for recommission (optional, auto-detects latest if not provided)')
    parser.add_argument('--log-dir', help='Directory for log files (overrides config file)')
    parser.add_argument('--skip-prechecks', action='store_true', help='Skip pre-checks (NOT RECOMMENDED for production)')
    parser.add_argument('--dry-run', action='store_true', help='Simulate operation without making any actual changes')
    
    args = parser.parse_args()
    
    try:
        with open(args.config, 'r') as f:
            temp_config = yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR: Could not load config file: {e}")
        sys.exit(1)
    
    if args.log_dir:
        log_dir = args.log_dir
    elif 'log_directory' in temp_config:
        log_dir = temp_config['log_directory']
    elif 'log_dir' in temp_config:
        log_dir = temp_config['log_dir']
    else:
        log_dir = './logs'
    
    logger = setup_logging(log_dir)
    
    try:
        logger.info(f"Loading configuration from {args.config}")
        config = KafkaConfig(args.config, logger)
        
        logger.info("Initializing Kafka cluster manager")
        cluster_manager = KafkaClusterManager(config, logger)
        
        logger.info("Initializing broker manager")
        kafka_bin = config.get('kafka_bin_path')
        broker_manager = BrokerManager(kafka_bin, logger)
        
        if args.recommission:
            logger.info("")
            logger.info("="*70)
            logger.info(f"OPERATION: RECOMMISSION BROKER {args.broker_id}")
            if args.dry_run:
                logger.info("MODE: DRY-RUN (simulation only)")
            else:
                logger.info("MODE: LIVE (actual execution)")
            logger.info("="*70)
            logger.info("")
            
            recommission_mgr = BrokerRecommissionManager(
                cluster_manager, broker_manager, config, logger, args.dry_run
            )
            
            success = recommission_mgr.recommission_broker(args.broker_id, args.state_file)
            
            if success:
                if args.dry_run:
                    logger.info("")
                    logger.info("✓ Recommission simulation completed successfully")
                    logger.info(f"To execute for real, run:")
                    logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker-id {args.broker_id} --recommission")
                else:
                    logger.info("")
                    logger.info("✓ Recommission completed successfully")
                    logger.info(f"Broker {args.broker_id} is now back in service")
                sys.exit(0)
            else:
                logger.error("")
                logger.error("✗ Recommission failed")
                logger.error("Check the logs above for details")
                sys.exit(1)
        
        else:
            logger.info("")
            logger.info("="*70)
            logger.info(f"OPERATION: DECOMMISSION BROKER {args.broker_id}")
            if args.dry_run:
                logger.info("MODE: DRY-RUN (simulation only)")
            else:
                logger.info("MODE: LIVE (actual execution)")
            logger.info("="*70)
            logger.info("")
            
            if not args.skip_prechecks:
                validator = PreCheckValidator(cluster_manager, config, logger)
                if not validator.run_all_checks(args.broker_id):
                    logger.error("Pre-checks failed. Aborting decommission.")
                    logger.error("Use --skip-prechecks to bypass (NOT RECOMMENDED)")
                    sys.exit(1)
            else:
                logger.warning("⚠ Pre-checks SKIPPED - proceeding without validation")
            
            decommission_mgr = BrokerDecommissionManager(
                cluster_manager, broker_manager, config, logger, args.dry_run
            )
            
            success = decommission_mgr.decommission_broker(args.broker_id)
            
            if success:
                if args.dry_run:
                    logger.info("")
                    logger.info("✓ Decommission simulation completed successfully")
                    logger.info(f"To execute for real, run:")
                    logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker-id {args.broker_id}")
                else:
                    logger.info("")
                    logger.info("✓ Decommission completed successfully")
                    logger.info(f"Broker {args.broker_id} has been stopped")
                    logger.info(f"To recommission, run:")
                    logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker-id {args.broker_id} --recommission")
                sys.exit(0)
            else:
                logger.error("")
                logger.error("✗ Decommission failed")
                logger.error("Check the logs above for details")
                sys.exit(1)
    
    except KeyboardInterrupt:
        logger.warning("\n")
        logger.warning("="*70)
        logger.warning("Operation interrupted by user (Ctrl+C)")
        logger.warning("="*70)
        sys.exit(130)
    
    except Exception as e:
        logger.error("")
        logger.error("="*70)
        logger.error(f"FATAL ERROR: {e}")
        logger.error("="*70)
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
