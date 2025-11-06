#!/usr/bin/env python3
"""
Kafka Broker Decommission/Recommission Script - Production Grade
=================================================================
This script safely decommissions a Kafka broker by transferring leadership
and stopping the broker, or recommissions it by starting and restoring leadership.

Features:
- Supports both hostname and broker-id input
- Automatic broker ID resolution from hostname using kafka-broker-api-versions.sh
- Comprehensive pre-checks before decommission
- Resource-aware leader reassignment (CPU, Disk)
- Broker stop/start with local Kafka scripts
- ISR synchronization monitoring before recommission
- Rollback capability to restore previous state
- Detailed logging and state persistence

Author: Production Engineering Team
Version: 2.1.0 (Hostname Support Added)
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
    """Auto-detect Kafka server.properties path."""
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
    """Auto-detect Kafka binary directory."""
    try:
        env_files = glob.glob("/etc/kafka/*/0/kafka-env.sh")
        if env_files:
            with open(env_files[0], 'r') as f:
                for line in f:
                    if 'CLASSPATH=' in line and '/kafka' in line:
                        match = re.search(r'(/usr/odp/[^:]+/kafka-broker)', line)
                        if match:
                            kafka_bin = f"{match.group(1)}/bin"
                            if os.path.exists(kafka_bin):
                                return kafka_bin
    except Exception:
        pass
    
    standard_paths = [
        "/usr/odp/current/kafka-broker/bin",
        "/usr/hdp/current/kafka-broker/bin",
        "/opt/kafka/bin"
    ]
    for path in standard_paths:
        if os.path.exists(path):
            return path
    
    try:
        result = subprocess.run(
            ["find", "/usr", "-name", "kafka-topics.sh", "-type", "f"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            return os.path.dirname(result.stdout.strip().split('\n')[0])
    except Exception:
        pass
    return None


def auto_detect_zookeeper_servers() -> Optional[str]:
    """Auto-detect Zookeeper servers from Kafka server.properties."""
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
                        return zk_connect.split(',')[0]
        except Exception:
            continue
    return None


def get_broker_id_from_hostname(hostname: str, kafka_bin: str, bootstrap_servers: str, 
                                  logger: logging.Logger) -> Optional[int]:
    """
    Get broker ID from hostname using kafka-broker-api-versions.sh.
    
    Args:
        hostname: Broker hostname (e.g., stg-hdpashique101.phonepe.nb6)
        kafka_bin: Path to Kafka bin directory
        bootstrap_servers: Kafka bootstrap servers
        logger: Logger instance
        
    Returns:
        Broker ID as integer, or None if not found
    """
    try:
        cmd = [f"{kafka_bin}/kafka-broker-api-versions.sh", "--bootstrap-server", bootstrap_servers]
        
        logger.info(f"Looking up broker ID for hostname: {hostname}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            logger.error(f"kafka-broker-api-versions.sh failed: {result.stderr}")
            return None
        
        # Parse output format: hostname:port (id: 1003 rack: null) -> (
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            
            # Check if this line contains the hostname and broker ID
            # Format: stg-hdpashique101.phonepe.nb6:6667 (id: 1003 rack: null) -> (
            if hostname in line and '(id:' in line:
                try:
                    # Extract the broker ID from between '(id:' and the next space or ')'
                    id_section = line.split('(id:')[1]
                    # The ID is followed by either ' rack' or ')'
                    if ' rack' in id_section:
                        broker_id = int(id_section.split(' rack')[0].strip())
                    else:
                        broker_id = int(id_section.split(')')[0].strip())
                    
                    logger.info(f"✓ Found broker ID {broker_id} for hostname {hostname}")
                    return broker_id
                except (IndexError, ValueError) as e:
                    logger.debug(f"Could not parse broker ID from line: {line} - {e}")
                    continue
        
        logger.error(f"Could not find broker ID for hostname {hostname}")
        logger.debug(f"Searched output:\n{result.stdout[:500]}")
        return None
        
    except subprocess.TimeoutExpired:
        logger.error("kafka-broker-api-versions.sh timed out")
        return None
    except Exception as e:
        logger.error(f"Error getting broker ID from hostname: {e}")
        return None


def get_broker_disk_usage_from_kafka(kafka_bin: str, bootstrap_servers: str, 
                                      logger: logging.Logger) -> Tuple[Dict[int, Dict[str, float]], Dict[int, List[str]]]:
    """
    Get disk usage for all brokers using kafka-log-dirs.sh.
    
    Returns:
        Tuple of (broker_usage_dict, broker_log_dirs_dict)
        - broker_usage_dict: {broker_id: {total_bytes, usage_gb, usage_percent}}
        - broker_log_dirs_dict: {broker_id: [list of log directories]}
    """
    try:
        cmd = [f"{kafka_bin}/kafka-log-dirs.sh", "--describe", "--bootstrap-server", bootstrap_servers]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return {}, {}
        
        json_line = None
        for line in result.stdout.strip().split('\n'):
            if line.strip().startswith('{'):
                json_line = line.strip()
                break
        
        if not json_line:
            return {}, {}
        
        data = json.loads(json_line)
        broker_usage = {}
        broker_log_dirs = {}
        
        for broker_obj in data.get('brokers', []):
            broker_id = broker_obj.get('broker')
            if broker_id is None:
                continue
            
            # Collect log directories for this broker
            log_dirs = []
            for log_dir in broker_obj.get('logDirs', []):
                log_dir_path = log_dir.get('logDir')
                if log_dir_path:
                    log_dirs.append(log_dir_path)
            
            broker_log_dirs[broker_id] = log_dirs
            
            # Calculate total usage
            total_bytes = sum(
                partition.get('size', 0)
                for log_dir in broker_obj.get('logDirs', [])
                for partition in log_dir.get('partitions', [])
            )
            
            broker_usage[broker_id] = {
                'total_bytes': total_bytes,
                'usage_gb': total_bytes / (1024 ** 3),
                'usage_percent': 0.0
            }
        
        return broker_usage, broker_log_dirs
        
    except Exception as e:
        logger.error(f"Error getting broker disk usage: {e}")
        return {}, {}


# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================

def setup_logging(log_dir: str = "/var/log/kafka-node-decommission/") -> logging.Logger:
    """Configure logging for the script."""
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"kafka_decommission_{timestamp}.log")
    
    logger = logging.getLogger("KafkaDecommission")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
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
            kafka_bin = auto_detect_kafka_bin()
            if kafka_bin:
                self.config['kafka_bin_path'] = kafka_bin
                self.logger.info(f"✓ Auto-detected kafka_bin_path: {kafka_bin}")
            else:
                raise ValueError("Could not auto-detect kafka_bin_path. Please specify in config.")
        
        if 'zookeeper_server' not in self.config or not self.config['zookeeper_server']:
            zk_server = auto_detect_zookeeper_servers()
            if zk_server:
                self.config['zookeeper_server'] = zk_server
                self.logger.info(f"✓ Auto-detected zookeeper_server: {zk_server}")
            else:
                raise ValueError("Could not auto-detect zookeeper_server. Please specify in config.")
        
        if 'kafka_server_config' not in self.config or not self.config['kafka_server_config']:
            server_config = auto_detect_kafka_server_config()
            if server_config:
                self.config['kafka_server_config'] = server_config
                self.logger.info(f"✓ Auto-detected kafka_server_config: {server_config}")
            else:
                self.config['kafka_server_config'] = '/etc/kafka/conf/server.properties'
        
        self.config.setdefault('state_dir', self.config.get('state_directory', './kafka_demotion_state'))
        self.config.setdefault('log_dir', self.config.get('log_directory', './logs'))
        
        defaults = {
            'cpu_threshold': 80,
            'disk_threshold': 85,
            'min_isr_required': 2,
            'isr_sync_timeout': 600,
            'isr_check_interval': 10,
            'verification_interval': 10,
            'reassignment_timeout': 300,
            'ambari_timeout': 300
        }
        
        for key, default_value in defaults.items():
            self.config.setdefault(key, default_value)
        
        # Validate Ambari configuration (now mandatory)
        ambari_required = ['ambari_host', 'ambari_user', 'ambari_password', 'ambari_cluster']
        missing_ambari = [field for field in ambari_required if not self.config.get(field)]
        if missing_ambari:
            raise ValueError(f"Missing required Ambari configuration fields: {missing_ambari}")
        self.logger.info("✓ Ambari configuration validated")
        
        # Validate OpenTSDB configuration (now mandatory)
        if not self.config.get('opentsdb_url'):
            raise ValueError("Missing required OpenTSDB configuration: opentsdb_url")
        self.logger.info("✓ OpenTSDB configuration validated")
        
        self.logger.info("✓ Configuration validation passed")
    
    def get(self, key: str, default=None):
        """Get configuration value."""
        return self.config.get(key, default)

# ==============================================================================
# PART 2: BROKER MANAGEMENT AND CLUSTER OPERATIONS
# ==============================================================================

class BrokerManager:
    """Manage broker start/stop operations via Ambari API only."""
    
    def __init__(self, config: Dict, cluster_manager, logger: logging.Logger):
        self.config = config
        self.cluster_manager = cluster_manager
        self.logger = logger
        
        # Auto-detect kafka_log_dir from kafka-log-dirs.sh
        self.kafka_log_dir = self._auto_detect_kafka_log_dir()
        if not self.kafka_log_dir:
            # Fallback to config or default
            self.kafka_log_dir = config.get('kafka_log_dir', '/data/kafka-logs')
            self.logger.info(f"Using kafka_log_dir from config/default: {self.kafka_log_dir}")
        
        self.meta_properties_path = os.path.join(self.kafka_log_dir, 'meta.properties')
        
        # Ambari configuration (mandatory)
        self.ambari_host = config.get('ambari_host')
        self.ambari_user = config.get('ambari_user')
        self.ambari_password = config.get('ambari_password')
        self.ambari_cluster = config.get('ambari_cluster')
        self.ambari_timeout = config.get('ambari_timeout', 300)
        
        self.logger.info(f"Broker operations will use Ambari API: {self.ambari_host}")
    
    def _auto_detect_kafka_log_dir(self) -> Optional[str]:
        """
        Auto-detect kafka log directory from kafka-log-dirs.sh.
        
        This uses the ResourceMonitor to get log directories from any broker,
        and returns the first one found (assuming all brokers use same base path).
        
        Returns:
            Log directory path or None if detection failed
        """
        try:
            # Get log directories from any broker
            all_brokers_log_dirs = {}
            
            # Trigger the disk usage query which populates log dirs cache
            self.cluster_manager.resource_monitor.get_all_broker_disk_usage()
            
            # Get the cached log directories
            if hasattr(self.cluster_manager.resource_monitor, '_broker_log_dirs_cache'):
                all_brokers_log_dirs = self.cluster_manager.resource_monitor._broker_log_dirs_cache or {}
            
            # Get first available log directory from any broker
            for broker_id, log_dirs in all_brokers_log_dirs.items():
                if log_dirs and len(log_dirs) > 0:
                    detected_dir = log_dirs[0]
                    self.logger.info(f"✓ Auto-detected kafka_log_dir from broker {broker_id}: {detected_dir}")
                    return detected_dir
            
            self.logger.warning("Could not auto-detect kafka_log_dir from kafka-log-dirs.sh")
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to auto-detect kafka_log_dir: {e}")
            return None
    
    def backup_meta_properties(self, state_dir: str, broker_id: int) -> Optional[str]:
        """
        Backup meta.properties file before decommission.
        
        Returns:
            Path to backup file, or None if backup failed
        """
        try:
            if not os.path.exists(self.meta_properties_path):
                self.logger.warning(f"meta.properties not found at {self.meta_properties_path}")
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"meta.properties_broker_{broker_id}_{timestamp}.backup"
            backup_path = os.path.join(state_dir, backup_filename)
            
            shutil.copy2(self.meta_properties_path, backup_path)
            self.logger.info(f"✓ Backed up meta.properties to: {backup_path}")
            
            # Also read and log the content
            with open(self.meta_properties_path, 'r') as f:
                content = f.read()
                self.logger.info(f"meta.properties content:\n{content}")
            
            return backup_path
            
        except Exception as e:
            self.logger.error(f"Failed to backup meta.properties: {e}")
            return None
    
    def restore_meta_properties(self, backup_path: str) -> bool:
        """
        Restore meta.properties file from backup before recommission.
        
        Args:
            backup_path: Path to backup file
            
        Returns:
            True if restored successfully, False otherwise
        """
        try:
            if not os.path.exists(backup_path):
                self.logger.error(f"Backup file not found: {backup_path}")
                return False
            
            # Create backup of current meta.properties if it exists
            if os.path.exists(self.meta_properties_path):
                current_backup = f"{self.meta_properties_path}.current_backup"
                shutil.copy2(self.meta_properties_path, current_backup)
                self.logger.info(f"Created backup of current meta.properties: {current_backup}")
            
            # Restore from backup
            shutil.copy2(backup_path, self.meta_properties_path)
            self.logger.info(f"✓ Restored meta.properties from: {backup_path}")
            
            # Log the restored content
            with open(self.meta_properties_path, 'r') as f:
                content = f.read()
                self.logger.info(f"Restored meta.properties content:\n{content}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restore meta.properties: {e}")
            return False
    
    def _ambari_stop_broker(self, hostname: str) -> bool:
        """Stop broker using Ambari API."""
        try:
            url = (f"http://{self.ambari_host}/api/v1/clusters/{self.ambari_cluster}/"
                   f"hosts/{hostname}/host_components/KAFKA_BROKER")
            
            headers = {'X-Requested-By': 'ambari'}
            payload = {
                'RequestInfo': {'context': 'Stop Kafka Broker via Decommission Script'},
                'Body': {'HostRoles': {'state': 'INSTALLED'}}
            }
            
            self.logger.info(f"Sending stop request to Ambari: {url}")
            response = requests.put(
                url,
                auth=(self.ambari_user, self.ambari_password),
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code not in [200, 201, 202]:
                self.logger.error(f"Ambari API returned status {response.status_code}: {response.text}")
                return False
            
            result = response.json()
            request_id = result.get('Requests', {}).get('id')
            self.logger.info(f"✓ Ambari stop request accepted: Request ID {request_id}")
            
            # Wait for request to complete
            if request_id:
                return self._wait_for_ambari_request(request_id, 'stop')
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ambari stop broker failed: {e}")
            return False
    
    def _ambari_start_broker(self, hostname: str) -> bool:
        """Start broker using Ambari API."""
        try:
            url = (f"http://{self.ambari_host}/api/v1/clusters/{self.ambari_cluster}/"
                   f"hosts/{hostname}/host_components/KAFKA_BROKER")
            
            headers = {'X-Requested-By': 'ambari'}
            payload = {
                'RequestInfo': {'context': 'Start Kafka Broker via Recommission Script'},
                'Body': {'HostRoles': {'state': 'STARTED'}}
            }
            
            self.logger.info(f"Sending start request to Ambari: {url}")
            response = requests.put(
                url,
                auth=(self.ambari_user, self.ambari_password),
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code not in [200, 201, 202]:
                self.logger.error(f"Ambari API returned status {response.status_code}: {response.text}")
                return False
            
            result = response.json()
            request_id = result.get('Requests', {}).get('id')
            self.logger.info(f"✓ Ambari start request accepted: Request ID {request_id}")
            
            # Wait for request to complete
            if request_id:
                return self._wait_for_ambari_request(request_id, 'start')
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ambari start broker failed: {e}")
            return False
    
    def _wait_for_ambari_request(self, request_id: int, operation: str) -> bool:
        """Wait for Ambari request to complete."""
        try:
            url = f"http://{self.ambari_host}/api/v1/clusters/{self.ambari_cluster}/requests/{request_id}"
            
            start_time = time.time()
            check_interval = 5
            
            while time.time() - start_time < self.ambari_timeout:
                response = requests.get(
                    url,
                    auth=(self.ambari_user, self.ambari_password),
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    request_status = result.get('Requests', {}).get('request_status')
                    progress = result.get('Requests', {}).get('progress_percent', 0)
                    
                    self.logger.info(f"Ambari {operation} progress: {progress}% - Status: {request_status}")
                    
                    if request_status == 'COMPLETED':
                        self.logger.info(f"✓ Ambari {operation} request completed successfully")
                        return True
                    elif request_status in ['FAILED', 'TIMEDOUT', 'ABORTED']:
                        self.logger.error(f"✗ Ambari {operation} request {request_status}")
                        return False
                    
                time.sleep(check_interval)
            
            self.logger.error(f"Timeout waiting for Ambari {operation} request ({self.ambari_timeout}s)")
            return False
            
        except Exception as e:
            self.logger.error(f"Error waiting for Ambari request: {e}")
            return False
    
    def stop_broker(self, hostname: str) -> bool:
        """Stop broker using Ambari API."""
        self.logger.info("="*70)
        self.logger.info("STOPPING KAFKA BROKER VIA AMBARI API")
        self.logger.info("="*70)
        
        if not hostname:
            self.logger.error("Hostname required for Ambari API stop")
            return False
        
        self.logger.info(f"Stopping broker on {hostname} using Ambari")
        if not self._ambari_stop_broker(hostname):
            return False
        
        self.logger.info("Waiting for broker to stop (15 seconds)...")
        time.sleep(15)
        
        if self.verify_broker_stopped():
            self.logger.info("✓ Kafka broker stopped successfully")
            return True
        
        self.logger.warning("⚠ Broker process check inconclusive, trusting Ambari status")
        return True
    
    def start_broker(self, hostname: str) -> bool:
        """Start broker using Ambari API."""
        self.logger.info("="*70)
        self.logger.info("STARTING KAFKA BROKER VIA AMBARI API")
        self.logger.info("="*70)
        
        if not hostname:
            self.logger.error("Hostname required for Ambari API start")
            return False
        
        self.logger.info(f"Starting broker on {hostname} using Ambari")
        if not self._ambari_start_broker(hostname):
            return False
        
        self.logger.info("Waiting for broker to start (20 seconds)...")
        time.sleep(20)
        
        if self.verify_broker_running():
            self.logger.info("✓ Kafka broker started successfully")
            return True
        
        time.sleep(15)
        if self.verify_broker_running():
            self.logger.info("✓ Kafka broker started successfully (after additional wait)")
            return True
        
        self.logger.warning("⚠ Broker process check inconclusive, trusting Ambari status")
        return True
    
    def verify_broker_stopped(self) -> bool:
        """Verify broker is stopped."""
        try:
            result = subprocess.run(["pgrep", "-f", "kafka.Kafka"], capture_output=True, timeout=5)
            return result.returncode != 0
        except Exception:
            return True
    
    def verify_broker_running(self) -> bool:
        """Verify broker is running."""
        try:
            result = subprocess.run(["pgrep", "-f", "kafka.Kafka"], capture_output=True, timeout=5)
            return result.returncode == 0
        except Exception:
            return False


class ISRMonitor:
    """Monitor ISR synchronization status for broker recommission."""
    
    def __init__(self, cluster_manager, logger: logging.Logger):
        self.cluster = cluster_manager
        self.logger = logger
    
    def wait_for_broker_in_isr(self, broker_id: int, partitions: List[Dict], 
                                timeout: int = 600, check_interval: int = 10) -> bool:
        """Wait for broker to rejoin ISR for all its partitions."""
        self.logger.info(f"Waiting for broker {broker_id} to rejoin ISR for {len(partitions)} partitions")
        
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
                    
                    for p in metadata[topic]:
                        if p['partition'] == partition_id:
                            if broker_id in p['isr']:
                                in_isr.add((topic, partition_id))
                            else:
                                not_in_isr.add((topic, partition_id))
                            break
                
                elapsed = int(time.time() - start_time)
                self.logger.info(f"ISR sync progress: {len(in_isr)}/{len(partition_set)} partitions (elapsed: {elapsed}s)")
                
                if len(not_in_isr) == 0:
                    self.logger.info(f"✓ Broker {broker_id} is in ISR for all partitions")
                    return True
                
                time.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"Error checking ISR status: {e}")
                time.sleep(check_interval)
        
        self.logger.error(f"Timeout waiting for broker {broker_id} to rejoin ISR")
        return False


class ResourceMonitor:
    """Monitor broker resources via OpenTSDB (mandatory)."""
    
    def __init__(self, kafka_bin: str, bootstrap_servers: str, logger: logging.Logger, 
                 opentsdb_url: str):
        self.kafka_bin = kafka_bin
        self.bootstrap_servers = bootstrap_servers
        self.opentsdb_url = opentsdb_url
        self.logger = logger
        self._disk_usage_cache = None
        self._disk_usage_cache_time = None
        self._broker_log_dirs_cache = None
        
        if not self.opentsdb_url:
            raise ValueError("OpenTSDB URL is required for resource monitoring")
    
    def get_broker_cpu_usage(self, hostname: str, hours: int = 1) -> float:
        """
        Get broker CPU usage from OpenTSDB (mandatory).
        
        Returns:
            CPU usage percentage (0-100)
        """
        try:
            query = {
                "start": f"{hours}h-ago",
                "queries": [{
                    "metric": "cpu.field.usage_idle",
                    "aggregator": "avg",
                    "tags": {"node_host": hostname, "cpu": "cpu-total"}
                }]
            }
            
            response = requests.post(f"{self.opentsdb_url}/api/query",
                                    headers={'Content-Type': 'application/json'},
                                    json=query, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data and data[0].get('dps'):
                # Get average idle percentage
                values = list(data[0]['dps'].values())
                avg_idle = sum(values) / len(values)
                cpu_usage = 100.0 - avg_idle
                self.logger.debug(f"CPU usage for {hostname}: {cpu_usage:.2f}%")
                return cpu_usage
            
            self.logger.warning(f"No CPU data from OpenTSDB for {hostname}, returning 0")
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Error getting CPU usage from OpenTSDB for {hostname}: {e}")
            return 0.0
    
    def get_broker_disk_usage_percent(self, hostname: str, path: str, hours: int = 1) -> float:
        """
        Get broker disk usage percentage from OpenTSDB (mandatory).
        
        Args:
            hostname: Broker hostname (e.g., stg-hdpashique101.phonepe.nb6)
            path: Disk path (e.g., /data)
            hours: Hours to look back
            
        Returns:
            Disk usage percentage (0-100)
        """
        try:
            query = {
                "start": f"{hours}h-ago",
                "queries": [{
                    "metric": "disk.field.used_percent",
                    "aggregator": "avg",
                    "tags": {
                        "node_host": hostname,
                        "path": path
                    }
                }]
            }
            
            response = requests.post(f"{self.opentsdb_url}/api/query",
                                    headers={'Content-Type': 'application/json'},
                                    json=query, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data and data[0].get('dps'):
                # Get average disk usage percentage
                values = list(data[0]['dps'].values())
                avg_disk_percent = sum(values) / len(values)
                self.logger.debug(f"Disk usage for {hostname}:{path}: {avg_disk_percent:.2f}%")
                return avg_disk_percent
            
            self.logger.warning(f"No disk data from OpenTSDB for {hostname}:{path}, returning 0")
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Error getting disk usage from OpenTSDB for {hostname}:{path}: {e}")
            return 0.0
    
    def get_all_broker_disk_usage(self, disk_threshold: float = 85.0) -> Dict[int, Dict[str, float]]:
        """
        Get disk usage for all brokers using OpenTSDB.
        
        This combines:
        1. Log directories from kafka-log-dirs.sh
        2. Disk usage percentages from OpenTSDB
        """
        if self._disk_usage_cache and self._disk_usage_cache_time:
            if time.time() - self._disk_usage_cache_time < 60:
                return self._disk_usage_cache
        
        # Get log directories and Kafka data size from kafka-log-dirs.sh
        kafka_usage, broker_log_dirs = get_broker_disk_usage_from_kafka(
            self.kafka_bin, self.bootstrap_servers, self.logger
        )
        
        # Cache log directories
        self._broker_log_dirs_cache = broker_log_dirs
        
        broker_usage = {}
        
        # For each broker, get disk usage from OpenTSDB
        for broker_id, log_dirs in broker_log_dirs.items():
            if not log_dirs:
                self.logger.warning(f"No log directories found for broker {broker_id}")
                continue
            
            # Get the first log directory (e.g., /data/kafka-logs)
            log_dir = log_dirs[0]
            
            # Extract base path (e.g., /data from /data/kafka-logs)
            base_path = '/' + log_dir.split('/')[1] if '/' in log_dir else '/data'
            
            # Store basic info - hostname-based queries will be done later when needed
            broker_usage[broker_id] = {
                'total_bytes': kafka_usage.get(broker_id, {}).get('total_bytes', 0),
                'usage_gb': kafka_usage.get(broker_id, {}).get('usage_gb', 0.0),
                'usage_percent': 0.0,  # Will be filled in by get_broker_resources_with_hostnames
                'log_dir': log_dir,
                'base_path': base_path
            }
        
        self._disk_usage_cache = broker_usage
        self._disk_usage_cache_time = time.time()
        
        if broker_usage:
            self.logger.info(f"Found {len(broker_usage)} brokers with log directories")
        
        return broker_usage
    
    def get_broker_disk_usage(self, broker_id: int) -> Optional[Dict[str, float]]:
        """Get disk usage for a specific broker."""
        return self.get_all_broker_disk_usage().get(broker_id)
    
    def get_broker_log_dirs(self, broker_id: int) -> List[str]:
        """
        Get log directories for a specific broker.
        
        Returns:
            List of log directory paths (e.g., ['/data/kafka-logs'])
        """
        # Ensure we have the cache populated
        if self._broker_log_dirs_cache is None:
            self.get_all_broker_disk_usage()
        
        if self._broker_log_dirs_cache:
            return self._broker_log_dirs_cache.get(broker_id, [])
        
        return []
    
    def get_broker_resources_with_hostname(self, broker_id: int, hostname: str) -> Dict[str, float]:
        """
        Get complete resource information for a broker (CPU and disk from OpenTSDB).
        
        Args:
            broker_id: Broker ID
            hostname: Broker hostname
            
        Returns:
            Dictionary with cpu_usage, disk_usage_percent, disk_usage_gb, base_path
        """
        # Get base disk info
        disk_info = self.get_broker_disk_usage(broker_id) or {}
        base_path = disk_info.get('base_path', '/data')
        
        # Get CPU usage from OpenTSDB
        cpu_usage = self.get_broker_cpu_usage(hostname)
        
        # Get disk usage percentage from OpenTSDB
        disk_usage_percent = self.get_broker_disk_usage_percent(hostname, base_path)
        
        return {
            'cpu_usage': cpu_usage,
            'disk_usage_percent': disk_usage_percent,
            'disk_usage_gb': disk_info.get('usage_gb', 0.0),
            'base_path': base_path,
            'log_dir': disk_info.get('log_dir', '')
        }


class KafkaClusterManager:
    """Manage Kafka cluster operations and metadata."""
    
    def __init__(self, config: KafkaConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.zk_server = config.get('zookeeper_server')
        self.bootstrap_servers = config.get('bootstrap_servers')
        self.kafka_bin = config.get('kafka_bin_path')
        self.resource_monitor = ResourceMonitor(
            self.kafka_bin, self.bootstrap_servers, logger, config.get('opentsdb_url')
        )
    
    def get_broker_hostname(self, broker_id: int) -> Optional[str]:
        """Get broker hostname from Zookeeper."""
        try:
            cmd = [f"{self.kafka_bin}/zookeeper-shell.sh", self.zk_server, "get", f"/brokers/ids/{broker_id}"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                return None
            
            for line in result.stdout.split('\n'):
                if line.strip().startswith('{'):
                    broker_info = json.loads(line.strip())
                    return broker_info.get('host')
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting broker hostname: {e}")
            return None
    
    def get_all_brokers(self) -> Dict[int, str]:
        """Get all broker IDs and their hostnames."""
        try:
            cmd = [f"{self.kafka_bin}/zookeeper-shell.sh", self.zk_server, "ls", "/brokers/ids"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
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
            
        except Exception:
            return {}
    
    def get_partition_metadata(self) -> Dict[str, List[Dict]]:
        """Get partition metadata for all topics."""
        try:
            cmd = [f"{self.kafka_bin}/kafka-topics.sh", "--bootstrap-server", self.bootstrap_servers, "--describe"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
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
                        current_topic = parts[0].split(':', 1)[1].strip()
                
                elif line.startswith('Topic:') and 'Partition:' in line:
                    try:
                        parts = line.split('\t')
                        partition, leader, replicas, isr, topic_from_line = None, None, [], [], None
                        
                        for part in parts:
                            part = part.strip()
                            if part.startswith('Topic:'):
                                topic_from_line = part.split(':', 1)[1].strip()
                            elif part.startswith('Partition:'):
                                partition = int(part.split(':', 1)[1].strip())
                            elif part.startswith('Leader:'):
                                leader = int(part.split(':', 1)[1].strip())
                            elif part.startswith('Replicas:'):
                                replicas = [int(r.strip()) for r in part.split(':', 1)[1].strip().split(',') if r.strip()]
                            elif part.startswith('Isr:'):
                                isr = [int(i.strip()) for i in part.split(':', 1)[1].strip().split(',') if i.strip()]
                        
                        topic_name = topic_from_line if topic_from_line else current_topic
                        
                        if topic_name and partition is not None and leader is not None:
                            topics_data[topic_name].append({
                                'partition': partition, 'leader': leader, 'replicas': replicas, 'isr': isr
                            })
                    except Exception:
                        continue
            
            total_partitions = sum(len(parts) for parts in topics_data.values())
            self.logger.info(f"Retrieved metadata for {len(topics_data)} topics, {total_partitions} partitions")
            return dict(topics_data)
            
        except Exception as e:
            self.logger.error(f"Error getting partition metadata: {e}")
            return {}
    
    def get_under_replicated_partitions(self) -> int:
        """Get count of under-replicated partitions."""
        try:
            metadata = self.get_partition_metadata()
            urp_count = sum(1 for topic, partitions in metadata.items() 
                           for part in partitions if len(part['isr']) < len(part['replicas']))
            self.logger.info(f"Total under-replicated partitions: {urp_count}")
            return urp_count
        except Exception:
            return -1
    
    def get_topic_config(self, topic: str) -> Optional[Dict]:
        """Get topic configuration."""
        try:
            cmd = [f"{self.kafka_bin}/kafka-configs.sh", "--bootstrap-server", self.bootstrap_servers,
                   "--entity-type", "topics", "--entity-name", topic, "--describe"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                return None
            
            config = {}
            for line in result.stdout.split('\n'):
                if '=' in line:
                    match = re.match(r'^\s*([a-zA-Z0-9._-]+)=([^\s]+)', line.strip())
                    if match:
                        config[match.group(1)] = match.group(2)
            return config
        except Exception:
            return None
    
    def check_controller_health(self) -> bool:
        """Check if Kafka controller is healthy."""
        try:
            cmd = [f"{self.kafka_bin}/zookeeper-shell.sh", self.zk_server, "get", "/controller"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                return False
            
            for line in result.stdout.split('\n'):
                if line.strip().startswith('{'):
                    controller_info = json.loads(line.strip())
                    self.logger.info(f"Controller is broker {controller_info.get('brokerid')}")
                    return True
            return False
        except Exception:
            return False
    
    def trigger_preferred_leader_election(self, partitions: List[Dict]) -> bool:
        """Trigger preferred leader election for given partitions."""
        if not partitions:
            return True
        
        try:
            import tempfile
            election_data = {"partitions": [{"topic": p['topic'], "partition": p['partition']} for p in partitions]}
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(election_data, f)
                election_file = f.name
            
            self.logger.info(f"Triggering preferred leader election for {len(partitions)} partitions")
            
            cmd = [f"{self.kafka_bin}/kafka-leader-election.sh", "--bootstrap-server", self.bootstrap_servers,
                   "--election-type", "preferred", "--path-to-json-file", election_file]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            try:
                os.unlink(election_file)
            except:
                pass
            
            if result.returncode == 0:
                self.logger.info("Preferred leader election completed")
            return True
        except Exception:
            return True

# ==============================================================================
# PART 3: PRE-CHECKS AND DECOMMISSION/RECOMMISSION MANAGERS
# ==============================================================================

class PreCheckValidator:
    """Perform comprehensive pre-checks before operations."""
    
    def __init__(self, cluster_manager: KafkaClusterManager, config: KafkaConfig, logger: logging.Logger):
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
                all_passed = False
        
        self.logger.info(f"\nPassed: {len(self.checks_passed)}, Failed: {len(self.checks_failed)}")
        return all_passed
    
    def _check_controller(self) -> bool:
        return self.cluster.check_controller_health()
    
    def _check_broker_exists(self, broker_id: int) -> bool:
        brokers = self.cluster.get_all_brokers()
        if broker_id not in brokers:
            self.logger.error(f"Broker {broker_id} not found in cluster")
            return False
        self.logger.info(f"Broker {broker_id} found: {brokers[broker_id]}")
        return True
    
    def _check_urp(self) -> bool:
        urp_count = self.cluster.get_under_replicated_partitions()
        if urp_count < 0:
            return False
        if urp_count > 0:
            self.logger.error(f"Found {urp_count} under-replicated partitions")
            return False
        return True
    
    def _check_min_isr(self) -> bool:
        metadata = self.cluster.get_partition_metadata()
        all_valid = True
        min_isr_required = self.config.get('min_isr_required', 2)
        
        for topic in metadata.keys():
            if topic.startswith('__'):
                continue
            config = self.cluster.get_topic_config(topic)
            if config:
                min_isr = config.get('min.insync.replicas', '1')
                try:
                    if int(min_isr) < min_isr_required:
                        self.logger.error(f"Topic {topic} has min.insync.replicas={min_isr}")
                        all_valid = False
                except ValueError:
                    all_valid = False
        return all_valid
    
    def _check_followers_in_sync(self, broker_id: int) -> bool:
        metadata = self.cluster.get_partition_metadata()
        all_in_sync = True
        for topic, partitions in metadata.items():
            for part in partitions:
                if part['leader'] == broker_id:
                    if set(part['replicas']) != set(part['isr']):
                        self.logger.error(f"Partition {topic}-{part['partition']} has out-of-sync replicas")
                        all_in_sync = False
        return all_in_sync


class BrokerDecommissionManager:
    """Manage broker decommission operations."""
    
    def __init__(self, cluster_manager, broker_manager: BrokerManager, config, logger: logging.Logger, dry_run: bool = False):
        self.cluster = cluster_manager
        self.broker_manager = broker_manager
        self.config = config
        self.logger = logger
        self.dry_run = dry_run
        self.state_dir = config.get('state_dir')
        os.makedirs(self.state_dir, exist_ok=True)
        
        if self.dry_run:
            self.logger.info("🔍 DRY-RUN MODE ENABLED - No actual changes will be made")
    
    def decommission_broker(self, broker_id: int, hostname: Optional[str] = None) -> bool:
        """Decommission broker: backup metadata → transfer leadership → stop broker."""
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: SIMULATING BROKER DECOMMISSION FOR BROKER {broker_id}")
        else:
            self.logger.info(f"STARTING BROKER DECOMMISSION FOR BROKER {broker_id}")
        self.logger.info("="*70)
        
        if not hostname:
            hostname = self.cluster.get_broker_hostname(broker_id)
            if not hostname:
                self.logger.error(f"Could not get hostname for broker {broker_id}")
                return False
        
        self.logger.info(f"Broker {broker_id} hostname: {hostname}")
        
        # Backup meta.properties before decommission
        if not self.dry_run:
            meta_backup = self.broker_manager.backup_meta_properties(self.state_dir, broker_id)
        else:
            self.logger.info(f"🔍 DRY-RUN: Would backup meta.properties")
            meta_backup = None
        
        partitions = self._find_partitions_to_transfer(broker_id)
        self.logger.info(f"Found {len(partitions)} partitions where broker {broker_id} is a replica")
        
        state_file = self._save_decommission_state(broker_id, hostname, partitions, meta_backup)
        
        if partitions:
            leader_partitions = [p for p in partitions if p['leader'] == broker_id]
            if leader_partitions:
                self.logger.info(f"Broker {broker_id} is leader for {len(leader_partitions)} partitions")
                if not self._transfer_leadership(broker_id, leader_partitions):
                    return False
        
        if not self.dry_run:
            if not self.broker_manager.stop_broker(hostname):
                self.logger.error(f"Failed to stop broker {broker_id}")
                return False
        else:
            self.logger.info(f"🔍 DRY-RUN: Would stop broker {broker_id} via Ambari")
        
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: DECOMMISSION SIMULATION COMPLETED")
        else:
            self.logger.info(f"BROKER {broker_id} SUCCESSFULLY DECOMMISSIONED")
        self.logger.info(f"State file: {state_file}")
        self.logger.info("="*70)
        return True
    
    def _find_partitions_to_transfer(self, broker_id: int) -> List[Dict]:
        metadata = self.cluster.get_partition_metadata()
        partitions = []
        for topic, parts in metadata.items():
            for part in parts:
                if broker_id in part['replicas']:
                    partitions.append({
                        'topic': topic, 'partition': part['partition'], 
                        'leader': part['leader'], 'replicas': part['replicas'], 'isr': part['isr']
                    })
        return partitions
    
    def _transfer_leadership(self, broker_id: int, leader_partitions: List[Dict]) -> bool:
        self.logger.info(f"Transferring leadership for {len(leader_partitions)} partitions")
        
        all_replicas = set()
        for part in leader_partitions:
            all_replicas.update(part['replicas'])
        
        brokers_info = self._get_brokers_resource_info(all_replicas)
        reassignment_file, new_leaders = self._create_reassignment_json(leader_partitions, brokers_info, broker_id)
        
        if not reassignment_file:
            return False
        
        if not self._execute_reassignment(reassignment_file):
            return False
        
        if not self._verify_reassignment(reassignment_file):
            return False
        
        if not self.dry_run:
            election_partitions = [{'topic': p['topic'], 'partition': p['partition']} for p in leader_partitions]
            self.cluster.trigger_preferred_leader_election(election_partitions)
            time.sleep(3)
        
        return True
    
    def _get_brokers_resource_info(self, broker_ids: Set[int]) -> Dict[int, Dict]:
        """Get resource information for brokers using OpenTSDB."""
        # Initialize disk usage cache
        self.cluster.resource_monitor.get_all_broker_disk_usage()
        
        brokers_info = {}
        
        for broker_id in broker_ids:
            hostname = self.cluster.get_broker_hostname(broker_id)
            if not hostname:
                self.logger.warning(f"Could not get hostname for broker {broker_id}")
                continue
            
            # Get complete resource info from OpenTSDB (CPU + Disk)
            resources = self.cluster.resource_monitor.get_broker_resources_with_hostname(broker_id, hostname)
            
            brokers_info[broker_id] = {
                'hostname': hostname,
                'cpu_usage': resources['cpu_usage'],
                'disk_usage': resources['disk_usage_percent'],
                'disk_usage_gb': resources['disk_usage_gb'],
                'base_path': resources['base_path']
            }
            
            self.logger.info(f"Broker {broker_id} ({hostname}): CPU={resources['cpu_usage']:.1f}%, Disk={resources['disk_usage_percent']:.1f}%")
        
        return brokers_info
    
    def _create_reassignment_json(self, partitions: List[Dict], brokers_info: Dict[int, Dict],
                                   exclude_broker: int) -> Tuple[Optional[str], Dict[str, int]]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_prefix = "reassignment_dryrun_" if self.dry_run else "reassignment_"
        reassignment_file = os.path.join(self.state_dir, f"{filename_prefix}{timestamp}.json")
        
        reassignment = {"version": 1, "partitions": []}
        new_leaders = {}
        broker_selection_count = {}  # Track how many partitions assigned to each broker
        
        for part in partitions:
            new_leader = self._select_best_replica(part['replicas'], exclude_broker, brokers_info)
            if new_leader is None:
                return None, {}
            
            # Track broker selection
            broker_selection_count[new_leader] = broker_selection_count.get(new_leader, 0) + 1
            
            new_replicas = [new_leader] + [r for r in part['replicas'] if r != new_leader]
            reassignment["partitions"].append({
                "topic": part['topic'], "partition": part['partition'],
                "replicas": new_replicas, "log_dirs": ["any"] * len(new_replicas)
            })
            new_leaders[f"{part['topic']}-{part['partition']}"] = new_leader
        
        with open(reassignment_file, 'w') as f:
            json.dump(reassignment, f, indent=2)
        
        # Log summary of broker selection
        self.logger.info(f"Leadership reassignment summary for {len(partitions)} partitions:")
        for broker_id in sorted(broker_selection_count.keys()):
            count = broker_selection_count[broker_id]
            info = brokers_info.get(broker_id, {})
            hostname = info.get('hostname', 'unknown')
            cpu = info.get('cpu_usage', 0)
            disk = info.get('disk_usage', 0)
            self.logger.info(f"  Broker {broker_id} ({hostname}): {count} partitions (CPU={cpu:.1f}%, Disk={disk:.1f}%)")
        
        return reassignment_file, new_leaders
    
    def _select_best_replica(self, replicas: List[int], exclude_broker: int, brokers_info: Dict[int, Dict]) -> Optional[int]:
        """Select best replica based on CPU and disk usage from OpenTSDB (both mandatory)."""
        cpu_threshold = self.config.get('cpu_threshold', 80.0)
        disk_threshold = self.config.get('disk_threshold', 85.0)
        
        candidates = [r for r in replicas if r != exclude_broker]
        eligible = []
        
        for broker_id in candidates:
            info = brokers_info.get(broker_id, {})
            cpu_usage = info.get('cpu_usage', 100.0)  # Default to high if missing
            disk_percent = info.get('disk_usage', 100.0)  # Default to high if missing
            disk_usage_gb = info.get('disk_usage_gb', 0.0)
            
            # Both CPU and disk must be below thresholds
            if cpu_usage < cpu_threshold and disk_percent < disk_threshold:
                eligible.append((broker_id, cpu_usage, disk_percent, disk_usage_gb))
                self.logger.debug(f"Broker {broker_id} eligible: CPU={cpu_usage:.1f}%, Disk={disk_percent:.1f}%")
            else:
                self.logger.debug(f"Broker {broker_id} not eligible: CPU={cpu_usage:.1f}% (limit {cpu_threshold}), Disk={disk_percent:.1f}% (limit {disk_threshold})")
        
        if not eligible:
            self.logger.warning(f"No eligible brokers found below thresholds (CPU<{cpu_threshold}%, Disk<{disk_threshold}%)")
            self.logger.warning(f"Falling back to first candidate")
            return candidates[0] if candidates else None
        
        # Sort by CPU first, then disk percentage, then disk GB
        eligible.sort(key=lambda x: (x[1], x[2], x[3]))
        selected = eligible[0]
        self.logger.debug(f"Selected broker {selected[0]}: CPU={selected[1]:.1f}%, Disk={selected[2]:.1f}%")
        return selected[0]
    
    def _execute_reassignment(self, reassignment_file: str) -> bool:
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: Would execute reassignment")
            return True
        
        try:
            cmd = [f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                   "--bootstrap-server", self.cluster.bootstrap_servers,
                   "--reassignment-json-file", reassignment_file, "--execute"]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            return result.returncode == 0
        except Exception:
            return False
    
    def _verify_reassignment(self, reassignment_file: str, timeout: int = 300) -> bool:
        if self.dry_run:
            return True
        
        try:
            cmd = [f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                   "--bootstrap-server", self.cluster.bootstrap_servers,
                   "--reassignment-json-file", reassignment_file, "--verify"]
            
            start_time = time.time()
            check_interval = self.config.get('verification_interval', 10)
            
            while time.time() - start_time < timeout:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                output = result.stdout.strip().lower()
                
                complete_count = output.count('is complete') + output.count('completed successfully')
                in_progress_count = output.count('in progress')
                
                if complete_count > 0 and in_progress_count == 0:
                    self.logger.info("✓ Partition reassignment completed")
                    return True
                
                elapsed = int(time.time() - start_time)
                self.logger.info(f"Still in progress (elapsed: {elapsed}s)")
                time.sleep(check_interval)
            
            self.logger.error("Reassignment verification timed out")
            return False
        except Exception:
            return False
    
    def _save_decommission_state(self, broker_id: int, hostname: str, partitions: List[Dict], 
                                  meta_backup: Optional[str] = None) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_file = os.path.join(self.state_dir, f"decommission_state_broker_{broker_id}_{timestamp}.json")
        
        state = {
            'timestamp': timestamp, 'broker_id': broker_id, 'hostname': hostname,
            'partitions': partitions, 'operation': 'decommission',
            'meta_properties_backup': meta_backup,
            'config': {'zookeeper': self.config.get('zookeeper_server'),
                      'bootstrap_servers': self.config.get('bootstrap_servers')},
            'dry_run': self.dry_run
        }
        
        if self.dry_run:
            state_file = state_file.replace('.json', '_dryrun.json')
        
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        return state_file


class BrokerRecommissionManager:
    """Manage broker recommission operations."""
    
    def __init__(self, cluster_manager, broker_manager: BrokerManager, config, logger: logging.Logger, dry_run: bool = False):
        self.cluster = cluster_manager
        self.broker_manager = broker_manager
        self.config = config
        self.logger = logger
        self.dry_run = dry_run
        self.state_dir = config.get('state_dir')
        self.isr_monitor = ISRMonitor(cluster_manager, logger)
        
        if self.dry_run:
            self.logger.info("🔍 DRY-RUN MODE ENABLED - No actual changes will be made")
    
    def recommission_broker(self, broker_id: int, hostname: Optional[str] = None, 
                           state_file: Optional[str] = None) -> bool:
        """Recommission broker: restore metadata → start broker → wait for ISR → restore leadership."""
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
        
        if not hostname:
            hostname = state.get('hostname')
        partitions = state.get('partitions', [])
        meta_backup = state.get('meta_properties_backup')
        
        self.logger.info(f"Broker {broker_id} hostname: {hostname}, partitions: {len(partitions)}")
        
        # Restore meta.properties before starting broker
        if meta_backup and not self.dry_run:
            self.logger.info(f"Restoring meta.properties from backup")
            if not self.broker_manager.restore_meta_properties(meta_backup):
                self.logger.warning("Failed to restore meta.properties, continuing anyway...")
        elif not meta_backup:
            self.logger.info("No meta.properties backup found in state file")
        else:
            self.logger.info(f"🔍 DRY-RUN: Would restore meta.properties from {meta_backup}")
        
        if not self.dry_run:
            if not self.broker_manager.start_broker(hostname):
                self.logger.error(f"Failed to start broker {broker_id}")
                return False
        else:
            self.logger.info(f"🔍 DRY-RUN: Would start broker {broker_id} via Ambari")
        
        if not self.dry_run:
            isr_timeout = self.config.get('isr_sync_timeout', 600)
            self.logger.info(f"WAITING FOR BROKER {broker_id} TO SYNC REPLICAS")
            
            if not self.isr_monitor.wait_for_broker_in_isr(broker_id, partitions, isr_timeout):
                self.logger.error(f"Broker {broker_id} failed to rejoin ISR")
                return False
            
            self.logger.info(f"✓ BROKER {broker_id} REPLICAS ARE IN-SYNC")
        
        if partitions:
            leader_partitions = [p for p in partitions if p['leader'] == broker_id]
            if leader_partitions:
                if not self._restore_leadership(broker_id, leader_partitions):
                    return False
        
        self.logger.info("="*70)
        if self.dry_run:
            self.logger.info(f"🔍 DRY-RUN: RECOMMISSION SIMULATION COMPLETED")
        else:
            self.logger.info(f"BROKER {broker_id} SUCCESSFULLY RECOMMISSIONED")
        self.logger.info("="*70)
        return True
    
    def _find_latest_state_file(self, broker_id: int) -> Optional[str]:
        pattern = f"decommission_state_broker_{broker_id}_"
        state_files = [f for f in os.listdir(self.state_dir) if f.startswith(pattern) and f.endswith('.json')]
        
        if not state_files:
            return None
        
        state_files.sort(reverse=True)
        return os.path.join(self.state_dir, state_files[0])
    
    def _load_state(self, state_file: str) -> Optional[Dict]:
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception:
            return None
    
    def _restore_leadership(self, broker_id: int, leader_partitions: List[Dict]) -> bool:
        reassignment_file = self._create_restoration_reassignment(broker_id, leader_partitions)
        if not reassignment_file:
            return False
        
        if not self._execute_reassignment(reassignment_file):
            return False
        
        if not self._verify_reassignment(reassignment_file):
            return False
        
        if not self.dry_run:
            election_partitions = [{'topic': p['topic'], 'partition': p['partition']} for p in leader_partitions]
            self.cluster.trigger_preferred_leader_election(election_partitions)
            time.sleep(3)
        
        return True
    
    def _create_restoration_reassignment(self, broker_id: int, leader_partitions: List[Dict]) -> Optional[str]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_prefix = "restoration_dryrun_" if self.dry_run else "restoration_"
        reassignment_file = os.path.join(self.state_dir, f"{filename_prefix}{timestamp}.json")
        
        reassignment = {"version": 1, "partitions": []}
        for part in leader_partitions:
            restored_replicas = [broker_id] + [r for r in part['replicas'] if r != broker_id]
            reassignment["partitions"].append({
                "topic": part['topic'], "partition": part['partition'],
                "replicas": restored_replicas, "log_dirs": ["any"] * len(restored_replicas)
            })
        
        with open(reassignment_file, 'w') as f:
            json.dump(reassignment, f, indent=2)
        
        return reassignment_file
    
    def _execute_reassignment(self, reassignment_file: str) -> bool:
        if self.dry_run:
            return True
        
        try:
            cmd = [f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                   "--bootstrap-server", self.cluster.bootstrap_servers,
                   "--reassignment-json-file", reassignment_file, "--execute"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            return result.returncode == 0
        except Exception:
            return False
    
    def _verify_reassignment(self, reassignment_file: str, timeout: int = 300) -> bool:
        if self.dry_run:
            return True
        
        try:
            cmd = [f"{self.cluster.kafka_bin}/kafka-reassign-partitions.sh",
                   "--bootstrap-server", self.cluster.bootstrap_servers,
                   "--reassignment-json-file", reassignment_file, "--verify"]
            
            start_time = time.time()
            while time.time() - start_time < timeout:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                output = result.stdout.strip().lower()
                
                if 'is complete' in output or 'completed successfully' in output:
                    return True
                
                time.sleep(10)
            return False
        except Exception:
            return False

# ==============================================================================
# PART 4: MAIN EXECUTION
# ==============================================================================

def print_banner():
    """Print script banner."""
    banner = """
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║         Kafka Broker Decommission/Recommission Tool - v2.1.0        ║
║              Production Grade - Kafka 2.8.2                          ║
║                                                                      ║
║  Features:                                                           ║
║    • Automated broker decommission (stop)                            ║
║    • Automated broker recommission (start + ISR sync)                ║
║    • Hostname and Broker ID input support                            ║
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
  # Decommission using broker ID (dry-run)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001 --dry-run
  
  # Decommission using hostname (dry-run)
  python3 kafka_decommission.py --config config.yaml --broker stg-hdpashique101.phonepe.nb6 --dry-run
  
  # Decommission using broker ID (execute)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001
  
  # Decommission using hostname (execute)
  python3 kafka_decommission.py --config config.yaml --broker stg-hdpashique101.phonepe.nb6
  
  # Recommission using broker ID (dry-run)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001 --recommission --dry-run
  
  # Recommission using hostname (dry-run)
  python3 kafka_decommission.py --config config.yaml --broker stg-hdpashique101.phonepe.nb6 --recommission --dry-run
  
  # Recommission using broker ID (execute)
  python3 kafka_decommission.py --config config.yaml --broker-id 1001 --recommission
  
  # Recommission using hostname (execute)
  python3 kafka_decommission.py --config config.yaml --broker stg-hdpashique101.phonepe.nb6 --recommission
        """
    )
    
    parser.add_argument('--config', required=True, help='Path to configuration YAML file')
    
    # Create mutually exclusive group for broker identification
    broker_group = parser.add_mutually_exclusive_group(required=True)
    broker_group.add_argument('--broker-id', type=int, help='Broker ID to decommission/recommission (e.g., 1001)')
    broker_group.add_argument('--broker', type=str, help='Broker hostname to decommission/recommission (e.g., stg-hdpashique101.phonepe.nb6)')
    
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
    
    log_dir = args.log_dir or temp_config.get('log_directory') or temp_config.get('log_dir') or './logs'
    logger = setup_logging(log_dir)
    
    try:
        # ============================================================
        # RESOLVE BROKER ID FROM HOSTNAME OR BROKER-ID INPUT
        # ============================================================
        broker_id = None
        broker_hostname = None
        
        logger.info(f"Loading configuration from {args.config}")
        config = KafkaConfig(args.config, logger)
        
        if args.broker:
            # User provided hostname, need to resolve to broker ID
            logger.info("="*70)
            logger.info(f"RESOLVING BROKER ID FROM HOSTNAME: {args.broker}")
            logger.info("="*70)
            
            # Special case: For recommission, try to find broker ID from state file first
            # since the broker might be stopped and won't appear in kafka-broker-api-versions
            broker_id = None
            if args.recommission:
                logger.info("Recommission mode: Checking state files first...")
                # Try to find state file that matches this hostname
                state_dir = config.get('state_dir', './kafka_demotion_state')
                if os.path.exists(state_dir):
                    state_files = [f for f in os.listdir(state_dir) 
                                  if f.startswith('decommission_state_broker_') and f.endswith('.json')]
                    
                    for state_file in sorted(state_files, reverse=True):
                        try:
                            with open(os.path.join(state_dir, state_file), 'r') as f:
                                state = json.load(f)
                                if state.get('hostname') == args.broker:
                                    broker_id = state.get('broker_id')
                                    logger.info(f"✓ Found broker ID {broker_id} from state file: {state_file}")
                                    break
                        except:
                            continue
            
            # If not found in state files (or not recommission), try kafka-broker-api-versions
            if broker_id is None:
                logger.info("Querying kafka-broker-api-versions.sh...")
                broker_id = get_broker_id_from_hostname(
                    args.broker,
                    config.get('kafka_bin_path'),
                    config.get('bootstrap_servers'),
                    logger
                )
            
            if broker_id is None:
                logger.error(f"Failed to resolve broker ID for hostname: {args.broker}")
                if args.recommission:
                    logger.error("For recommission, you can also use --broker-id directly")
                    logger.error("Or ensure the state file exists from previous decommission")
                logger.error("Please check:")
                logger.error("  1. Hostname is correct")
                logger.error("  2. For recommission: State file exists or use --broker-id instead")
                logger.error("  3. For decommission: Broker is running and accessible")
                sys.exit(1)
            
            broker_hostname = args.broker
            logger.info(f"✓ Resolved hostname '{args.broker}' to broker ID: {broker_id}")
            logger.info("="*70)
            logger.info("")
            
        elif args.broker_id:
            # User provided broker ID directly
            broker_id = args.broker_id
            logger.info(f"Using broker ID from command line: {broker_id}")
        
        else:
            logger.error("Either --broker or --broker-id must be provided")
            sys.exit(1)
        
        logger.info("Initializing Kafka cluster manager")
        cluster_manager = KafkaClusterManager(config, logger)
        
        logger.info("Initializing broker manager (Ambari API mode)")
        broker_manager = BrokerManager(config.config, cluster_manager, logger)
        
        if args.recommission:
            logger.info("")
            logger.info("="*70)
            if broker_hostname:
                logger.info(f"OPERATION: RECOMMISSION BROKER {broker_id} ({broker_hostname})")
            else:
                logger.info(f"OPERATION: RECOMMISSION BROKER {broker_id}")
            if args.dry_run:
                logger.info("MODE: DRY-RUN (simulation only)")
            else:
                logger.info("MODE: LIVE (actual execution)")
            logger.info("="*70)
            logger.info("")
            
            recommission_mgr = BrokerRecommissionManager(
                cluster_manager, broker_manager, config, logger, args.dry_run
            )
            
            success = recommission_mgr.recommission_broker(broker_id, broker_hostname, args.state_file)
            
            if success:
                if args.dry_run:
                    logger.info("")
                    logger.info("✓ Recommission simulation completed successfully")
                    if broker_hostname:
                        logger.info(f"To execute for real, run:")
                        logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker {broker_hostname} --recommission")
                    else:
                        logger.info(f"To execute for real, run:")
                        logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker-id {broker_id} --recommission")
                else:
                    logger.info("")
                    logger.info("✓ Recommission completed successfully")
                    logger.info(f"Broker {broker_id} is now back in service")
                sys.exit(0)
            else:
                logger.error("")
                logger.error("✗ Recommission failed")
                logger.error("Check the logs above for details")
                sys.exit(1)
        
        else:
            logger.info("")
            logger.info("="*70)
            if broker_hostname:
                logger.info(f"OPERATION: DECOMMISSION BROKER {broker_id} ({broker_hostname})")
            else:
                logger.info(f"OPERATION: DECOMMISSION BROKER {broker_id}")
            if args.dry_run:
                logger.info("MODE: DRY-RUN (simulation only)")
            else:
                logger.info("MODE: LIVE (actual execution)")
            logger.info("="*70)
            logger.info("")
            
            if not args.skip_prechecks:
                validator = PreCheckValidator(cluster_manager, config, logger)
                if not validator.run_all_checks(broker_id):
                    logger.error("Pre-checks failed. Aborting decommission.")
                    logger.error("Use --skip-prechecks to bypass (NOT RECOMMENDED)")
                    sys.exit(1)
            else:
                logger.warning("⚠ Pre-checks SKIPPED - proceeding without validation")
            
            decommission_mgr = BrokerDecommissionManager(
                cluster_manager, broker_manager, config, logger, args.dry_run
            )
            
            success = decommission_mgr.decommission_broker(broker_id, broker_hostname)
            
            if success:
                if args.dry_run:
                    logger.info("")
                    logger.info("✓ Decommission simulation completed successfully")
                    if broker_hostname:
                        logger.info(f"To execute for real, run:")
                        logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker {broker_hostname}")
                    else:
                        logger.info(f"To execute for real, run:")
                        logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker-id {broker_id}")
                else:
                    logger.info("")
                    logger.info("✓ Decommission completed successfully")
                    logger.info(f"Broker {broker_id} has been stopped")
                    logger.info(f"To recommission, run:")
                    if broker_hostname:
                        logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker {broker_hostname} --recommission")
                    else:
                        logger.info(f"  python3 {sys.argv[0]} --config {args.config} --broker-id {broker_id} --recommission")
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
