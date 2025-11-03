#!/usr/bin/env python3
"""
Kafka Topic Alteration Script with Pre-checks

Production-grade script for safely altering Kafka topics with comprehensive
validation and automated execution.

Usage: python kafka_topic_alter.py --config config.yaml [--dry-run]
"""

import argparse
import logging
import os
import re
import subprocess
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yaml


# ==============================================================================
# LOGGING
# ==============================================================================

def setup_logging() -> logging.Logger:
    """Setup logging with file and console handlers."""
    log_dir = Path("/var/log/kafka-topic-alter/")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"kafka_alter_{timestamp}.log"
    
    logger = logging.getLogger("KafkaAlter")
    logger.setLevel(logging.DEBUG)
    
    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Log file: {log_file}")
    
    return logger


# ==============================================================================
# CONFIGURATION
# ==============================================================================

class Config:
    """Configuration loader and validator."""
    
    TIME_UNITS = {'ms': 1, 's': 1000, 'm': 60000, 'h': 3600000, 'd': 86400000}
    
    def __init__(self, config_path: str, logger: logging.Logger):
        self.logger = logger
        self.data = self._load_and_validate(config_path)
    
    def _load_and_validate(self, path: str) -> Dict:
        """Load and validate YAML configuration."""
        try:
            with open(path) as f:
                config = yaml.safe_load(f)
            
            self._validate_structure(config)
            self.logger.info("✓ Configuration loaded and validated")
            return config
            
        except FileNotFoundError:
            self.logger.error(f"Config file not found: {path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            self.logger.error(f"YAML error: {e}")
            sys.exit(1)
    
    def _validate_structure(self, config: Dict):
        """Validate configuration structure."""
        errors = []
        
        # Required fields
        if 'topic' not in config:
            errors.append("Missing 'topic' section")
        else:
            if 'name' not in config['topic']:
                errors.append("Missing topic.name")
            else:
                # Validate topic name format (single or comma-separated)
                topic_name = config['topic']['name']
                if not isinstance(topic_name, (str, list)):
                    errors.append("topic.name must be a string or list")
            
            if 'bootstrap_servers' not in config['topic']:
                errors.append("Missing topic.bootstrap_servers")
            elif not isinstance(config['topic']['bootstrap_servers'], list):
                errors.append("bootstrap_servers must be a list")
        
        if 'alteration' not in config:
            errors.append("Missing 'alteration' section")
        elif not any(k in config['alteration'] for k in ['partitions', 'retention', 'other_configs']):
            errors.append("No alterations specified")
        
        if errors:
            for error in errors:
                self.logger.error(f"  - {error}")
            sys.exit(1)
    
    def parse_time(self, value: str) -> int:
        """Parse time value to milliseconds."""
        if isinstance(value, int):
            return value
        
        match = re.match(r'^(\d+)([a-z]+)$', str(value).lower())
        if not match:
            raise ValueError(f"Invalid time format: {value}")
        
        number, unit = match.groups()
        if unit not in self.TIME_UNITS:
            raise ValueError(f"Unknown time unit: {unit}")
        
        return int(number) * self.TIME_UNITS[unit]
    
    @property
    def topic_name(self) -> str:
        return self.data['topic']['name']
    
    @property
    def topic_names(self) -> List[str]:
        """
        Get list of topic names to process.
        Supports: single string, comma-separated string, or list.
        """
        topic_name = self.data['topic']['name']
        
        # If already a list, return it
        if isinstance(topic_name, list):
            return [name.strip() for name in topic_name]
        
        # If string with commas, split it
        if isinstance(topic_name, str):
            if ',' in topic_name:
                return [name.strip() for name in topic_name.split(',') if name.strip()]
            else:
                return [topic_name.strip()]
        
        return [str(topic_name)]
    
    @property
    def bootstrap_servers(self) -> List[str]:
        return self.data['topic']['bootstrap_servers']
    
    @property
    def kafka_home(self) -> Optional[str]:
        return self.data['topic'].get('kafka_home')
    
    @property
    def alterations(self) -> Dict:
        return self.data.get('alteration', {})


# ==============================================================================
# KAFKA CLI
# ==============================================================================

class KafkaCLI:
    """Wrapper for Kafka CLI tools."""
    
    def __init__(self, bootstrap_servers: List[str], logger: logging.Logger, kafka_home: Optional[str] = None):
        self.bootstrap = ','.join(bootstrap_servers)
        self.logger = logger
        self.bin_path = self._find_kafka_bin(kafka_home)
        self.logger.info(f"Using Kafka tools: {self.bin_path or 'from PATH'}")
    
    def _find_kafka_bin(self, kafka_home: Optional[str]) -> str:
        """Find Kafka bin directory."""
        
        # Strategy 1: Check kafka-env.sh files
        try:
            kafka_env_paths = [
                '/etc/kafka/*/kafka-env.sh',
                '/etc/kafka/*/*/kafka-env.sh'
            ]
            
            import glob
            for pattern in kafka_env_paths:
                for env_file in glob.glob(pattern):
                    self.logger.debug(f"Checking {env_file}")
                    try:
                        with open(env_file, 'r') as f:
                            content = f.read()
                            # Look for CLASSPATH with kafka-broker
                            match = re.search(r'CLASSPATH.*?(/usr/odp/[^/]+/kafka-broker)', content)
                            if match:
                                kafka_base = match.group(1)
                                bin_path = os.path.join(kafka_base, 'bin')
                                if os.path.exists(os.path.join(bin_path, 'kafka-topics.sh')):
                                    self.logger.info(f"Found Kafka from kafka-env.sh: {bin_path}")
                                    return bin_path
                    except Exception as e:
                        self.logger.debug(f"Could not parse {env_file}: {e}")
        except Exception as e:
            self.logger.debug(f"kafka-env.sh check failed: {e}")
        
        # Strategy 2: Check /usr/odp/current/kafka-broker
        current_link = '/usr/odp/current/kafka-broker/bin'
        if os.path.exists(os.path.join(current_link, 'kafka-topics.sh')):
            self.logger.info(f"Found Kafka at: {current_link}")
            return current_link
        
        # Strategy 3: Try 'which' command
        try:
            result = subprocess.run(['which', 'kafka-topics.sh'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                path = result.stdout.strip()
                bin_dir = str(Path(path).parent)
                self.logger.info(f"Found Kafka via 'which': {bin_dir}")
                return bin_dir
        except Exception as e:
            self.logger.debug(f"'which' command failed: {e}")
        
        # Strategy 4: Search /usr/ for kafka-topics.sh
        self.logger.info("Searching for kafka-topics.sh in /usr/ ...")
        try:
            result = subprocess.run(
                ['find', '/usr/', '-name', 'kafka-topics.sh', '-type', 'f'],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0 and result.stdout.strip():
                paths = result.stdout.strip().split('\n')
                if paths:
                    # Prefer paths with 'current' or version numbers
                    for path in paths:
                        if 'current' in path or re.search(r'\d+\.\d+', path):
                            bin_dir = str(Path(path).parent)
                            self.logger.info(f"Found Kafka via find: {bin_dir}")
                            return bin_dir
                    # Use first found
                    bin_dir = str(Path(paths[0]).parent)
                    self.logger.info(f"Found Kafka via find: {bin_dir}")
                    return bin_dir
        except Exception as e:
            self.logger.debug(f"find command failed: {e}")
        
        # Strategy 5: Check common paths
        common_paths = [
            kafka_home and Path(kafka_home) / 'bin',
            Path(os.getenv('KAFKA_HOME', '/opt/kafka')) / 'bin',
            Path('/usr/local/kafka/bin'),
            Path('/opt/kafka/bin'),
        ]
        
        for path in common_paths:
            if path and (path / 'kafka-topics.sh').exists():
                self.logger.info(f"Found Kafka at: {path}")
                return str(path)
        
        # Strategy 6: Fall back to PATH
        self.logger.warning("Kafka tools not found in standard locations, using PATH")
        return ''
    
    def _tool(self, name: str) -> str:
        """Get full path to tool."""
        return str(Path(self.bin_path) / name) if self.bin_path else name
    
    def _run(self, cmd: List[str], check: bool = True) -> Tuple[int, str, str]:
        """Execute command."""
        self.logger.debug(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
        
        return result.returncode, result.stdout, result.stderr
    
    def test_connection(self) -> bool:
        """Test broker connectivity."""
        self.logger.info("Testing broker connectivity...")
        try:
            self._run([self._tool('kafka-broker-api-versions.sh'), 
                      '--bootstrap-server', self.bootstrap])
            self.logger.info("✓ Connected to brokers")
            return True
        except Exception as e:
            self.logger.error(f"✗ Connection failed: {e}")
            return False
    
    def get_topic_info(self, topic: str) -> Optional[Dict]:
        """Get topic information."""
        try:
            _, output, stderr = self._run([
                self._tool('kafka-topics.sh'),
                '--bootstrap-server', self.bootstrap,
                '--describe', '--topic', topic
            ], check=False)
            
            if 'does not exist' in stderr or 'UnknownTopicOrPartitionException' in stderr:
                return None
            
            info = {'name': topic, 'partitions': [], 'partition_count': 0, 'replication_factor': 0}
            
            for line in output.split('\n'):
                line = line.strip()
                
                # Parse first line with topic summary
                if line.startswith('Topic:') and 'PartitionCount:' in line:
                    partition_match = re.search(r'PartitionCount:\s*(\d+)', line)
                    replication_match = re.search(r'ReplicationFactor:\s*(\d+)', line)
                    
                    if partition_match:
                        info['partition_count'] = int(partition_match.group(1))
                    if replication_match:
                        info['replication_factor'] = int(replication_match.group(1))
                
                # Parse partition lines (format: Topic: name Partition: N Leader: X Replicas: [...] Isr: [...])
                elif line.startswith('Topic:') and 'Partition:' in line and 'Isr:' in line:
                    # Extract ISR and Replicas using more flexible patterns
                    isr_match = re.search(r'Isr:\s*([\d,]+)', line)
                    replicas_match = re.search(r'Replicas:\s*([\d,]+)', line)
                    
                    if isr_match and replicas_match:
                        # Convert comma-separated string to list of integers
                        isr = [int(x.strip()) for x in isr_match.group(1).split(',') if x.strip()]
                        replicas = [int(x.strip()) for x in replicas_match.group(1).split(',') if x.strip()]
                        info['partitions'].append({'isr': isr, 'replicas': replicas})
            
            return info
        except Exception as e:
            self.logger.error(f"Failed to get topic info: {e}")
            raise
    
    def get_configs(self, topic: str) -> Dict[str, str]:
        """Get topic configurations."""
        try:
            _, output, _ = self._run([
                self._tool('kafka-configs.sh'),
                '--bootstrap-server', self.bootstrap,
                '--describe', '--entity-type', 'topics',
                '--entity-name', topic
            ])
            
            configs = {}
            for line in output.split('\n'):
                match = re.search(r'(\S+)=(\S+)', line)
                if match and 'sensitive' not in line.lower():
                    configs[match.group(1)] = match.group(2)
            
            return configs
        except Exception as e:
            self.logger.error(f"Failed to get configs: {e}")
            raise
    
    def save_topic_snapshot(self, topic: str, snapshot_dir: str = "/home/sre/snapshot") -> bool:
        """
        Save current topic configuration to snapshot directory.
        
        Args:
            topic: Topic name
            snapshot_dir: Directory to save snapshots
            
        Returns:
            True if snapshot saved successfully
        """
        try:
            # Create snapshot directory if it doesn't exist
            os.makedirs(snapshot_dir, exist_ok=True)
            
            # Get timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Get topic details
            topic_info = self.get_topic_info(topic)
            if not topic_info:
                self.logger.error(f"Cannot snapshot non-existent topic: {topic}")
                return False
            
            # Get topic configs
            topic_configs = self.get_configs(topic)
            
            # Create snapshot data
            snapshot = {
                'topic': topic,
                'timestamp': timestamp,
                'snapshot_time': datetime.now().isoformat(),
                'partition_count': topic_info.get('partition_count'),
                'replication_factor': topic_info.get('replication_factor'),
                'configs': topic_configs,
                'partition_details': topic_info.get('partitions', [])
            }
            
            # Save to file
            snapshot_file = os.path.join(
                snapshot_dir, 
                f"{topic}_{timestamp}.json"
            )
            
            with open(snapshot_file, 'w') as f:
                json.dump(snapshot, f, indent=2)
            
            self.logger.info(f"✓ Snapshot saved: {snapshot_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save snapshot: {e}")
            return False
    
    def get_disk_usage(self) -> Tuple[float, Dict]:
        """
        Check disk space using kafka-log-dirs.sh.
        Parses JSON output and calculates disk usage per broker.
        Returns highest disk usage across all brokers.
        
        Returns:
            Tuple of (max_usage_percentage, disk_info_dict)
        """
        self.logger.info("Checking disk usage via kafka-log-dirs.sh...")
        
        try:
            _, output, _ = self._run([
                self._tool('kafka-log-dirs.sh'),
                '--bootstrap-server', self.bootstrap,
                '--describe'
            ])
            
            # kafka-log-dirs.sh may output warnings/info before JSON
            # Find the first '{' character to start parsing JSON
            json_start = output.find('{')
            if json_start == -1:
                self.logger.warning("No JSON found in kafka-log-dirs.sh output")
                self.logger.debug(f"Output: {output[:500]}")
                return 0.0, {}
            
            # Extract only the JSON part
            json_output = output[json_start:]
            
            # Parse JSON output
            data = json.loads(json_output)
            
            max_usage = 0.0
            disk_info = {}
            broker_stats = []
            
            # Iterate through brokers
            for broker_data in data.get('brokers', []):
                broker_id = broker_data.get('broker')
                log_dirs = broker_data.get('logDirs', [])
                
                for log_dir_data in log_dirs:
                    log_dir = log_dir_data.get('logDir')
                    
                    # Check for errors
                    if log_dir_data.get('error'):
                        self.logger.warning(f"⚠ Error on broker {broker_id} log dir {log_dir}: {log_dir_data.get('error')}")
                        continue
                    
                    # Calculate total size used by summing all partition sizes
                    partitions = log_dir_data.get('partitions', [])
                    total_used_bytes = sum(p.get('size', 0) for p in partitions)
                    
                    # Get total and usable bytes (if available in newer Kafka versions)
                    total_bytes = log_dir_data.get('totalBytes')
                    usable_bytes = log_dir_data.get('usableBytes')
                    
                    # Calculate usage percentage
                    if total_bytes and total_bytes > 0:
                        # If totalBytes is available (Kafka 2.6+)
                        used_bytes = total_bytes - (usable_bytes or 0)
                        used_pct = (used_bytes / total_bytes) * 100
                        free_gb = (usable_bytes or 0) / (1024**3)
                        total_gb = total_bytes / (1024**3)
                        kafka_data_gb = total_used_bytes / (1024**3)
                    else:
                        # Fallback: Only show Kafka data size
                        # We can't calculate percentage without totalBytes
                        kafka_data_gb = total_used_bytes / (1024**3)
                        
                        # Try to get disk stats using statvfs if we're on local broker
                        try:
                            if os.path.exists(log_dir):
                                stat = os.statvfs(log_dir)
                                free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
                                total_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)
                                used_bytes = total_gb * (1024**3) - free_gb * (1024**3)
                                used_pct = (used_bytes / (total_gb * (1024**3))) * 100 if total_gb > 0 else 0
                            else:
                                # Can't determine disk usage
                                self.logger.debug(f"Cannot access {log_dir} locally, skipping disk usage calculation")
                                free_gb = None
                                total_gb = None
                                used_pct = 0
                        except Exception as e:
                            self.logger.debug(f"statvfs failed for {log_dir}: {e}")
                            free_gb = None
                            total_gb = None
                            used_pct = 0
                    
                    broker_stat = {
                        'broker': broker_id,
                        'path': log_dir,
                        'kafka_data_gb': kafka_data_gb,
                        'usage_pct': used_pct if used_pct else 0,
                        'free_gb': free_gb,
                        'total_gb': total_gb
                    }
                    broker_stats.append(broker_stat)
                    
                    # Log broker disk info
                    if free_gb is not None and total_gb is not None:
                        self.logger.info(
                            f"Broker {broker_id}: {log_dir} - "
                            f"Kafka data: {kafka_data_gb:.2f}GB | "
                            f"Disk: {free_gb:.1f}GB free / {total_gb:.1f}GB total ({used_pct:.1f}% used)"
                        )
                    else:
                        self.logger.info(
                            f"Broker {broker_id}: {log_dir} - "
                            f"Kafka data: {kafka_data_gb:.2f}GB"
                        )
                    
                    # Track max usage
                    if used_pct > max_usage:
                        max_usage = used_pct
                        disk_info = broker_stat
                    
                    # Warnings based on disk usage percentage
                    if used_pct > 0:
                        if used_pct >= 85:
                            self.logger.warning(
                                f"⚠ HIGH DISK USAGE: {used_pct:.1f}% on broker {broker_id} at {log_dir}"
                            )
                        if free_gb is not None and free_gb < 10:
                            self.logger.warning(
                                f"⚠ LOW DISK SPACE: {free_gb:.1f}GB free on broker {broker_id} at {log_dir}"
                            )
            
            if not broker_stats:
                self.logger.warning("Could not determine disk usage from kafka-log-dirs.sh")
                return 0.0, {}
            
            if max_usage > 0:
                self.logger.info(f"Highest disk usage: {max_usage:.1f}% on broker {disk_info.get('broker')}")
            else:
                self.logger.info("Disk usage data collected for all brokers")
            
            return max_usage, disk_info
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse kafka-log-dirs.sh JSON output: {e}")
            self.logger.debug(f"Output was: {output[:500] if 'output' in locals() else 'N/A'}")
            return 0.0, {}
        except Exception as e:
            self.logger.error(f"Failed to check disk usage: {e}")
            return 0.0, {}
            
            max_usage = 0.0
            disk_info = {}
            broker_stats = []
            
            # Iterate through brokers
            for broker_data in data.get('brokers', []):
                broker_id = broker_data.get('broker')
                log_dirs = broker_data.get('logDirs', [])
                
                for log_dir_data in log_dirs:
                    log_dir = log_dir_data.get('logDir')
                    
                    # Check for errors
                    if log_dir_data.get('error'):
                        self.logger.warning(f"⚠ Error on broker {broker_id} log dir {log_dir}: {log_dir_data.get('error')}")
                        continue
                    
                    # Calculate total size used by summing all partition sizes
                    partitions = log_dir_data.get('partitions', [])
                    total_used_bytes = sum(p.get('size', 0) for p in partitions)
                    
                    # Get total and usable bytes (if available in newer Kafka versions)
                    total_bytes = log_dir_data.get('totalBytes')
                    usable_bytes = log_dir_data.get('usableBytes')
                    
                    # Calculate usage percentage
                    if total_bytes and total_bytes > 0:
                        # If totalBytes is available (Kafka 2.6+)
                        used_bytes = total_bytes - (usable_bytes or 0)
                        used_pct = (used_bytes / total_bytes) * 100
                        free_gb = (usable_bytes or 0) / (1024**3)
                        total_gb = total_bytes / (1024**3)
                        kafka_data_gb = total_used_bytes / (1024**3)
                    else:
                        # Fallback: Only show Kafka data size
                        # We can't calculate percentage without totalBytes
                        kafka_data_gb = total_used_bytes / (1024**3)
                        
                        # Try to get disk stats using statvfs if we're on local broker
                        try:
                            if os.path.exists(log_dir):
                                stat = os.statvfs(log_dir)
                                free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
                                total_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)
                                used_bytes = total_gb * (1024**3) - free_gb * (1024**3)
                                used_pct = (used_bytes / (total_gb * (1024**3))) * 100 if total_gb > 0 else 0
                            else:
                                # Can't determine disk usage
                                self.logger.debug(f"Cannot access {log_dir} locally, skipping disk usage calculation")
                                free_gb = None
                                total_gb = None
                                used_pct = 0
                        except Exception as e:
                            self.logger.debug(f"statvfs failed for {log_dir}: {e}")
                            free_gb = None
                            total_gb = None
                            used_pct = 0
                    
                    broker_stat = {
                        'broker': broker_id,
                        'path': log_dir,
                        'kafka_data_gb': kafka_data_gb,
                        'usage_pct': used_pct if used_pct else 0,
                        'free_gb': free_gb,
                        'total_gb': total_gb
                    }
                    broker_stats.append(broker_stat)
                    
                    # Log broker disk info
                    if free_gb is not None and total_gb is not None:
                        self.logger.info(
                            f"Broker {broker_id}: {log_dir} - "
                            f"Kafka data: {kafka_data_gb:.2f}GB | "
                            f"Disk: {free_gb:.1f}GB free / {total_gb:.1f}GB total ({used_pct:.1f}% used)"
                        )
                    else:
                        self.logger.info(
                            f"Broker {broker_id}: {log_dir} - "
                            f"Kafka data: {kafka_data_gb:.2f}GB"
                        )
                    
                    # Track max usage
                    if used_pct > max_usage:
                        max_usage = used_pct
                        disk_info = broker_stat
                    
                    # Warnings based on disk usage percentage
                    if used_pct > 0:
                        if used_pct >= 85:
                            self.logger.warning(
                                f"⚠ HIGH DISK USAGE: {used_pct:.1f}% on broker {broker_id} at {log_dir}"
                            )
                        if free_gb is not None and free_gb < 10:
                            self.logger.warning(
                                f"⚠ LOW DISK SPACE: {free_gb:.1f}GB free on broker {broker_id} at {log_dir}"
                            )
            
            if not broker_stats:
                self.logger.warning("Could not determine disk usage from kafka-log-dirs.sh")
                return 0.0, {}
            
            if max_usage > 0:
                self.logger.info(f"Highest disk usage: {max_usage:.1f}% on broker {disk_info.get('broker')}")
            else:
                self.logger.info("Disk usage data collected for all brokers")
            
            return max_usage, disk_info
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse kafka-log-dirs.sh JSON output: {e}")
            self.logger.debug(f"Output was: {output[:500]}")
            return 0.0, {}
        except Exception as e:
            self.logger.error(f"Failed to check disk usage: {e}")
            return 0.0, {}
    
    def alter_partitions(self, topic: str, count: int, dry_run: bool) -> bool:
        """Alter partition count."""
        if dry_run:
            self.logger.info(f"[DRY-RUN] Would set partitions to {count}")
            return True
        
        try:
            self._run([
                self._tool('kafka-topics.sh'),
                '--bootstrap-server', self.bootstrap,
                '--alter', '--topic', topic,
                '--partitions', str(count)
            ])
            self.logger.info(f"✓ Partitions set to {count}")
            return True
        except Exception as e:
            self.logger.error(f"✗ Failed to alter partitions: {e}")
            return False
    
    def alter_config(self, topic: str, key: str, value: str, dry_run: bool) -> bool:
        """Alter topic configuration."""
        if dry_run:
            self.logger.info(f"[DRY-RUN] Would set {key}={value}")
            return True
        
        try:
            self._run([
                self._tool('kafka-configs.sh'),
                '--bootstrap-server', self.bootstrap,
                '--alter', '--entity-type', 'topics',
                '--entity-name', topic,
                '--add-config', f'{key}={value}'
            ])
            self.logger.info(f"✓ Set {key}={value}")
            return True
        except Exception as e:
            self.logger.error(f"✗ Failed to alter {key}: {e}")
            return False


# ==============================================================================
# PRE-CHECKS
# ==============================================================================

class PreChecks:
    """Pre-flight validation checks."""
    
    def __init__(self, kafka: KafkaCLI, config: Config, logger: logging.Logger):
        self.kafka = kafka
        self.config = config
        self.logger = logger
        self.warnings = []
        self.errors = []
    
    def validate_partition_increase(self, topic: str, target: int, current: Optional[int] = None) -> bool:
        """Validate partition increase."""
        self.logger.info("=" * 80)
        self.logger.info("PARTITION INCREASE PRE-CHECKS")
        self.logger.info("=" * 80)
        
        # Get topic info
        info = self.kafka.get_topic_info(topic)
        if not info:
            self.errors.append(f"Topic {topic} does not exist")
            self.logger.error(f"✗ Topic not found")
            return False
        
        self.logger.info(f"✓ Topic exists")
        
        actual = info['partition_count']
        self.logger.info(f"Current partitions: {actual}")
        
        if current and actual != current:
            self.warnings.append(f"Config shows {current} partitions, actual is {actual}")
            self.logger.warning(f"⚠ Partition count mismatch")
        
        # Check target > current
        if target <= actual:
            self.errors.append(f"Target ({target}) must be > current ({actual})")
            self.logger.error(f"✗ Invalid target partition count")
            return False
        
        self.logger.info(f"✓ Target ({target}) > Current ({actual})")
        
        # Check ISR health
        unhealthy = [p for p in info['partitions'] if len(p['isr']) < len(p['replicas'])]
        if unhealthy:
            self.errors.append(f"Found {len(unhealthy)} partitions with unhealthy ISR")
            self.logger.error(f"✗ Unhealthy ISR detected")
            return False
        
        self.logger.info(f"✓ All {len(info['partitions'])} partitions have healthy ISR")
        
        # Check disk usage
        disk_usage, disk_info = self.kafka.get_disk_usage()
        
        if disk_usage >= 80.0:
            broker = disk_info.get('broker', 'unknown')
            path = disk_info.get('path', 'unknown')
            self.errors.append(f"Disk usage ({disk_usage:.1f}%) >= 80% on broker {broker} at {path}")
            self.logger.error(f"✗ High disk usage: {disk_usage:.1f}%")
            return False
        
        self.logger.info(f"✓ Disk usage acceptable: {disk_usage:.1f}%")
        
        # Warn about resource impact
        increase = target - actual
        self.warnings.append(f"Adding {increase} partitions will increase broker resource usage")
        self.logger.warning(f"⚠ Resource impact: +{increase} partitions")
        
        self.logger.info("=" * 80)
        return True
    
    def validate_config_change(self, topic: str, key: str, new_value: any, 
                              current_value: Optional[any] = None) -> bool:
        """Validate configuration change."""
        self.logger.info("=" * 80)
        self.logger.info(f"CONFIG CHANGE PRE-CHECKS: {key}")
        self.logger.info("=" * 80)
        
        # Check topic exists
        info = self.kafka.get_topic_info(topic)
        if not info:
            self.errors.append(f"Topic {topic} does not exist")
            self.logger.error(f"✗ Topic not found")
            return False
        
        self.logger.info(f"✓ Topic exists")
        
        # Get current configs
        configs = self.kafka.get_configs(topic)
        
        # Special validation for retention.ms
        if key == 'retention.ms':
            actual = int(configs.get('retention.ms', 604800000))
            self.logger.info(f"Current retention.ms: {actual}")
            
            if new_value < actual:
                self.warnings.append(
                    f"⚠ REDUCING retention from {actual}ms to {new_value}ms - "
                    "Data older than new retention will be deleted!"
                )
                self.logger.warning(f"⚠ RETENTION DECREASE: {actual}ms → {new_value}ms")
                self.logger.warning(f"⚠ DATA LOSS WARNING: Old data will be permanently deleted!")
            else:
                disk_usage, disk_info = self.kafka.get_disk_usage()
                if disk_usage >= 70.0:
                    broker = disk_info.get('broker', 'unknown')
                    path = disk_info.get('path', 'unknown')
                    self.warnings.append(
                        f"Disk at {disk_usage:.1f}% on broker {broker} at {path} with retention increase"
                    )
                    self.logger.warning(f"⚠ High disk usage with retention increase")
        
        # Special validation for min.insync.replicas
        elif key == 'min.insync.replicas':
            rf = info['replication_factor']
            if int(new_value) > rf:
                self.errors.append(f"min.insync.replicas ({new_value}) cannot exceed RF ({rf})")
                self.logger.error(f"✗ Invalid min.insync.replicas")
                return False
            self.logger.info(f"✓ min.insync.replicas ({new_value}) <= RF ({rf})")
        
        self.logger.info("=" * 80)
        return True


# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================

class TopicAlterator:
    """Main orchestrator for topic alteration."""
    
    def __init__(self, config_path: str, dry_run: bool, logger: logging.Logger, snapshot_dir: str = "/home/sre/snapshot"):
        self.dry_run = dry_run
        self.logger = logger
        self.snapshot_dir = snapshot_dir
        self.config = Config(config_path, logger)
        self.kafka = KafkaCLI(self.config.bootstrap_servers, logger, self.config.kafka_home)
        self.checks = PreChecks(self.kafka, self.config, logger)
    
    def run(self) -> bool:
        """Execute alterations with pre-checks."""
        topics = self.config.topic_names
        
        self.logger.info("=" * 80)
        self.logger.info("KAFKA TOPIC ALTERATION")
        self.logger.info("=" * 80)
        self.logger.info(f"Topics to process: {len(topics)}")
        for topic in topics:
            self.logger.info(f"  - {topic}")
        self.logger.info(f"Dry run: {self.dry_run}")
        self.logger.info("=" * 80)
        
        # Test connectivity once
        if not self.kafka.test_connection():
            return False
        
        # Process each topic
        overall_success = True
        results = []
        
        for idx, topic in enumerate(topics, 1):
            self.logger.info("\n" + "=" * 80)
            self.logger.info(f"PROCESSING TOPIC {idx}/{len(topics)}: {topic}")
            self.logger.info("=" * 80)
            
            success = self._process_single_topic(topic)
            results.append({'topic': topic, 'success': success})
            overall_success &= success
            
            if not success:
                self.logger.error(f"✗ Failed to process {topic}")
            else:
                self.logger.info(f"✓ Successfully processed {topic}")
        
        # Summary for multiple topics
        if len(topics) > 1:
            self._print_multi_topic_summary(results)
        else:
            self._print_summary(overall_success)
        
        return overall_success
    
    def _process_single_topic(self, topic: str) -> bool:
        """Process alterations for a single topic."""
        
        # Save snapshot before making any changes (skip in dry-run)
        if not self.dry_run:
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info("SAVING CONFIGURATION SNAPSHOT")
            self.logger.info("=" * 80)
            
            snapshot_saved = self.kafka.save_topic_snapshot(topic, self.snapshot_dir)
            if not snapshot_saved:
                self.logger.warning("⚠ Failed to save snapshot, but continuing with alterations")
            self.logger.info("=" * 80)
        else:
            self.logger.info("\n[DRY-RUN] Would save snapshot before alterations")
        
        success = True
        alterations = self.config.alterations
        
        # Process partition alteration
        if 'partitions' in alterations:
            success &= self._alter_partitions(topic, alterations['partitions'])
        
        # Process retention alteration
        if 'retention' in alterations:
            success &= self._alter_retention(topic, alterations['retention'])
        
        # Process other configs
        if 'other_configs' in alterations:
            success &= self._alter_configs(topic, alterations['other_configs'])
        
        return success
    
    def _alter_partitions(self, topic: str, config: Dict) -> bool:
        """Process partition alteration."""
        self.logger.info("\n" + "-" * 80)
        self.logger.info("PROCESSING: PARTITION ALTERATION")
        self.logger.info("-" * 80)
        
        target = config['target']
        current = config.get('current')
        
        if not self.checks.validate_partition_increase(topic, target, current):
            self.logger.error("Pre-checks failed")
            return False
        
        self._log_warnings()
        return self.kafka.alter_partitions(topic, target, self.dry_run)
    
    def _alter_retention(self, topic: str, config: Dict) -> bool:
        """Process retention alteration."""
        self.logger.info("\n" + "-" * 80)
        self.logger.info("PROCESSING: RETENTION ALTERATION")
        self.logger.info("-" * 80)
        
        new_ms = self.config.parse_time(config['new'])
        current_ms = self.config.parse_time(config['current']) if 'current' in config else None
        
        if not self.checks.validate_config_change(topic, 'retention.ms', new_ms, current_ms):
            self.logger.error("Pre-checks failed")
            return False
        
        self._log_warnings()
        return self.kafka.alter_config(topic, 'retention.ms', str(new_ms), self.dry_run)
    
    def _alter_configs(self, topic: str, configs: Dict) -> bool:
        """Process other config alterations."""
        self.logger.info("\n" + "-" * 80)
        self.logger.info("PROCESSING: CONFIG ALTERATIONS")
        self.logger.info("-" * 80)
        
        success = True
        for key, value in configs.items():
            self.logger.info(f"\nProcessing: {key}={value}")
            
            if not self.checks.validate_config_change(topic, key, value):
                self.logger.error(f"Pre-checks failed for {key}")
                success = False
                continue
            
            self._log_warnings()
            if not self.kafka.alter_config(topic, key, str(value), self.dry_run):
                success = False
        
        return success
    
    def _log_warnings(self):
        """Log any warnings from checks."""
        if self.checks.warnings:
            for warning in self.checks.warnings:
                self.logger.warning(f"⚠ {warning}")
            self.checks.warnings = []
    
    def _print_summary(self, success: bool):
        """Print operation summary for single topic."""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("OPERATION SUMMARY")
        self.logger.info("=" * 80)
        
        if self.checks.errors:
            self.logger.error(f"Errors: {len(self.checks.errors)}")
            for error in self.checks.errors:
                self.logger.error(f"  - {error}")
        
        if success:
            self.logger.info("✓ Operation completed successfully")
        else:
            self.logger.error("✗ Operation failed")
        
        self.logger.info("=" * 80)
    
    def _print_multi_topic_summary(self, results: List[Dict]):
        """Print summary for multiple topics."""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("BATCH OPERATION SUMMARY")
        self.logger.info("=" * 80)
        
        success_count = sum(1 for r in results if r['success'])
        fail_count = len(results) - success_count
        
        self.logger.info(f"Total topics: {len(results)}")
        self.logger.info(f"Successful: {success_count}")
        self.logger.info(f"Failed: {fail_count}")
        self.logger.info("")
        
        if fail_count > 0:
            self.logger.error("Failed topics:")
            for result in results:
                if not result['success']:
                    self.logger.error(f"  ✗ {result['topic']}")
        
        if success_count > 0:
            self.logger.info("Successful topics:")
            for result in results:
                if result['success']:
                    self.logger.info(f"  ✓ {result['topic']}")
        
        self.logger.info("=" * 80)


# ==============================================================================
# CLI
# ==============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Kafka Topic Alteration Script with Pre-checks',
        epilog='Example: python kafka_topic_alter.py --config config.yaml --dry-run'
    )
    parser.add_argument('--config', required=True, help='YAML configuration file')
    parser.add_argument('--dry-run', action='store_true', help='Simulate without making changes')
    parser.add_argument('--snapshot-dir', default='/home/sre/snapshot', 
                       help='Directory to save configuration snapshots (default: /home/sre/snapshot)')
    parser.add_argument('--version', action='version', version='v2.0.0')
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        alterator = TopicAlterator(args.config, args.dry_run, logger, args.snapshot_dir)
        success = alterator.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
