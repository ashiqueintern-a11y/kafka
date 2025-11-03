#!/usr/bin/env python3
"""
Production-Grade Kafka Topic Manager with Pre-check Mode
=========================================================
Unified script for Kafka topic validation and creation.
Now with OpenTSDB integration for accurate disk usage monitoring.
Compatible with Kafka 2.8.2

Author: DevOps Team
Version: 3.1.0

Usage:
  Pre-check only:  python3 kafka_topic_manager.py topic.yaml --precheck
  Full creation:   python3 kafka_topic_manager.py topic.yaml
"""

import sys
import os
import re
import logging
import time
import subprocess
import glob
import json
import requests
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse

import yaml


class KafkaTopicManager:
    """Kafka topic manager with pre-checks and creation capabilities."""

    VALID_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9._-]+$')
    MAX_NAME_LENGTH = 249
    
    # OpenTSDB configuration
    OPENTSDB_URL = "http://opentsdb-read-no-dp-limit.nixy.stg-drove.phonepe.nb6/api/query"
    OPENTSDB_TIMEOUT = 10  # seconds

    def __init__(self, config_file: str, log_level: str = "INFO", precheck_only: bool = False):
        self.config_file = config_file
        self.config = None
        self.precheck_only = precheck_only
        self.validation_results = {}
        self.logger = self._setup_logging(log_level)
        self.kafka_bin_path = self._find_kafka_bin()
        self.kafka_topics_cmd = os.path.join(self.kafka_bin_path, 'kafka-topics.sh')
        self.kafka_log_dirs_cmd = os.path.join(self.kafka_bin_path, 'kafka-log-dirs.sh')
        self.bootstrap_servers = None
        self.broker_hostnames = {}  # Map broker ID to hostname

    def _setup_logging(self, log_level: str) -> logging.Logger:
        """Configure logging with file and console handlers."""
        logger_name = "KafkaTopicPreChecker" if self.precheck_only else "KafkaTopicManager"
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, log_level.upper()))
        logger.handlers.clear()

        # Define the log directory based on mode
        if self.precheck_only:
            log_dir = "/var/log/kafka-topic-prechecks"
            log_file_prefix = "kafka_topic_prechecks"
        else:
            log_dir = "/var/log/kafka-topic-creation"
            log_file_prefix = "kafka_topic_manager"

        # Create the directory if it doesn't exist
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            # Handle potential permission errors
            print(f"ERROR: Could not create log directory {log_dir}. "
                  f"Check permissions. Error: {e}", file=sys.stderr)
            # Fall back to current directory
            log_dir = "."
            print(f"Warning: Falling back to logging in current directory: {os.path.abspath(log_dir)}", 
                  file=sys.stderr)

        # File handler
        log_file_name = f"{log_file_prefix}_{time.strftime('%Y%m%d_%H%M%S')}.log"
        log_file_path = os.path.join(log_dir, log_file_name)

        try:
            fh = logging.FileHandler(log_file_path)
        except PermissionError:
            print(f"ERROR: Permission denied writing to {log_file_path}.", file=sys.stderr)
            log_dir = "."
            log_file_path = os.path.join(log_dir, log_file_name)
            print(f"Warning: Falling back to logging in current directory: {os.path.abspath(log_file_path)}", 
                  file=sys.stderr)
            fh = logging.FileHandler(log_file_path)

        fh.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)s | %(funcName)s | %(message)s'
        ))

        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        logger.addHandler(fh)
        logger.addHandler(ch)

        # Now we can log (logger is created)
        logger.info(f"Logging to: {os.path.abspath(log_file_path)}")
        return logger

    def _find_kafka_bin(self) -> str:
        """Find Kafka bin directory by reading kafka-env.sh or searching filesystem."""
        self.logger.debug("Searching for Kafka bin directory...")

        # Strategy 1: Read from kafka-env.sh (most reliable for production systems)
        kafka_env_paths = [
            '/etc/kafka/*/0/kafka-env.sh',
            '/etc/kafka/conf/kafka-env.sh',
            '/usr/odp/*/kafka/config/kafka-env.sh',
            '/usr/hdp/*/kafka/config/kafka-env.sh',
        ]

        for pattern in kafka_env_paths:
            for kafka_env in glob.glob(pattern):
                try:
                    self.logger.debug(f"Checking kafka-env.sh at: {kafka_env}")
                    with open(kafka_env, 'r') as f:
                        for line in f:
                            # Look for CLASSPATH export that contains kafka-broker path
                            if 'CLASSPATH' in line and 'kafka-broker' in line:
                                # Extract path like: /usr/odp/current/kafka-broker/config
                                match = re.search(r'(/[^\s:]+/kafka-broker)', line)
                                if match:
                                    kafka_base = match.group(1)
                                    bin_path = os.path.join(kafka_base, 'bin')
                                    if os.path.exists(bin_path):
                                        self.logger.info(f"Found Kafka bin from kafka-env.sh: {bin_path}")
                                        return bin_path
                except Exception as e:
                    self.logger.debug(f"Failed to read {kafka_env}: {e}")

        # Strategy 2: Check symbolic link /usr/odp/current/kafka-broker (ODP standard)
        odp_current = '/usr/odp/current/kafka-broker/bin'
        if os.path.exists(odp_current):
            self.logger.info(f"Found Kafka bin at: {odp_current}")
            return odp_current

        # Strategy 3: Check symbolic link /usr/hdp/current/kafka-broker (HDP standard)
        hdp_current = '/usr/hdp/current/kafka-broker/bin'
        if os.path.exists(hdp_current):
            self.logger.info(f"Found Kafka bin at: {hdp_current}")
            return hdp_current

        # Strategy 4: Use 'which' to find kafka-topics.sh
        try:
            result = subprocess.run(['which', 'kafka-topics.sh'],
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                tool_path = result.stdout.strip()
                bin_path = os.path.dirname(tool_path)
                self.logger.info(f"Found Kafka bin via 'which': {bin_path}")
                return bin_path
        except Exception as e:
            self.logger.debug(f"'which' command failed: {e}")

        # Strategy 5: Search filesystem for kafka-topics.sh
        self.logger.info("Searching filesystem for kafka-topics.sh (this may take a moment)...")
        try:
            result = subprocess.run(
                ['find', '/usr/', '/opt/', '-name', 'kafka-topics.sh', '-type', 'f'],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0 and result.stdout.strip():
                paths = result.stdout.strip().split('\n')
                for path in paths:
                    if os.access(path, os.X_OK):
                        bin_path = os.path.dirname(path)
                        self.logger.info(f"Found Kafka bin via filesystem search: {bin_path}")
                        return bin_path
        except Exception as e:
            self.logger.debug(f"Filesystem search failed: {e}")

        # If nothing found, show error
        self.logger.error("="*80)
        self.logger.error("KAFKA INSTALLATION NOT FOUND")
        self.logger.error("="*80)
        self.logger.error("Could not locate Kafka installation.")
        self.logger.error("")
        self.logger.error("Searched locations:")
        self.logger.error("  - /etc/kafka/*/0/kafka-env.sh")
        self.logger.error("  - /usr/odp/current/kafka-broker/bin")
        self.logger.error("  - /usr/hdp/current/kafka-broker/bin")
        self.logger.error("  - System PATH")
        self.logger.error("  - Filesystem search in /usr/ and /opt/")
        self.logger.error("")
        self.logger.error("Please ensure Kafka is installed on this system.")
        self.logger.error("="*80)
        sys.exit(1)

    def _run_cmd(self, cmd: List[str]) -> Tuple[bool, str, str]:
        """Execute command and return (success, stdout, stderr)."""
        try:
            self.logger.debug(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return (result.returncode == 0, result.stdout.strip(), result.stderr.strip())
        except subprocess.TimeoutExpired:
            self.logger.error("Command timed out after 30 seconds")
            return (False, "", "Command timeout")
        except FileNotFoundError as e:
            self.logger.error(f"Command not found: {cmd[0]}")
            self.logger.error("The kafka-topics.sh tool is not available at the detected path")
            self.logger.error(f"Detected path: {self.kafka_topics_cmd}")
            self.logger.error("")
            self.logger.error("Solutions:")
            self.logger.error("  1. Verify Kafka installation location")
            self.logger.error("  2. Re-run with correct KAFKA_HOME:")
            self.logger.error(f"     python3 {sys.argv[0]} {self.config_file}")
            return (False, "", str(e))
        except Exception as e:
            self.logger.error(f"Command execution error: {e}")
            return (False, "", str(e))

    def _extract_broker_hostname(self, bootstrap_server: str) -> str:
        """Extract hostname from bootstrap server string (host:port)."""
        try:
            # Handle different formats
            if '://' in bootstrap_server:
                # URL format
                parsed = urlparse(bootstrap_server)
                return parsed.hostname or bootstrap_server.split(':')[0]
            else:
                # Simple host:port format
                return bootstrap_server.split(':')[0]
        except Exception as e:
            self.logger.warning(f"Could not parse hostname from {bootstrap_server}: {e}")
            return bootstrap_server

    def _map_broker_ids_to_hostnames(self) -> None:
        """Map broker IDs to their hostnames."""
        self.logger.debug("Mapping broker IDs to hostnames...")
        
        # Try to get broker information using kafka-broker-api-versions.sh
        kafka_broker_api_cmd = os.path.join(self.kafka_bin_path, 'kafka-broker-api-versions.sh')
        if os.path.exists(kafka_broker_api_cmd):
            cmd = [kafka_broker_api_cmd, '--bootstrap-server', self.bootstrap_servers]
            success, stdout, _ = self._run_cmd(cmd)
            if success:
                # Parse output for broker information
                # Format: stg-hdpashique101.phonepe.nb6:6667 (id: 1001 rack: null)
                for line in stdout.split('\n'):
                    if '(id:' in line:
                        try:
                            # Extract hostname and broker ID
                            host_part = line.split('(')[0].strip()
                            hostname = self._extract_broker_hostname(host_part)
                            
                            id_match = re.search(r'id:\s*(\d+)', line)
                            if id_match:
                                broker_id = int(id_match.group(1))
                                self.broker_hostnames[broker_id] = hostname
                                self.logger.debug(f"Mapped broker {broker_id} to {hostname}")
                        except Exception as e:
                            self.logger.debug(f"Could not parse broker info from line: {e}")
        
        # If we don't have mappings yet, use kafka-metadata.sh
        if not self.broker_hostnames:
            cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers,
                   '--describe', '--exclude-internal']
            success, stdout, _ = self._run_cmd(cmd)
            
            if success:
                # Get unique broker IDs from the output
                broker_ids = set()
                for line in stdout.split('\n'):
                    # Look for Leader and Replicas to find broker IDs
                    if 'Leader:' in line or 'Replicas:' in line:
                        parts = line.split()
                        for part in parts:
                            if part.isdigit():
                                try:
                                    broker_ids.add(int(part))
                                except ValueError:
                                    pass
                            elif ',' in part:
                                # Handle comma-separated broker IDs
                                for bid in part.split(','):
                                    if bid.isdigit():
                                        try:
                                            broker_ids.add(int(bid))
                                        except ValueError:
                                            pass
                
                # Now we have broker IDs, map them to hostnames from bootstrap servers
                # For PhonePe staging, we can infer the mapping pattern
                if broker_ids:
                    self.logger.debug(f"Found broker IDs: {broker_ids}")
                    
                    # Extract hostnames from bootstrap servers
                    hostnames = []
                    for bootstrap_server in self.config['cluster']['bootstrap_servers']:
                        hostname = self._extract_broker_hostname(bootstrap_server)
                        # Ensure we have the full hostname with domain
                        if '.' not in hostname and 'phonepe.nb6' not in hostname:
                            hostname = f"{hostname}.phonepe.nb6"
                        hostnames.append(hostname)
                    
                    # Common pattern for staging: broker IDs like 1001-1004 map to hostnames in order
                    sorted_broker_ids = sorted(broker_ids)
                    for i, broker_id in enumerate(sorted_broker_ids):
                        if i < len(hostnames):
                            self.broker_hostnames[broker_id] = hostnames[i]
                            self.logger.info(f"Mapped broker {broker_id} to {hostnames[i]}")
        
        # Final fallback: if we still don't have mappings, use a direct mapping
        if not self.broker_hostnames:
            self.logger.warning("Could not automatically map broker IDs to hostnames")
            # Extract hostnames from bootstrap servers
            for i, bootstrap_server in enumerate(self.config['cluster']['bootstrap_servers']):
                hostname = self._extract_broker_hostname(bootstrap_server)
                # Map common broker IDs to hostnames in order
                broker_id = 1001 + i  # Assuming broker IDs start at 1001
                self.broker_hostnames[broker_id] = hostname
                self.logger.info(f"Fallback mapping: broker {broker_id} to {hostname}")
                    
        self.logger.info(f"Final broker hostname mapping: {self.broker_hostnames}")

    def _query_opentsdb_disk_usage(self, hostname: str, path: str) -> Optional[Dict]:
        """Query OpenTSDB for disk usage metrics for a specific host and path."""
        try:
            # Normalize path (remove trailing slashes except for root)
            if path != '/':
                path = path.rstrip('/')
            
            # Construct OpenTSDB query
            query = {
                "start": "5m-ago",  # Get data from last 5 minutes
                "queries": [
                    {
                        "metric": "disk.field.used_percent",
                        "aggregator": "avg",
                        "tags": {
                            "node_host": hostname,
                            "path": path
                        }
                    }
                ]
            }
            
            self.logger.debug(f"Querying OpenTSDB for {hostname}:{path}")
            self.logger.debug(f"URL: {self.OPENTSDB_URL}")
            self.logger.debug(f"Query: {json.dumps(query, indent=2)}")
            
            # Make the request to OpenTSDB
            response = requests.post(
                self.OPENTSDB_URL,
                json=query,
                headers={'Content-Type': 'application/json'},
                timeout=self.OPENTSDB_TIMEOUT
            )
            
            self.logger.debug(f"Response status: {response.status_code}")
            self.logger.debug(f"Response headers: {response.headers}")
            
            if response.status_code == 200:
                # Check if response has content
                if not response.text or response.text.strip() == "":
                    self.logger.warning(f"Empty response from OpenTSDB for {hostname}:{path}")
                    return None
                
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON response from OpenTSDB: {e}")
                    self.logger.debug(f"Response text: {response.text[:500]}")
                    return None
                
                if data and isinstance(data, list) and len(data) > 0:
                    # Get the most recent data point
                    timeseries = data[0]
                    if 'dps' in timeseries and timeseries['dps']:
                        # dps is a dict of timestamp: value
                        timestamps = list(timeseries['dps'].keys())
                        if timestamps:
                            # Convert string timestamps to int if needed
                            timestamps = [int(ts) if isinstance(ts, str) else ts for ts in timestamps]
                            latest_timestamp = max(timestamps)
                            used_percent = timeseries['dps'][str(latest_timestamp)]
                            
                            self.logger.debug(f"Got disk usage for {hostname}:{path} = {used_percent}%")
                            
                            # Also query for total and available space
                            space_query = {
                                "start": "5m-ago",
                                "queries": [
                                    {
                                        "metric": "disk.field.total",
                                        "aggregator": "avg",
                                        "tags": {
                                            "node_host": hostname,
                                            "path": path
                                        }
                                    },
                                    {
                                        "metric": "disk.field.available",
                                        "aggregator": "avg",
                                        "tags": {
                                            "node_host": hostname,
                                            "path": path
                                        }
                                    }
                                ]
                            }
                            
                            space_response = requests.post(
                                self.OPENTSDB_URL,
                                json=space_query,
                                headers={'Content-Type': 'application/json'},
                                timeout=self.OPENTSDB_TIMEOUT
                            )
                            
                            total_bytes = None
                            available_bytes = None
                            
                            if space_response.status_code == 200:
                                try:
                                    space_data = space_response.json()
                                    for metric_data in space_data:
                                        if 'metric' in metric_data and 'dps' in metric_data and metric_data['dps']:
                                            metric_name = metric_data['metric']
                                            dps_timestamps = list(metric_data['dps'].keys())
                                            if dps_timestamps:
                                                dps_timestamps = [int(ts) if isinstance(ts, str) else ts for ts in dps_timestamps]
                                                latest_ts = max(dps_timestamps)
                                                value = metric_data['dps'][str(latest_ts)]
                                                
                                                if metric_name == 'disk.field.total':
                                                    total_bytes = value
                                                elif metric_name == 'disk.field.available':
                                                    available_bytes = value
                                except Exception as e:
                                    self.logger.debug(f"Could not parse space metrics: {e}")
                            
                            return {
                                'used_percent': used_percent,
                                'total_bytes': total_bytes,
                                'available_bytes': available_bytes,
                                'timestamp': latest_timestamp
                            }
                        else:
                            self.logger.warning(f"No timestamps in data points for {hostname}:{path}")
                    else:
                        self.logger.warning(f"No data points found for {hostname}:{path}")
                else:
                    self.logger.warning(f"Empty or invalid response format from OpenTSDB for {hostname}:{path}")
                    self.logger.debug(f"Response data: {data}")
            else:
                self.logger.warning(f"OpenTSDB query failed with status {response.status_code}")
                self.logger.debug(f"Response text: {response.text[:500]}")
                
        except requests.exceptions.Timeout:
            self.logger.warning(f"OpenTSDB query timed out for {hostname}:{path}")
        except requests.exceptions.ConnectionError as e:
            self.logger.warning(f"Connection error to OpenTSDB for {hostname}:{path}: {e}")
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"OpenTSDB query failed for {hostname}:{path}: {e}")
        except Exception as e:
            self.logger.warning(f"Unexpected error querying OpenTSDB for {hostname}:{path}: {e}")
            self.logger.debug("Exception details:", exc_info=True)
        
        return None

    def load_config(self) -> bool:
        """Load and validate YAML configuration."""
        try:
            self.logger.info(f"Loading config: {self.config_file}")

            if not Path(self.config_file).exists():
                self.logger.error(f"Config file not found: {self.config_file}")
                self.validation_results['config_file'] = {'status': 'FAILED', 'message': 'File not found'}
                return False

            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)

            # Validate required fields
            required = {
                'topic': ['name', 'partitions', 'replication_factor'],
                'cluster': ['bootstrap_servers']
            }

            for section, fields in required.items():
                if section not in self.config:
                    self.logger.error(f"Missing section: {section}")
                    self.validation_results['config_structure'] = {
                        'status': 'FAILED', 
                        'message': f'Missing section: {section}'
                    }
                    return False
                for field in fields:
                    if field not in self.config[section]:
                        self.logger.error(f"Missing {section}.{field}")
                        self.validation_results['config_structure'] = {
                            'status': 'FAILED', 
                            'message': f'Missing {section}.{field}'
                        }
                        return False

            self.bootstrap_servers = ','.join(self.config['cluster']['bootstrap_servers'])
            
            # Optional: Load OpenTSDB configuration if provided
            if 'monitoring' in self.config and 'opentsdb_url' in self.config['monitoring']:
                opentsdb_url = self.config['monitoring']['opentsdb_url']
                # Ensure the URL ends with /api/query if it doesn't already
                if not opentsdb_url.endswith('/api/query'):
                    if opentsdb_url.endswith('/'):
                        opentsdb_url = opentsdb_url + 'api/query'
                    else:
                        opentsdb_url = opentsdb_url + '/api/query'
                self.OPENTSDB_URL = opentsdb_url
                self.logger.info(f"Using custom OpenTSDB URL: {self.OPENTSDB_URL}")
            
            self.logger.info("Config loaded successfully")
            self.validation_results['config_file'] = {'status': 'PASSED', 'message': 'Config loaded successfully'}
            return True

        except Exception as e:
            self.logger.error(f"Config load error: {e}")
            self.validation_results['config_file'] = {'status': 'FAILED', 'message': str(e)}
            return False

    def validate_topic_name(self, name: str) -> Tuple[bool, str]:
        """Validate topic name."""
        if not name or len(name) == 0:
            return False, "Topic name cannot be empty"

        if len(name) > self.MAX_NAME_LENGTH:
            return False, f"Name exceeds {self.MAX_NAME_LENGTH} characters"

        if not self.VALID_NAME_PATTERN.match(name):
            return False, "Name must contain only ASCII alphanumerics, '.', '_', '-'"

        if name in ['.', '..']:
            return False, "Name cannot be '.' or '..'"

        return True, None

    def check_connectivity(self) -> bool:
        """Test connection to Kafka brokers."""
        self.logger.info(f"Testing connectivity: {self.bootstrap_servers}")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers, '--list']
        success, _, stderr = self._run_cmd(cmd)

        if not success:
            self.logger.error(f"Connection failed: {stderr}")
            self.validation_results['connectivity'] = {'status': 'FAILED', 'message': stderr}
            return False

        self.logger.info("Connected successfully")
        self.validation_results['connectivity'] = {'status': 'PASSED', 'message': 'Connected successfully'}
        return True

    def check_topic_exists(self, topic: str) -> bool:
        """Check if topic already exists."""
        self.logger.info(f"Checking if topic '{topic}' exists")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers, '--list']
        success, stdout, _ = self._run_cmd(cmd)

        if not success:
            self.validation_results['topic_exists'] = {'status': 'ERROR', 'message': 'Could not check topic existence'}
            return True  # Fail-safe

        exists = topic in stdout.split('\n')
        if exists:
            self.logger.error(f"Topic '{topic}' already exists")
            self.validation_results['topic_exists'] = {'status': 'FAILED', 'message': 'Topic already exists'}
        else:
            self.logger.info("Topic does not exist (OK)")
            self.validation_results['topic_exists'] = {'status': 'PASSED', 'message': 'Topic does not exist'}

        return exists

    def get_broker_count(self) -> int:
        """Get number of available brokers."""
        self.logger.info("Checking broker count")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers,
               '--describe', '--exclude-internal']
        success, stdout, _ = self._run_cmd(cmd)

        if not success:
            # Fall back to bootstrap servers count
            count = len(self.config['cluster']['bootstrap_servers'])
            self.logger.warning(f"Using bootstrap servers count: {count}")
            return count

        # Parse broker IDs from output
        broker_ids = set()
        for line in stdout.split('\n'):
            if 'Replicas:' in line:
                replicas = line.split('Replicas:')[1].split()[0]
                for bid in replicas.split(','):
                    try:
                        broker_ids.add(int(bid))
                    except ValueError:
                        pass

        count = len(broker_ids) if broker_ids else len(self.config['cluster']['bootstrap_servers'])
        self.logger.info(f"Available brokers: {count}")
        return count

    def validate_replication_factor(self, rf: int, broker_count: int) -> bool:
        """Validate replication factor against broker count."""
        self.logger.info(f"Validating RF={rf} against {broker_count} brokers")

        if rf < 1:
            self.logger.error("Replication factor must be at least 1")
            self.validation_results['replication_factor'] = {'status': 'FAILED', 'message': 'RF must be at least 1'}
            return False

        if rf > broker_count:
            self.logger.error(f"RF ({rf}) exceeds available brokers ({broker_count})")
            self.validation_results['replication_factor'] = {
                'status': 'FAILED', 
                'message': f'RF ({rf}) exceeds available brokers ({broker_count})'
            }
            return False

        if rf < 3:
            self.logger.warning(f"RF={rf} is below production recommendation of 3")
            self.validation_results['replication_factor'] = {
                'status': 'WARNING', 
                'message': f'RF={rf} is below production recommendation of 3'
            }
        else:
            self.validation_results['replication_factor'] = {
                'status': 'PASSED', 
                'message': f'RF={rf} is valid for {broker_count} brokers'
            }

        self.logger.info(f"RF validation passed")
        return True

    def check_under_replicated_partitions(self) -> int:
        """Check for under-replicated partitions."""
        self.logger.info("Checking under-replicated partitions")
        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers,
               '--describe', '--under-replicated-partitions']
        success, stdout, _ = self._run_cmd(cmd)

        if not success:
            self.logger.warning("Could not check under-replicated partitions")
            self.validation_results['under_replicated_partitions'] = {
                'status': 'WARNING', 
                'message': 'Could not check'
            }
            return -1

        urp_count = len([l for l in stdout.split('\n') if l.strip() and 'Topic:' in l])

        if urp_count > 0:
            self.logger.warning(f"Found {urp_count} under-replicated partitions")
            self.validation_results['under_replicated_partitions'] = {
                'status': 'WARNING', 
                'message': f'{urp_count} under-replicated partitions found'
            }
        else:
            self.logger.info("No under-replicated partitions (OK)")
            self.validation_results['under_replicated_partitions'] = {
                'status': 'PASSED', 
                'message': 'No under-replicated partitions'
            }

        return urp_count

    def check_disk_space(self) -> bool:
        """Check disk space using kafka-log-dirs.sh and OpenTSDB metrics."""
        self.logger.info("Checking disk space using kafka-log-dirs.sh and OpenTSDB")

        # First, map broker IDs to hostnames
        self._map_broker_ids_to_hostnames()

        cmd = [
            self.kafka_log_dirs_cmd,
            '--bootstrap-server', self.bootstrap_servers,
            '--describe'
        ]

        success, stdout, stderr = self._run_cmd(cmd)

        if not success:
            self.logger.warning(f"Could not check disk space: {stderr}")
            self.validation_results['disk_space'] = {'status': 'WARNING', 'message': 'Could not check disk space'}
            return True  # Don't fail the workflow, just warn

        # Check if output is empty
        if not stdout or stdout.strip() == "":
            self.logger.warning("kafka-log-dirs.sh returned empty output")
            self.logger.debug(f"STDERR: {stderr}")
            self.validation_results['disk_space'] = {'status': 'WARNING', 'message': 'Empty output from kafka-log-dirs'}
            return True

        try:
            # Extract JSON from output (kafka-log-dirs.sh outputs text before JSON)
            json_start = stdout.find('{')
            if json_start == -1:
                self.logger.warning("No JSON found in kafka-log-dirs output")
                self.logger.debug(f"Output: {stdout[:500]}")
                self.validation_results['disk_space'] = {'status': 'WARNING', 'message': 'No JSON in output'}
                return True

            json_str = stdout[json_start:]
            self.logger.debug(f"Extracted JSON (first 200 chars): {json_str[:200]}")

            # Parse JSON output from kafka-log-dirs.sh
            data = json.loads(json_str)

            brokers = data.get('brokers', [])
            if not brokers:
                self.logger.warning("No broker data found in kafka-log-dirs output")
                self.validation_results['disk_space'] = {'status': 'WARNING', 'message': 'No broker data found'}
                return True

            total_issues = 0
            broker_stats = []
            disk_warnings = []

            # Process each broker
            for broker_info in brokers:
                broker_id = broker_info.get('broker', 'unknown')
                log_dirs = broker_info.get('logDirs', [])

                # Get hostname for this broker
                hostname = self.broker_hostnames.get(broker_id, f"broker-{broker_id}")
                
                broker_total_bytes = 0
                broker_has_error = False
                log_dir_paths = []

                # Sum all partition sizes for this broker
                for log_dir in log_dirs:
                    log_dir_path = log_dir.get('logDir', 'unknown')
                    log_dir_paths.append(log_dir_path)

                    error = log_dir.get('error', None)
                    if error:
                        self.logger.error(f"Broker {broker_id} - Error in {log_dir_path}: {error}")
                        broker_has_error = True
                        total_issues += 1
                        disk_warnings.append(f"Broker {broker_id}: {error}")
                        continue

                    # Sum all partition sizes in this log directory
                    partitions = log_dir.get('partitions', [])
                    for partition in partitions:
                        size = partition.get('size', 0)
                        broker_total_bytes += size

                # Convert to GB
                broker_total_gb = broker_total_bytes / (1024**3)

                # Query OpenTSDB for disk metrics
                if log_dir_paths and not broker_has_error:
                    for log_dir_path in log_dir_paths:
                        if log_dir_path and log_dir_path != 'unknown':
                            # Extract the mount point from the log directory path
                            # e.g., /var/kafka-logs -> /var or /data/kafka -> /data
                            mount_point = self._get_mount_point(log_dir_path)
                            
                            self.logger.info(f"Querying OpenTSDB for broker {broker_id} ({hostname}) path: {mount_point}")
                            
                            # Query OpenTSDB for this broker's disk usage
                            metrics = self._query_opentsdb_disk_usage(hostname, mount_point)
                            
                            if metrics:
                                used_percent = metrics['used_percent']
                                total_bytes = metrics.get('total_bytes')
                                available_bytes = metrics.get('available_bytes')
                                
                                # Convert bytes to GB
                                total_gb = total_bytes / (1024**3) if total_bytes else None
                                available_gb = available_bytes / (1024**3) if available_bytes else None
                                
                                # Display results
                                if total_gb and available_gb:
                                    self.logger.info(
                                        f"Broker {broker_id} ({hostname}:{mount_point}): "
                                        f"Kafka data: {broker_total_gb:.2f}GB | "
                                        f"Disk: {available_gb:.1f}GB free / {total_gb:.1f}GB total "
                                        f"({used_percent:.1f}% used)"
                                    )
                                else:
                                    self.logger.info(
                                        f"Broker {broker_id} ({hostname}:{mount_point}): "
                                        f"Kafka data: {broker_total_gb:.2f}GB | "
                                        f"Disk usage: {used_percent:.1f}%"
                                    )
                                
                                # Check thresholds
                                if available_gb and available_gb < 10:
                                    self.logger.warning(
                                        f"Broker {broker_id} ({hostname}) - LOW DISK SPACE: Only {available_gb:.1f}GB available"
                                    )
                                    total_issues += 1
                                    disk_warnings.append(f"Broker {broker_id} ({hostname}): Low disk space ({available_gb:.1f}GB)")
                                elif used_percent > 85:
                                    self.logger.warning(
                                        f"Broker {broker_id} ({hostname}) - HIGH DISK USAGE: {used_percent:.1f}% used"
                                    )
                                    total_issues += 1
                                    disk_warnings.append(f"Broker {broker_id} ({hostname}): High disk usage ({used_percent:.1f}%)")
                                
                                # Store stats
                                broker_stats.append({
                                    'broker_id': broker_id,
                                    'hostname': hostname,
                                    'kafka_data_gb': broker_total_gb,
                                    'total_disk_gb': total_gb,
                                    'available_disk_gb': available_gb,
                                    'disk_used_pct': used_percent,
                                    'log_dir': log_dir_path,
                                    'mount_point': mount_point,
                                    'has_error': broker_has_error,
                                    'source': 'opentsdb'
                                })
                            else:
                                # OpenTSDB query failed, just show Kafka data
                                self.logger.warning(
                                    f"Could not get OpenTSDB metrics for broker {broker_id} ({hostname}:{mount_point})"
                                )
                                self.logger.info(
                                    f"Broker {broker_id} ({hostname}): Kafka data size: {broker_total_gb:.2f}GB"
                                )
                                
                                broker_stats.append({
                                    'broker_id': broker_id,
                                    'hostname': hostname,
                                    'kafka_data_gb': broker_total_gb,
                                    'log_dir': log_dir_path,
                                    'mount_point': mount_point,
                                    'has_error': broker_has_error,
                                    'source': 'kafka_only'
                                })

            if total_issues > 0:
                self.logger.warning(f"Found {total_issues} disk space issue(s)")
                self.validation_results['disk_space'] = {
                    'status': 'WARNING', 
                    'message': f'{total_issues} disk issues: {"; ".join(disk_warnings)}'
                }
            else:
                self.logger.info("Disk space check passed - all brokers healthy")
                self.validation_results['disk_space'] = {
                    'status': 'PASSED', 
                    'message': 'All brokers have sufficient disk space'
                }

            return True

        except json.JSONDecodeError as e:
            self.logger.warning(f"Could not parse kafka-log-dirs output as JSON: {e}")
            self.logger.warning("This might be a Kafka version compatibility issue")
            self.validation_results['disk_space'] = {'status': 'WARNING', 'message': 'Could not parse kafka-log-dirs output'}
            return True  # Don't fail workflow
        except Exception as e:
            self.logger.warning(f"Disk space check error: {e}")
            self.logger.debug(f"Error details: {e}", exc_info=True)
            self.validation_results['disk_space'] = {'status': 'WARNING', 'message': f'Check error: {e}'}
            return True  # Don't fail workflow

    def _get_mount_point(self, path: str) -> str:
        """Extract mount point from a path.
        Common patterns:
        /var/kafka-logs -> /var
        /data/kafka -> /data
        /data1/kafka -> /data1
        /kafka-logs -> /
        """
        if not path or path == 'unknown':
            return '/'
        
        # Normalize path
        path = os.path.normpath(path)
        
        # Common mount points to check
        common_mounts = ['/data', '/data1', '/data2', '/var', '/opt', '/mnt']
        
        for mount in common_mounts:
            if path.startswith(mount):
                return mount
        
        # If no common mount point found, return root
        return '/'

    def run_prechecks(self) -> bool:
        """Run all pre-checks and generate report."""
        self.logger.info("="*80)
        self.logger.info("KAFKA TOPIC PRE-CHECKS")
        self.logger.info("="*80)

        # Step 1: Load config
        if not self.load_config():
            self.generate_precheck_report()
            return False

        topic_name = self.config['topic']['name']
        rf = self.config['topic']['replication_factor']

        # Step 2: Validate topic name
        valid, error = self.validate_topic_name(topic_name)
        if not valid:
            self.logger.error(f"Topic name validation failed: {error}")
            self.validation_results['topic_name'] = {'status': 'FAILED', 'message': error}
            self.generate_precheck_report()
            return False
        else:
            self.logger.info(f"Topic name '{topic_name}' is valid")
            self.validation_results['topic_name'] = {'status': 'PASSED', 'message': f"'{topic_name}' is valid"}

        # Step 3: Check connectivity
        if not self.check_connectivity():
            self.generate_precheck_report()
            return False

        # Step 4: Check if exists
        if self.check_topic_exists(topic_name):
            self.generate_precheck_report()
            return False

        # Step 5: Check broker count and validate RF
        broker_count = self.get_broker_count()
        self.validation_results['broker_count'] = {
            'status': 'PASSED', 
            'message': f'{broker_count} brokers available'
        }
        
        if not self.validate_replication_factor(rf, broker_count):
            self.generate_precheck_report()
            return False

        # Step 6: Check cluster health
        self.check_under_replicated_partitions()

        # Step 7: Check disk space with OpenTSDB
        self.check_disk_space()

        # Generate final report
        self.generate_precheck_report()

        # Return True only if no failures
        return sum(1 for v in self.validation_results.values() if v['status'] == 'FAILED') == 0

    def generate_precheck_report(self) -> None:
        """Generate a summary report of all validation checks."""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PRE-CHECK VALIDATION REPORT")
        self.logger.info("="*80)
        
        # Count status types
        passed = sum(1 for v in self.validation_results.values() if v['status'] == 'PASSED')
        failed = sum(1 for v in self.validation_results.values() if v['status'] == 'FAILED')
        warnings = sum(1 for v in self.validation_results.values() if v['status'] == 'WARNING')
        
        # Display each check result
        for check_name, result in self.validation_results.items():
            status_symbol = {
                'PASSED': '✓',
                'FAILED': '✗',
                'WARNING': '⚠',
                'ERROR': '!'
            }.get(result['status'], '?')
            
            self.logger.info(f"{status_symbol} {check_name.replace('_', ' ').title()}: {result['status']}")
            if result['message']:
                self.logger.info(f"  └─ {result['message']}")
        
        self.logger.info("")
        self.logger.info(f"Summary: {passed} PASSED, {failed} FAILED, {warnings} WARNINGS")
        
        if failed > 0:
            self.logger.info("")
            self.logger.error("RESULT: PRE-CHECKS FAILED - DO NOT PROCEED WITH TOPIC CREATION")
            self.logger.info("")
            self.logger.info("To create the topic after resolving issues:")
            self.logger.info(f"  python3 {sys.argv[0]} {self.config_file}")
        elif warnings > 0:
            self.logger.info("")
            self.logger.warning("RESULT: PRE-CHECKS PASSED WITH WARNINGS - PROCEED WITH CAUTION")
            self.logger.info("")
            self.logger.info("To create the topic:")
            self.logger.info(f"  python3 {sys.argv[0]} {self.config_file}")
        else:
            self.logger.info("")
            self.logger.info("RESULT: ALL PRE-CHECKS PASSED - SAFE TO PROCEED WITH TOPIC CREATION")
            self.logger.info("")
            self.logger.info("To create the topic:")
            self.logger.info(f"  python3 {sys.argv[0]} {self.config_file}")
        
        self.logger.info("="*80)

    def create_topic(self) -> bool:
        """Create the Kafka topic."""
        topic = self.config['topic']['name']
        partitions = self.config['topic']['partitions']
        rf = self.config['topic']['replication_factor']

        self.logger.info(f"Creating topic '{topic}' (partitions={partitions}, RF={rf})")

        cmd = [
            self.kafka_topics_cmd,
            '--bootstrap-server', self.bootstrap_servers,
            '--create',
            '--topic', topic,
            '--partitions', str(partitions),
            '--replication-factor', str(rf)
        ]

        # Add topic configs
        if 'config' in self.config and self.config['config']:
            for key, value in self.config['config'].items():
                cmd.extend(['--config', f"{key}={value}"])
                self.logger.debug(f"Config: {key}={value}")

        success, stdout, stderr = self._run_cmd(cmd)

        if not success:
            if 'already exists' in stderr.lower():
                self.logger.error("Topic already exists")
            else:
                self.logger.error(f"Creation failed: {stderr}")
            return False

        self.logger.info(f"Topic '{topic}' created successfully")
        return True

    def verify_topic(self, topic: str) -> bool:
        """Verify topic was created correctly."""
        self.logger.info(f"Verifying topic '{topic}'")
        time.sleep(2)  # Wait for metadata propagation

        cmd = [self.kafka_topics_cmd, '--bootstrap-server', self.bootstrap_servers,
               '--describe', '--topic', topic]
        success, stdout, stderr = self._run_cmd(cmd)

        if not success:
            self.logger.error(f"Verification failed: {stderr}")
            return False

        # Verify partition count
        expected_partitions = self.config['topic']['partitions']
        actual_partitions = len([l for l in stdout.split('\n') if 'Partition:' in l])

        if actual_partitions != expected_partitions:
            self.logger.error(f"Partition mismatch: expected {expected_partitions}, got {actual_partitions}")
            return False

        # Verify replication factor
        expected_rf = self.config['topic']['replication_factor']
        for line in stdout.split('\n'):
            if 'Replicas:' in line:
                replicas = line.split('Replicas:')[1].split()[0]
                actual_rf = len(replicas.split(','))
                if actual_rf != expected_rf:
                    self.logger.error(f"RF mismatch: expected {expected_rf}, got {actual_rf}")
                    return False
                break

        self.logger.info(f"Verification passed: {actual_partitions} partitions, RF={expected_rf}")
        return True

    def run(self) -> bool:
        """Execute the complete workflow based on mode."""
        try:
            # If precheck-only mode, run prechecks and exit
            if self.precheck_only:
                return self.run_prechecks()
            
            # Otherwise, run the full creation workflow
            self.logger.info("="*80)
            self.logger.info("Kafka Topic Creation Workflow")
            self.logger.info("="*80)

            # Step 1: Run pre-checks
            self.logger.info("")
            self.logger.info("Phase 1: Running pre-checks...")
            self.logger.info("-" * 40)
            if not self.run_prechecks():
                self.logger.error("")
                self.logger.error("Pre-checks failed. Aborting topic creation.")
                self.logger.info("")
                self.logger.info("To run only pre-checks:")
                self.logger.info(f"  python3 {sys.argv[0]} {self.config_file} --precheck")
                return False

            # Check if there were only warnings
            has_warnings = sum(1 for v in self.validation_results.values() if v['status'] == 'WARNING') > 0
            if has_warnings:
                self.logger.warning("")
                self.logger.warning("Pre-checks passed with warnings. Proceeding with topic creation...")
                time.sleep(2)  # Give user time to see the warning

            # Step 2: Create topic
            self.logger.info("")
            self.logger.info("Phase 2: Creating topic...")
            self.logger.info("-" * 40)
            
            topic_name = self.config['topic']['name']
            
            if not self.create_topic():
                self.logger.error("Topic creation failed")
                return False

            # Step 3: Verify topic
            self.logger.info("")
            self.logger.info("Phase 3: Verifying topic...")
            self.logger.info("-" * 40)
            
            if not self.verify_topic(topic_name):
                self.logger.error("Topic verification failed")
                return False

            self.logger.info("")
            self.logger.info("="*80)
            self.logger.info(f"SUCCESS: Topic '{topic_name}' created and verified")
            self.logger.info("="*80)
            return True

        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            return False


def main():
    """Main entry point."""
    # Parse command-line arguments
    if len(sys.argv) < 2:
        print("Usage: python kafka_topic_manager.py <config.yaml> [options]")
        print("")
        print("Options:")
        print("  --precheck       Run only pre-checks without creating the topic")
        print("  --log-level LEVEL    Set log level (DEBUG, INFO, WARNING, ERROR)")
        print("")
        print("Examples:")
        print("  # Run only pre-checks")
        print("  python kafka_topic_manager.py topic.yaml --precheck")
        print("")
        print("  # Run pre-checks and create topic (default)")
        print("  python kafka_topic_manager.py topic.yaml")
        print("")
        print("  # Run with debug logging")
        print("  python kafka_topic_manager.py topic.yaml --log-level DEBUG")
        print("")
        print("  # Run only pre-checks with debug logging")
        print("  python kafka_topic_manager.py topic.yaml --precheck --log-level DEBUG")
        print("")
        print("Note: Disk usage metrics are fetched from OpenTSDB for accurate monitoring.")
        sys.exit(1)

    config_file = sys.argv[1]
    
    # Check for --precheck flag
    precheck_only = '--precheck' in sys.argv
    
    # Parse log level
    log_level = "INFO"
    if '--log-level' in sys.argv:
        idx = sys.argv.index('--log-level')
        if idx + 1 < len(sys.argv):
            log_level = sys.argv[idx + 1]

    # Create and run the manager
    manager = KafkaTopicManager(config_file, log_level, precheck_only)
    success = manager.run()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
