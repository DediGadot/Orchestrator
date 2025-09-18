#!/usr/bin/env python3
"""
Distributed Coding Worker Agent

Simple worker that:
1. Pulls tasks from Redis queue
2. Executes them with Agno
3. Reports results
4. Dies if it fails (orchestrator respawns)

No fancy recovery - just fail fast and let orchestrator handle it.
"""

import os
import sys
import json
import time
import re
import argparse
import logging
import signal
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from types import SimpleNamespace

import yaml
import redis
from agno.agent import Agent
from agno.models.openai import OpenAIChat

try:
    from fakeredis import FakeRedis  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for tests
    from fake_redis import SimpleFakeRedis as FakeRedis  # type: ignore

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))


class RedactingFilter(logging.Filter):
    def __init__(self, compiled_patterns):
        super().__init__()
        self.patterns = compiled_patterns

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for pattern in self.patterns:
            message = pattern.sub('***', message)
        record.msg = message
        record.args = ()
        return True


class CodingWorker:
    """Simple coding worker that just does its job"""

    def __init__(
        self,
        worker_id: str,
        task_type: str,
        config_path: str,
        *,
        config: Optional[Dict[str, Any]] = None,
        redis_client: Optional[redis.Redis] = None,
    ):
        self.worker_id = worker_id
        self.task_type = task_type
        self.config = config or self._load_config(config_path)
        self.redis_client = redis_client or self._setup_redis()
        self.running = False
        self.current_task_id: Optional[str] = None
        self.last_task_time = time.time()
        self.pid = os.getpid()

        queues = self.config['redis']['queues']
        self.task_queue_key = queues['task_queue']
        self.task_type_prefix = queues.get('task_type_prefix', 'tasks:type')
        self.task_feature_prefix = queues.get('task_feature_prefix', 'tasks:feature')
        self.type_queue_key = f"{self.task_type_prefix}:{self.task_type}"
        self.result_queue_key = queues['result_queue']
        self.heartbeat_queue_key = queues['heartbeat_queue']
        self.assignment_queue_key = queues.get('assignment_queue', 'task_assignments')
        self.message_ttl = self.config['redis'].get('message_ttl')

        security_cfg = self.config.get('security', {})
        patterns = security_cfg.get('redact_patterns', [])
        self.redact_patterns = [re.compile(pattern) for pattern in patterns]
        self.sandbox_enabled = security_cfg.get('enable_sandbox', False)
        self.sandbox_root = security_cfg.get('sandbox_root', '.')

        # Setup logging
        self.logger = self._setup_logging()

        # Create Agno agent based on task type
        self.agent = self._create_agent()

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        self.logger.info(f"Worker {worker_id} initialized for {task_type} tasks")

    def _load_config(self, path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"FATAL: Cannot load config from {path}: {e}")
            sys.exit(1)

    def _setup_redis(self) -> redis.Redis:
        """Setup Redis connection"""
        redis_config = self.config['redis']

        if self.config.get('development', {}).get('test_mode'):
            if FakeRedis is None:
                print("FATAL: test_mode enabled but fakeredis is not installed")
                sys.exit(1)
            return FakeRedis(decode_responses=True)

        try:
            client = redis.Redis(
                host=redis_config['host'],
                port=redis_config['port'],
                db=redis_config['db'],
                password=redis_config.get('password'),
                socket_timeout=redis_config['socket_timeout'],
                decode_responses=True
            )
            client.ping()
            return client
        except Exception as e:
            print(f"FATAL: Cannot connect to Redis: {e}")
            sys.exit(1)

    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config['logging']

        # Create logs directory
        os.makedirs("./logs", exist_ok=True)

        logger = logging.getLogger(f"worker.{self.worker_id}")
        logger.setLevel(getattr(logging, self.config['system']['log_level']))
        logger.propagate = False

        if not logger.handlers:
            formatter = logging.Formatter(
                fmt=f"[{self.worker_id}] " + log_config['format'],
                datefmt=log_config['date_format'],
            )

            file_handler = logging.FileHandler(log_config['files']['workers'])
            file_handler.setFormatter(formatter)

            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)

            if self.redact_patterns:
                redactor = RedactingFilter(self.redact_patterns)
                file_handler.addFilter(redactor)
                stream_handler.addFilter(redactor)

            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)

        return logger

    def _create_agent(self) -> Agent:
        """Create Agno agent based on task type"""
        agno_config = self.config['agno']
        type_overrides = self.config.get('task_types', {}).get(self.task_type, {})
        model_id = type_overrides.get('model', agno_config['model'])
        max_tokens = type_overrides.get('max_tokens', agno_config['max_tokens'])
        temperature = type_overrides.get('temperature', agno_config['temperature'])

        # Base agent configuration
        agent_config = {
            "name": f"{self.task_type}_agent",
            "model": OpenAIChat(
                id=model_id,
                max_tokens=max_tokens,
                temperature=temperature
            ),
            "role": f"Expert {self.task_type} developer",
            "markdown": True,
        }

        # Task-specific instructions
        if self.task_type == "frontend":
            agent_config["instructions"] = [
                "You are a frontend developer specializing in React, Vue, and modern JavaScript.",
                "Write clean, maintainable code following best practices.",
                "Focus on user experience and accessibility.",
                "Include proper error handling and loading states.",
                "Use modern CSS and responsive design principles."
            ]

        elif self.task_type == "backend":
            agent_config["instructions"] = [
                "You are a backend developer specializing in APIs and server-side logic.",
                "Write secure, scalable code following REST/GraphQL best practices.",
                "Include proper error handling, validation, and logging.",
                "Consider database design and performance implications.",
                "Follow security best practices for authentication and data handling."
            ]

        elif self.task_type == "test":
            agent_config["instructions"] = [
                "You are a test automation expert specializing in comprehensive testing.",
                "Write unit tests, integration tests, and end-to-end tests as appropriate.",
                "Focus on edge cases and error conditions.",
                "Ensure good test coverage and maintainable test code.",
                "Include both positive and negative test cases."
            ]

        elif self.task_type == "docs":
            agent_config["instructions"] = [
                "You are a technical writer specializing in developer documentation.",
                "Write clear, comprehensive documentation with examples.",
                "Include API documentation, setup instructions, and usage examples.",
                "Focus on helping developers understand and use the code effectively.",
                "Use proper markdown formatting and structure."
            ]

        else:
            # Default instructions
            agent_config["instructions"] = [
                f"You are a {self.task_type} specialist.",
                "Write high-quality, maintainable code following best practices.",
                "Include proper error handling and documentation.",
                "Focus on solving the specific task requirements."
            ]

        if self.config.get('development', {}).get('mock_llm'):
            def run(prompt: str):
                snippet = prompt.strip().splitlines()[0] if prompt else ""
                return SimpleNamespace(content=f"[mock-{self.task_type}] {snippet}")

            return SimpleNamespace(run=run)

        return Agent(**agent_config)

    def send_heartbeat(self):
        """Send heartbeat to orchestrator"""
        try:
            heartbeat_data = {
                "worker_id": self.worker_id,
                "timestamp": datetime.now().isoformat(),
                "status": "working" if self.current_task_id else "idle",
                "task_type": self.task_type,
                "current_task_id": self.current_task_id,
                "pid": self.pid,
            }

            self.redis_client.lpush(self.heartbeat_queue_key, json.dumps(heartbeat_data))
            if self.message_ttl:
                self.redis_client.expire(self.heartbeat_queue_key, self.message_ttl)
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat: {e}")

    def get_task(self) -> Optional[Dict[str, Any]]:
        """Get next task from queue"""
        try:
            # Use blocking pop with timeout
            result = self.redis_client.blpop(
                [self.type_queue_key, self.task_queue_key],
                timeout=self.config['worker']['heartbeat_interval']
            )

            if result:
                queue_name, task_json = result
                try:
                    task_data = json.loads(task_json)
                except json.JSONDecodeError:
                    self.logger.error(f"Invalid task payload: {task_json}")
                    return None

                # Only process tasks for our type
                if task_data.get('type') == self.task_type:
                    assignment = {
                        "worker_id": self.worker_id,
                        "task_id": task_data.get('id'),
                        "task_type": self.task_type,
                        "feature_id": task_data.get('feature_id'),
                        "timestamp": datetime.now().isoformat(),
                        "pid": self.pid,
                    }
                    self.redis_client.lpush(self.assignment_queue_key, json.dumps(assignment))
                    if self.message_ttl:
                        self.redis_client.expire(self.assignment_queue_key, self.message_ttl)

                    self.current_task_id = task_data.get('id')
                    self.last_task_time = time.time()
                    return task_data

                # Put back in queue if wrong type
                self.redis_client.rpush(queue_name, task_json)
                if self.message_ttl:
                    self.redis_client.expire(queue_name, self.message_ttl)
                return None

            return None

        except Exception as e:
            self.logger.error(f"Failed to get task: {e}")
            return None

    def execute_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute task using Agno agent"""
        task_id = task['id']
        prompt = task['prompt']

        self.logger.info(f"Executing task {task_id}")

        try:
            # Execute the task with the agent
            result = self.agent.run(prompt)

            if result and hasattr(result, 'content'):
                output = result.content
            else:
                output = str(result)

            self.logger.info(f"Task {task_id} completed successfully")

            return {
                "task_id": task_id,
                "status": "completed",
                "output": output,
                "worker_id": self.worker_id,
                "completed_at": datetime.now().isoformat(),
                "task_type": self.task_type,
                "pid": self.pid,
            }

        except Exception as e:
            self.logger.error(f"Task {task_id} failed: {e}")

            return {
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "worker_id": self.worker_id,
                "completed_at": datetime.now().isoformat(),
                "task_type": self.task_type,
                "pid": self.pid,
            }

    def report_result(self, result: Dict[str, Any]):
        """Report task result back to orchestrator"""
        try:
            self.redis_client.lpush(self.result_queue_key, json.dumps(result))
            if self.message_ttl:
                self.redis_client.expire(self.result_queue_key, self.message_ttl)

            self.last_task_time = time.time()
            self.current_task_id = None

            self.logger.info(f"Reported result for task {result['task_id']}")

        except Exception as e:
            self.logger.error(f"Failed to report result: {e}")
            # This is serious - if we can't report results, we should die
            raise

    def run(self):
        """Main worker loop"""
        self.running = True
        self.logger.info(f"Worker {self.worker_id} starting main loop")

        self.send_heartbeat()
        last_heartbeat_sent = time.time()

        if self.sandbox_enabled:
            os.environ.setdefault('SANDBOX_MODE', '1')

        try:
            while self.running:
                current_time = time.time()

                # Send heartbeat periodically
                if current_time - last_heartbeat_sent >= self.config['worker']['heartbeat_interval']:
                    self.send_heartbeat()
                    last_heartbeat_sent = current_time

                # Get and execute task
                task = self.get_task()
                if task:
                    result = self.execute_task(task)
                    self.report_result(result)

                    # If task failed, consider dying (fail fast)
                    if result['status'] == 'failed':
                        self.logger.error("Task failed - worker will exit")
                        self.running = False
                        break

                # Check if we should shut down due to idle timeout
                if self.config['worker'].get('idle_timeout'):
                    if (
                        self.current_task_id is None
                        and current_time - self.last_task_time > self.config['worker']['idle_timeout']
                    ):
                        self.logger.info("Idle timeout reached - worker shutting down")
                        self.running = False
                        break

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Worker error: {e}")
            # Die on any unexpected error - orchestrator will respawn
            sys.exit(1)
        finally:
            self.running = False
            self.redis_client.close()
            self.logger.info(f"Worker {self.worker_id} stopped")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}")
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="Distributed Coding Worker Agent")
    parser.add_argument("--worker-id", required=True, help="Unique worker ID")
    parser.add_argument("--task-type", required=True, help="Type of tasks to handle")
    parser.add_argument("--config", default="config.yaml", help="Config file path")

    args = parser.parse_args()

    # Validate task type
    valid_types = ["frontend", "backend", "test", "docs"]
    if args.task_type not in valid_types:
        print(f"FATAL: Invalid task type '{args.task_type}'. Must be one of: {valid_types}")
        sys.exit(1)

    worker = CodingWorker(args.worker_id, args.task_type, args.config)
    worker.run()


if __name__ == "__main__":
    main()
