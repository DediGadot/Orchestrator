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
import argparse
import logging
import signal
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import yaml
import redis
from agno.agent import Agent
from agno.models.openai import OpenAIChat

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))


class CodingWorker:
    """Simple coding worker that just does its job"""

    def __init__(self, worker_id: str, task_type: str, config_path: str):
        self.worker_id = worker_id
        self.task_type = task_type
        self.config = self._load_config(config_path)
        self.redis_client = self._setup_redis()
        self.running = False

        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(f"worker.{worker_id}")

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

        # Configure logger
        logging.basicConfig(
            level=getattr(logging, self.config['system']['log_level']),
            format=f"[{self.worker_id}] " + log_config['format'],
            datefmt=log_config['date_format'],
            handlers=[
                logging.FileHandler(log_config['files']['workers']),
                logging.StreamHandler(sys.stdout)
            ]
        )

    def _create_agent(self) -> Agent:
        """Create Agno agent based on task type"""
        agno_config = self.config['agno']

        # Base agent configuration
        agent_config = {
            "name": f"{self.task_type}_agent",
            "model": OpenAIChat(
                id=agno_config['model'],
                max_tokens=agno_config['max_tokens'],
                temperature=agno_config['temperature']
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

        return Agent(**agent_config)

    def send_heartbeat(self):
        """Send heartbeat to orchestrator"""
        try:
            heartbeat_data = {
                "worker_id": self.worker_id,
                "timestamp": datetime.now().isoformat(),
                "status": "alive"
            }

            self.redis_client.lpush(
                self.config['redis']['queues']['heartbeat_queue'],
                json.dumps(heartbeat_data)
            )
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat: {e}")

    def get_task(self) -> Optional[Dict[str, Any]]:
        """Get next task from queue"""
        try:
            # Use blocking pop with timeout
            result = self.redis_client.blpop(
                self.config['redis']['queues']['task_queue'],
                timeout=self.config['worker']['heartbeat_interval']
            )

            if result:
                queue_name, task_json = result
                task_data = json.loads(task_json)

                # Only process tasks for our type
                if task_data.get('type') == self.task_type:
                    return task_data
                else:
                    # Put back in queue if wrong type
                    self.redis_client.lpush(
                        self.config['redis']['queues']['task_queue'],
                        task_json
                    )
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
                "completed_at": datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Task {task_id} failed: {e}")

            return {
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "worker_id": self.worker_id,
                "completed_at": datetime.now().isoformat()
            }

    def report_result(self, result: Dict[str, Any]):
        """Report task result back to orchestrator"""
        try:
            self.redis_client.lpush(
                self.config['redis']['queues']['result_queue'],
                json.dumps(result)
            )

            self.logger.info(f"Reported result for task {result['task_id']}")

        except Exception as e:
            self.logger.error(f"Failed to report result: {e}")
            # This is serious - if we can't report results, we should die
            raise

    def run(self):
        """Main worker loop"""
        self.running = True
        self.logger.info(f"Worker {self.worker_id} starting main loop")

        last_heartbeat = time.time()

        try:
            while self.running:
                current_time = time.time()

                # Send heartbeat periodically
                if current_time - last_heartbeat >= self.config['worker']['heartbeat_interval']:
                    self.send_heartbeat()
                    last_heartbeat = current_time

                # Get and execute task
                task = self.get_task()
                if task:
                    result = self.execute_task(task)
                    self.report_result(result)

                    # If task failed, consider dying (fail fast)
                    if result['status'] == 'failed':
                        self.logger.error("Task failed - worker will exit")
                        break

                # Check if we should shut down due to idle timeout
                if self.config['worker'].get('idle_timeout'):
                    if current_time - last_heartbeat > self.config['worker']['idle_timeout']:
                        self.logger.info("Idle timeout reached - worker shutting down")
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