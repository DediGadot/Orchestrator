#!/usr/bin/env python3
"""
Distributed Coding Agent Orchestrator

The brain that controls the swarm. Does three things:
1. Takes feature requests and breaks them into tasks
2. Spawns workers to handle tasks
3. Monitors workers and respawns when they die

No magic, no bullshit. Just works.
"""

import os
import sys
import time
import json
import sqlite3
import logging
import argparse
import subprocess
import signal
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path

import yaml
import redis
import psutil

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))


@dataclass
class Task:
    """Single coding task"""
    id: str
    type: str  # frontend, backend, test, docs
    prompt: str
    status: str  # pending, running, completed, failed
    worker_id: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None
    retry_count: int = 0
    output: Optional[str] = None
    error: Optional[str] = None


@dataclass
class Worker:
    """Single worker agent"""
    id: str
    pid: int
    task_type: str
    status: str  # spawning, idle, working, dead
    last_heartbeat: Optional[str] = None
    current_task_id: Optional[str] = None
    failure_count: int = 0


class SimpleOrchestrator:
    """Dead simple orchestrator that just works"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.redis_client = self._setup_redis()
        self.db = self._setup_database()
        self.workers: Dict[str, Worker] = {}
        self.running = False

        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger("orchestrator")

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        self.logger.info("Orchestrator initialized")

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
            # Test connection
            client.ping()
            return client
        except Exception as e:
            print(f"FATAL: Cannot connect to Redis: {e}")
            sys.exit(1)

    def _setup_database(self) -> sqlite3.Connection:
        """Setup SQLite database"""
        db_path = self.config['storage']['path']

        # Create directory if needed
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = sqlite3.connect(db_path, check_same_thread=False)

        # Set pragmas from config
        for pragma, value in self.config['storage']['pragma'].items():
            conn.execute(f"PRAGMA {pragma}={value}")

        # Create tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                worker_id TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                retry_count INTEGER DEFAULT 0,
                output TEXT,
                error TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'spawning',
                last_heartbeat TEXT,
                current_task_id TEXT,
                failure_count INTEGER DEFAULT 0
            )
        """)

        conn.commit()
        return conn

    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config['logging']

        # Create logs directory
        os.makedirs("./logs", exist_ok=True)

        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, self.config['system']['log_level']),
            format=log_config['format'],
            datefmt=log_config['date_format'],
            handlers=[
                logging.FileHandler(log_config['files']['orchestrator']),
                logging.StreamHandler(sys.stdout)
            ]
        )

    def decompose_feature(self, feature_request: str) -> List[Task]:
        """
        Break feature request into tasks

        Dead simple approach: Look for keywords and create tasks.
        No AI needed here - just regex and common sense.
        """
        tasks = []
        now = datetime.now().isoformat()

        # Simple keyword-based task decomposition
        task_id_base = f"task_{int(time.time())}"

        # Always create a backend task if it's a feature
        if any(word in feature_request.lower() for word in ['api', 'backend', 'database', 'service']):
            tasks.append(Task(
                id=f"{task_id_base}_backend",
                type="backend",
                prompt=f"Implement backend functionality: {feature_request}",
                status="pending",
                created_at=now
            ))

        # Frontend task if mentioned
        if any(word in feature_request.lower() for word in ['ui', 'frontend', 'interface', 'component']):
            tasks.append(Task(
                id=f"{task_id_base}_frontend",
                type="frontend",
                prompt=f"Implement frontend functionality: {feature_request}",
                status="pending",
                created_at=now
            ))

        # Always add tests
        tasks.append(Task(
            id=f"{task_id_base}_test",
            type="test",
            prompt=f"Write tests for: {feature_request}",
            status="pending",
            created_at=now
        ))

        # Add docs if it's a significant feature
        if len(feature_request) > 50:  # Arbitrary but reasonable threshold
            tasks.append(Task(
                id=f"{task_id_base}_docs",
                type="docs",
                prompt=f"Write documentation for: {feature_request}",
                status="pending",
                created_at=now
            ))

        self.logger.info(f"Decomposed feature into {len(tasks)} tasks")
        return tasks

    def store_tasks(self, tasks: List[Task]):
        """Store tasks in database and queue"""
        for task in tasks:
            # Store in database
            self.db.execute("""
                INSERT INTO tasks (id, type, prompt, status, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (task.id, task.type, task.prompt, task.status, task.created_at))

            # Add to Redis queue
            task_data = asdict(task)
            self.redis_client.lpush(
                self.config['redis']['queues']['task_queue'],
                json.dumps(task_data)
            )

        self.db.commit()
        self.logger.info(f"Stored {len(tasks)} tasks")

    def spawn_worker(self, task_type: str) -> Worker:
        """Spawn a new worker process"""
        worker_id = f"worker_{task_type}_{int(time.time())}"

        # Start worker process
        cmd = [
            sys.executable,
            "workers/agent.py",
            "--worker-id", worker_id,
            "--task-type", task_type,
            "--config", "config.yaml"
        ]

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(Path(__file__).parent.parent)
            )

            worker = Worker(
                id=worker_id,
                pid=proc.pid,
                task_type=task_type,
                status="spawning"
            )

            # Store in database
            self.db.execute("""
                INSERT INTO workers (id, pid, task_type, status)
                VALUES (?, ?, ?, ?)
            """, (worker.id, worker.pid, worker.task_type, worker.status))
            self.db.commit()

            self.workers[worker_id] = worker
            self.logger.info(f"Spawned worker {worker_id} (PID: {proc.pid})")

            return worker

        except Exception as e:
            self.logger.error(f"Failed to spawn worker: {e}")
            raise

    def check_worker_health(self):
        """Check if workers are alive and responsive"""
        dead_workers = []

        for worker_id, worker in self.workers.items():
            try:
                # Check if process is alive
                proc = psutil.Process(worker.pid)
                if not proc.is_running():
                    self.logger.warning(f"Worker {worker_id} process is dead")
                    dead_workers.append(worker_id)
                    continue

                # Check heartbeat (if we have one)
                if worker.last_heartbeat:
                    last_heartbeat = datetime.fromisoformat(worker.last_heartbeat)
                    timeout = timedelta(seconds=self.config['orchestrator']['health_check_interval'] * 3)

                    if datetime.now() - last_heartbeat > timeout:
                        self.logger.warning(f"Worker {worker_id} heartbeat timeout")
                        dead_workers.append(worker_id)
                        continue

            except psutil.NoSuchProcess:
                self.logger.warning(f"Worker {worker_id} process not found")
                dead_workers.append(worker_id)

        # Clean up dead workers and spawn replacements
        for worker_id in dead_workers:
            self._handle_dead_worker(worker_id)

    def _handle_dead_worker(self, worker_id: str):
        """Handle a dead worker - clean up and potentially spawn replacement"""
        if worker_id not in self.workers:
            return

        worker = self.workers[worker_id]
        self.logger.info(f"Handling dead worker {worker_id}")

        # Kill process if still alive
        try:
            proc = psutil.Process(worker.pid)
            proc.terminate()
            proc.wait(timeout=5)
        except:
            pass

        # Update database
        self.db.execute("""
            UPDATE workers SET status = 'dead' WHERE id = ?
        """, (worker_id,))

        # If worker was handling a task, put it back in queue
        if worker.current_task_id:
            self.db.execute("""
                UPDATE tasks SET status = 'pending', worker_id = NULL
                WHERE id = ?
            """, (worker.current_task_id,))

            # Put task back in Redis queue
            task_row = self.db.execute("""
                SELECT * FROM tasks WHERE id = ?
            """, (worker.current_task_id,)).fetchone()

            if task_row:
                task_data = {
                    'id': task_row[0],
                    'type': task_row[1],
                    'prompt': task_row[2],
                    'status': 'pending'
                }
                self.redis_client.lpush(
                    self.config['redis']['queues']['task_queue'],
                    json.dumps(task_data)
                )

        self.db.commit()

        # Remove from active workers
        del self.workers[worker_id]

        # Spawn replacement if needed
        queue_size = self.redis_client.llen(self.config['redis']['queues']['task_queue'])
        active_workers = len([w for w in self.workers.values() if w.status in ['idle', 'working']])

        if queue_size > 0 and active_workers < self.config['orchestrator']['max_workers']:
            time.sleep(self.config['orchestrator']['replacement_delay'])
            self.spawn_worker(worker.task_type)

    def start_feature(self, feature_request: str, num_workers: int = None) -> Dict[str, Any]:
        """Start working on a feature"""
        if num_workers is None:
            num_workers = self.config['orchestrator']['min_workers']

        self.logger.info(f"Starting feature: {feature_request}")

        # Decompose feature into tasks
        tasks = self.decompose_feature(feature_request)

        # Store tasks
        self.store_tasks(tasks)

        # Spawn initial workers
        task_types = list(set(task.type for task in tasks))
        workers_spawned = []

        for task_type in task_types:
            # Spawn at least one worker per task type
            worker = self.spawn_worker(task_type)
            workers_spawned.append(worker.id)

        # Spawn additional workers up to the limit
        remaining_workers = max(0, num_workers - len(workers_spawned))
        for i in range(remaining_workers):
            task_type = task_types[i % len(task_types)]  # Round-robin
            worker = self.spawn_worker(task_type)
            workers_spawned.append(worker.id)

        return {
            "status": "started",
            "tasks_created": len(tasks),
            "workers_spawned": len(workers_spawned),
            "worker_ids": workers_spawned
        }

    def get_status(self) -> Dict[str, Any]:
        """Get current system status"""
        # Get task counts
        task_counts = {}
        for status in ['pending', 'running', 'completed', 'failed']:
            count = self.db.execute("""
                SELECT COUNT(*) FROM tasks WHERE status = ?
            """, (status,)).fetchone()[0]
            task_counts[status] = count

        # Get worker counts
        worker_counts = {}
        for status in ['spawning', 'idle', 'working', 'dead']:
            worker_counts[status] = len([w for w in self.workers.values() if w.status == status])

        # Get queue sizes
        queue_sizes = {}
        for queue_name, queue_key in self.config['redis']['queues'].items():
            queue_sizes[queue_name] = self.redis_client.llen(queue_key)

        return {
            "orchestrator_status": "running" if self.running else "stopped",
            "tasks": task_counts,
            "workers": worker_counts,
            "queues": queue_sizes,
            "active_workers": list(self.workers.keys())
        }

    def kill_all(self) -> Dict[str, Any]:
        """Kill all workers and stop orchestrator"""
        self.logger.info("Killing all workers")

        killed_count = 0
        for worker_id, worker in list(self.workers.items()):
            try:
                proc = psutil.Process(worker.pid)
                proc.terminate()
                proc.wait(timeout=5)
                killed_count += 1
            except:
                pass

            # Update database
            self.db.execute("""
                UPDATE workers SET status = 'dead' WHERE id = ?
            """, (worker_id,))

        self.db.commit()
        self.workers.clear()
        self.running = False

        return {
            "status": "stopped",
            "workers_killed": killed_count
        }

    def run(self):
        """Main orchestrator loop"""
        self.running = True
        self.logger.info("Orchestrator starting main loop")

        try:
            while self.running:
                # Check worker health
                self.check_worker_health()

                # Sleep for health check interval
                time.sleep(self.config['orchestrator']['health_check_interval'])

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        finally:
            self.kill_all()
            self.db.close()
            self.redis_client.close()
            self.logger.info("Orchestrator stopped")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}")
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="Distributed Coding Agent Orchestrator")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--feature", help="Feature to implement")
    parser.add_argument("--workers", type=int, help="Number of workers to spawn")

    args = parser.parse_args()

    orchestrator = SimpleOrchestrator(args.config)

    if args.feature:
        # Start feature and then run
        result = orchestrator.start_feature(args.feature, args.workers)
        print(f"Started feature implementation: {result}")
        orchestrator.run()
    else:
        # Just run the orchestrator
        orchestrator.run()


if __name__ == "__main__":
    main()