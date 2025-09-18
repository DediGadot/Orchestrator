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
from dataclasses import dataclass, field, asdict, replace
from pathlib import Path

import yaml
import redis
import psutil

try:
    from fakeredis import FakeRedis  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for tests
    from fake_redis import SimpleFakeRedis as FakeRedis  # type: ignore

try:  # pragma: no cover - allow module execution as script
    from .planner import NaiveDecomposition
except ImportError:  # pragma: no cover
    from planner import NaiveDecomposition  # type: ignore

try:  # pragma: no cover - allow module execution as script
    from .merger import GitMerger
except ImportError:  # pragma: no cover
    from merger import GitMerger  # type: ignore

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))


@dataclass
class Task:
    """Single coding task with explicit contract metadata"""

    id: str
    feature_id: str
    type: str  # frontend, backend, test, docs
    prompt: str
    status: str  # pending, running, completed, failed
    worker_id: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None
    retry_count: int = 0
    output: Optional[str] = None
    error: Optional[str] = None
    inputs: List[str] = field(default_factory=list)
    artifacts: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    budget_tokens: int = 0
    priority: int = 5
    acceptance_tests: List[str] = field(default_factory=list)
    cost_tokens: int = 0
    console_logs: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


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

    def __init__(
        self,
        config_path: str = "config.yaml",
        *,
        config: Optional[Dict[str, Any]] = None,
        redis_client: Optional[redis.Redis] = None,
        db_connection: Optional[sqlite3.Connection] = None,
    ):
        self.config = config or self._load_config(config_path)
        self.redis_client = redis_client or self._setup_redis()
        self.db = db_connection or self._setup_database()
        self.task_started_at: Dict[str, datetime] = {}
        self.workers: Dict[str, Worker] = {}
        self.running = False

        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger("orchestrator")

        # Cache queue keys
        queues = self.config['redis']['queues']
        self.task_queue_key = queues['task_queue']
        self.result_queue_key = queues['result_queue']
        self.heartbeat_queue_key = queues['heartbeat_queue']
        self.failure_queue_key = queues['failure_queue']
        self.task_type_prefix = queues.get('task_type_prefix', 'tasks:type')
        self.task_feature_prefix = queues.get('task_feature_prefix', 'tasks:feature')
        self.assignment_queue_key = queues.get('assignment_queue', 'task_assignments')
        self.event_queue_key = queues.get('event_queue', 'system_events')

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        # Ensure database schema and load previous state
        self._initialize_database()
        self._load_existing_workers()

        # Strategy components and conflict tracking
        self.decomposer = NaiveDecomposition(self)
        self.active_artifacts: Dict[str, str] = {}
        repo_root = Path(self.config.get('repository', {}).get('path', Path.cwd()))
        self.merger = GitMerger(repo_root)

        budgets_cfg = self.config.get('budgets', {})
        self.default_budget = budgets_cfg.get('default_tokens', 0)
        self.type_budget = budgets_cfg.get('per_task_type', {})
        self.feature_budgets: Dict[str, int] = {}

        rate_cfg = self.config.get('rate_limits', {})
        self.rate_limit_window = rate_cfg.get('window_seconds', 60)
        self.rate_limit_max = rate_cfg.get('max_requests', 0)
        self.rate_counters: Dict[str, List[float]] = {}

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
            # Test connection
            client.ping()
            return client
        except Exception as e:
            print(f"FATAL: Cannot connect to Redis: {e}")
            sys.exit(1)

    def _setup_database(self) -> sqlite3.Connection:
        """Setup SQLite database connection"""
        db_path = self.config['storage']['path']

        # Create directory if needed
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # Set pragmas from config
        for pragma, value in self.config['storage']['pragma'].items():
            conn.execute(f"PRAGMA {pragma}={value}")

        return conn

    def _initialize_database(self):
        """Ensure required tables and columns exist"""
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                feature_id TEXT NOT NULL,
                type TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                worker_id TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                retry_count INTEGER DEFAULT 0,
                output TEXT,
                error TEXT,
                inputs TEXT,
                artifacts TEXT,
                dependencies TEXT,
                budget_tokens INTEGER DEFAULT 0,
                priority INTEGER DEFAULT 5,
                acceptance_tests TEXT,
                cost_tokens INTEGER DEFAULT 0,
                console_logs TEXT,
                metrics TEXT
            )
        """
        )

        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'spawning',
                last_heartbeat TEXT,
                current_task_id TEXT,
                failure_count INTEGER DEFAULT 0
            )
        """
        )

        self.db.commit()

        # Ensure new columns exist for upgraded deployments
        required_columns = {
            'tasks': {
                'feature_id': "ALTER TABLE tasks ADD COLUMN feature_id TEXT DEFAULT 'feature_unknown'",
                'inputs': "ALTER TABLE tasks ADD COLUMN inputs TEXT",
                'artifacts': "ALTER TABLE tasks ADD COLUMN artifacts TEXT",
                'dependencies': "ALTER TABLE tasks ADD COLUMN dependencies TEXT",
                'budget_tokens': "ALTER TABLE tasks ADD COLUMN budget_tokens INTEGER DEFAULT 0",
                'priority': "ALTER TABLE tasks ADD COLUMN priority INTEGER DEFAULT 5",
                'acceptance_tests': "ALTER TABLE tasks ADD COLUMN acceptance_tests TEXT",
                'cost_tokens': "ALTER TABLE tasks ADD COLUMN cost_tokens INTEGER DEFAULT 0",
                'console_logs': "ALTER TABLE tasks ADD COLUMN console_logs TEXT",
                'metrics': "ALTER TABLE tasks ADD COLUMN metrics TEXT"
            }
        }

        for table, statements in required_columns.items():
            existing = {
                row[1]
                for row in self.db.execute(f"PRAGMA table_info({table})")
            }
            for column, ddl in statements.items():
                if column not in existing:
                    logging.getLogger("orchestrator.migrations").debug(
                        "Adding missing column %s to %s", column, table
                    )
                    self.db.execute(ddl)

        self.db.commit()

    def _load_existing_workers(self):
        """Load workers from the database into memory"""
        rows = self.db.execute(
            """
            SELECT id, pid, task_type, status, last_heartbeat, current_task_id, failure_count
            FROM workers
            WHERE status != 'dead'
        """
        ).fetchall()

        for row in rows:
            worker = Worker(
                id=row['id'],
                pid=row['pid'],
                task_type=row['task_type'],
                status=row['status'],
                last_heartbeat=row['last_heartbeat'],
                current_task_id=row['current_task_id'],
                failure_count=row['failure_count'] or 0,
            )

            # Drop any workers whose process is already gone
            if not self._process_is_alive(worker.pid):
                self.logger.info(f"Cleaning up stale worker {worker.id}")
                self.db.execute(
                    "UPDATE workers SET status='dead', current_task_id=NULL WHERE id=?",
                    (worker.id,),
                )
                continue

            self.workers[worker.id] = worker

        self.db.commit()

    @staticmethod
    def _process_is_alive(pid: int) -> bool:
        try:
            proc = psutil.Process(pid)
            return proc.is_running() and not proc.status() == psutil.STATUS_ZOMBIE
        except psutil.Error:
            return False

    def _get_or_create_worker(self, worker_id: str, pid: Optional[int], task_type: Optional[str]) -> Worker:
        if worker_id in self.workers:
            worker = self.workers[worker_id]
            if pid and worker.pid != pid:
                worker.pid = pid
                self.db.execute("UPDATE workers SET pid=? WHERE id=?", (pid, worker_id))
            return worker

        row = self.db.execute(
            "SELECT id, pid, task_type, status, last_heartbeat, current_task_id, failure_count FROM workers WHERE id=?",
            (worker_id,),
        ).fetchone()

        if row:
            worker = Worker(
                id=row[0],
                pid=row[1],
                task_type=row[2],
                status=row[3],
                last_heartbeat=row[4],
                current_task_id=row[5],
                failure_count=row[6] or 0,
            )
        else:
            if pid is None:
                pid = -1
            if task_type is None:
                task_type = "unknown"
            worker = Worker(
                id=worker_id,
                pid=pid,
                task_type=task_type,
                status="spawning",
            )
            self.db.execute(
                """
                INSERT INTO workers (id, pid, task_type, status)
                VALUES (?, ?, ?, ?)
                """,
                (worker.id, worker.pid, worker.task_type, worker.status),
            )

        self.workers[worker.id] = worker
        return worker

    def _process_assignments(self):
        processed = False
        ttl = self.config['redis'].get('message_ttl')

        while True:
            payload = self.redis_client.rpop(self.assignment_queue_key)
            if payload is None:
                break

            processed = True
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                self.logger.error(f"Invalid assignment payload: {payload}")
                continue

            worker_id = data.get('worker_id')
            task_id = data.get('task_id')
            task_type = data.get('task_type')
            pid = data.get('pid')
            feature_id = data.get('feature_id', 'feature_unknown')
            timestamp = data.get('timestamp', datetime.now().isoformat())

            if not worker_id or not task_id:
                self.logger.error(f"Assignment missing worker_id or task_id: {data}")
                continue

            task_row = self.db.execute(
                "SELECT * FROM tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            if not task_row:
                self.logger.warning(f"Assignment references unknown task {task_id}")
                continue

            task_obj = self._row_to_task(task_row)

            if not self._dependencies_satisfied(task_obj):
                self.logger.info(f"Deferring {task_id} until dependencies complete")
                self._emit_event(
                    "task.deferred.dependencies",
                    {"task_id": task_id, "waiting_on": task_obj.dependencies},
                )
                self._requeue_task(task_id)
                continue

            if not self._budget_available(task_obj):
                self.logger.info(f"Deferring {task_id} due to exhausted budget")
                self._emit_event(
                    "task.deferred.budget",
                    {
                        "task_id": task_id,
                        "feature_id": task_obj.feature_id,
                        "budget_tokens": task_obj.budget_tokens,
                        "remaining": self.feature_budgets.get(task_obj.feature_id, self.default_budget),
                    },
                )
                self._requeue_task(task_id)
                continue

            if not self._rate_limit_allow(task_type or 'default'):
                self.logger.info(f"Deferring {task_id} due to rate limit")
                self._emit_event(
                    "task.deferred.rate_limit",
                    {
                        "task_id": task_id,
                        "task_type": task_type,
                    },
                )
                self._requeue_task(task_id)
                continue

            conflict = self._has_conflict(task_obj)
            if conflict:
                self.logger.info(f"Deferring {task_id} due to artifact conflict with {conflict}")
                self._emit_event(
                    "task.deferred.conflict",
                    {"task_id": task_id, "conflicts_with": conflict, "artifacts": task_obj.artifacts},
                )
                self._requeue_task(task_id)
                continue

            worker = self._get_or_create_worker(worker_id, pid, task_type)
            worker.status = 'working'
            worker.current_task_id = task_id
            worker.last_heartbeat = timestamp

            self.db.execute(
                """
                UPDATE workers
                SET status='working', current_task_id=?, last_heartbeat=?
                WHERE id=?
                """,
                (task_id, timestamp, worker_id),
            )

            self.db.execute(
                "UPDATE tasks SET status='running', worker_id=?, feature_id=? WHERE id=?",
                (worker_id, feature_id, task_id),
            )

            task_obj.status = 'running'
            task_obj.worker_id = worker_id
            self._purge_task_from_queues(task_obj)
            self._register_task_artifacts(task_obj)

            self.task_started_at[task_id] = datetime.now()
            self._emit_event(
                "task.assigned",
                {
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "task_type": task_type,
                    "feature_id": feature_id,
                },
            )

        if processed and ttl:
            self.redis_client.expire(self.assignment_queue_key, ttl)

        if processed:
            self.db.commit()

    def _requeue_task(self, task_id: str):
        row = self.db.execute(
            "SELECT * FROM tasks WHERE id=?",
            (task_id,),
        ).fetchone()
        if not row:
            self.logger.warning(f"Cannot requeue unknown task {task_id}")
            return

        task = self._row_to_task(row)
        task.status = 'pending'
        task.worker_id = None

        # Remove stale payloads before requeuing
        self._purge_task_from_queues(task)

        task_payload = self._task_payload(task)

        self.redis_client.rpush(self.task_queue_key, task_payload)
        self.redis_client.rpush(self._type_queue(task.type), task_payload)
        self.redis_client.rpush(self._feature_queue(task.feature_id, task.type), task_payload)

        ttl = self.config['redis'].get('message_ttl')
        if ttl:
            self.redis_client.expire(self.task_queue_key, ttl)
            self.redis_client.expire(self._type_queue(task.type), ttl)
            self.redis_client.expire(self._feature_queue(task.feature_id, task.type), ttl)

    def _process_results(self):
        processed = False
        ttl = self.config['redis'].get('message_ttl')

        while True:
            payload = self.redis_client.rpop(self.result_queue_key)
            if payload is None:
                break

            processed = True
            try:
                result = json.loads(payload)
            except json.JSONDecodeError:
                self.logger.error(f"Invalid result payload: {payload}")
                continue

            task_id = result.get('task_id')
            if not task_id:
                self.logger.error(f"Result missing task_id: {result}")
                continue

            worker_id = result.get('worker_id')
            status = result.get('status')
            completed_at = result.get('completed_at', datetime.now().isoformat())
            output = result.get('output')
            error = result.get('error')
            console_logs = result.get('console_logs', [])
            if isinstance(console_logs, str):
                console_logs = [console_logs]
            if not isinstance(console_logs, list):
                console_logs = [str(console_logs)]

            cost_tokens = result.get('cost_tokens')

            task_row = self.db.execute(
                "SELECT * FROM tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            task_obj = self._row_to_task(task_row) if task_row else None
            if task_obj and cost_tokens is None:
                cost_tokens = task_obj.cost_tokens
            if cost_tokens is None:
                cost_tokens = 0
            cost_tokens = int(cost_tokens)
            task_metrics = task_obj.metrics if task_obj else {}

            recycle_worker: Optional[str] = None

            if worker_id:
                worker = self._get_or_create_worker(worker_id, result.get('pid'), result.get('task_type'))
                worker.current_task_id = None
                worker.last_heartbeat = completed_at
                worker.status = 'idle' if status == 'completed' else 'spawning'
                if status == 'completed':
                    worker.failure_count = 0
                else:
                    worker.failure_count += 1

                self.db.execute(
                    """
                    UPDATE workers
                    SET status=?, current_task_id=NULL, last_heartbeat=?, failure_count=?
                    WHERE id=?
                    """,
                    (worker.status, completed_at, worker.failure_count, worker_id),
                )

                if worker.failure_count >= self.config['orchestrator']['failure_threshold']:
                    recycle_worker = worker_id

            retry_limit = self.config['orchestrator']['retry_limit']
            retry_count_row = self.db.execute(
                "SELECT retry_count FROM tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            current_retry_count = retry_count_row['retry_count'] if retry_count_row else 0

            if status == 'completed':
                merge_info: Dict[str, Any] = {}
                validation_info: Dict[str, Any] = {}
                if task_obj:
                    merge_info = self.merger.apply_result(task_obj, result)
                    validation_info = self.merger.validate(task_obj)
                    task_metrics.update({"merge": merge_info, "validation": validation_info})
                    task_obj.metrics = task_metrics
                    task_obj.cost_tokens = cost_tokens

                self.db.execute(
                    """
                    UPDATE tasks
                    SET status='completed', worker_id=?, completed_at=?, output=?, error=NULL,
                        cost_tokens=?, console_logs=?, metrics=?
                    WHERE id=?
                    """,
                    (
                        worker_id,
                        completed_at,
                        output,
                        cost_tokens,
                        json.dumps(console_logs),
                        json.dumps(task_metrics),
                        task_id,
                    ),
                )
                self.task_started_at.pop(task_id, None)
                if task_obj:
                    self._consume_budget(task_obj.feature_id, cost_tokens)
                self._emit_event(
                    "task.completed",
                    {
                        "task_id": task_id,
                        "worker_id": worker_id,
                        "cost_tokens": cost_tokens,
                        "merge": merge_info,
                        "validation": validation_info,
                    },
                )
            else:
                new_retry_count = current_retry_count + 1
                if new_retry_count <= retry_limit:
                    self.logger.warning(f"Task {task_id} failed - retry {new_retry_count}/{retry_limit}")
                    self.db.execute(
                        """
                        UPDATE tasks
                        SET status='pending', worker_id=NULL, completed_at=NULL, error=?, retry_count=?,
                            cost_tokens=?, console_logs=?, metrics=?
                        WHERE id=?
                        """,
                        (
                            error,
                            new_retry_count,
                            cost_tokens,
                            json.dumps(console_logs),
                            json.dumps(task_metrics),
                            task_id,
                        ),
                    )
                    self.task_started_at.pop(task_id, None)
                    if task_obj:
                        self._consume_budget(task_obj.feature_id, cost_tokens)
                    self._requeue_task(task_id)
                    self._emit_event(
                        "task.requeued",
                        {
                            "task_id": task_id,
                            "worker_id": worker_id,
                            "retry": new_retry_count,
                            "error": error,
                        },
                    )
                else:
                    self.logger.error(f"Task {task_id} exhausted retries")
                    self.db.execute(
                        """
                        UPDATE tasks
                        SET status='failed', worker_id=?, completed_at=?, error=?, retry_count=?,
                            cost_tokens=?, console_logs=?, metrics=?
                        WHERE id=?
                        """,
                        (
                            worker_id,
                            completed_at,
                            error,
                            new_retry_count,
                            cost_tokens,
                            json.dumps(console_logs),
                            json.dumps(task_metrics),
                            task_id,
                        ),
                    )
                    self.redis_client.lpush(self.failure_queue_key, json.dumps(result))
                    if ttl:
                        self.redis_client.expire(self.failure_queue_key, ttl)
                    self.task_started_at.pop(task_id, None)
                    if task_obj:
                        self._consume_budget(task_obj.feature_id, cost_tokens)
                    self._emit_event(
                        "task.failed",
                        {
                            "task_id": task_id,
                            "worker_id": worker_id,
                            "error": error,
                            "retry_count": new_retry_count,
                        },
                    )

            self._release_task_artifacts(task_id)

            if recycle_worker:
                self.logger.error(
                    f"Worker {recycle_worker} exceeded failure threshold, recycling"
                )
                self._handle_dead_worker(recycle_worker)

        if processed:
            self.db.commit()

    def _process_heartbeats(self):
        ttl = self.config['redis'].get('message_ttl')
        processed = False

        while True:
            payload = self.redis_client.rpop(self.heartbeat_queue_key)
            if payload is None:
                break

            processed = True
            try:
                heartbeat = json.loads(payload)
            except json.JSONDecodeError:
                self.logger.error(f"Invalid heartbeat payload: {payload}")
                continue

            worker_id = heartbeat.get('worker_id')
            if not worker_id:
                self.logger.error(f"Heartbeat missing worker_id: {heartbeat}")
                continue

            timestamp = heartbeat.get('timestamp', datetime.now().isoformat())
            task_type = heartbeat.get('task_type')
            pid = heartbeat.get('pid')
            current_task_id = heartbeat.get('current_task_id')
            state = heartbeat.get('status', 'idle')

            worker = self._get_or_create_worker(worker_id, pid, task_type)
            worker.last_heartbeat = timestamp
            worker.current_task_id = current_task_id
            worker.status = state

            self.db.execute(
                """
                UPDATE workers
                SET last_heartbeat=?, current_task_id=?, status=?
                WHERE id=?
                """,
                (timestamp, current_task_id, state, worker_id),
            )

        if processed:
            self.db.commit()
            if ttl:
                self.redis_client.expire(self.heartbeat_queue_key, ttl)

    def process_once(self):
        """Process any queued assignments, results, and heartbeats"""
        self._process_assignments()
        self._process_results()
        self._process_heartbeats()

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

    def _type_queue(self, task_type: str) -> str:
        return f"{self.task_type_prefix}:{task_type}"

    def _feature_queue(self, feature_id: str, task_type: str) -> str:
        safe_feature = feature_id.replace(' ', '_')
        return f"{self.task_feature_prefix}:{safe_feature}:{task_type}"

    def _task_to_db_tuple(self, task: Task) -> tuple:
        return (
            task.id,
            task.feature_id,
            task.type,
            task.prompt,
            task.status,
            task.worker_id,
            task.created_at,
            task.completed_at,
            task.retry_count,
            task.output,
            task.error,
            json.dumps(task.inputs),
            json.dumps(task.artifacts),
            json.dumps(task.dependencies),
            task.budget_tokens,
            task.priority,
            json.dumps(task.acceptance_tests),
            task.cost_tokens,
            json.dumps(task.console_logs),
            json.dumps(task.metrics),
        )

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        row_keys = set(row.keys())

        return Task(
            id=row['id'],
            feature_id=row['feature_id'] if 'feature_id' in row_keys else 'feature_unknown',
            type=row['type'] if 'type' in row_keys else 'unknown',
            prompt=row['prompt'] if 'prompt' in row_keys else '',
            status=row['status'] if 'status' in row_keys else 'pending',
            worker_id=row['worker_id'] if 'worker_id' in row_keys else None,
            created_at=row['created_at'] if 'created_at' in row_keys else datetime.now().isoformat(),
            completed_at=row['completed_at'] if 'completed_at' in row_keys else None,
            retry_count=row['retry_count'] if 'retry_count' in row_keys else 0,
            output=row['output'] if 'output' in row_keys else None,
            error=row['error'] if 'error' in row_keys else None,
            inputs=json.loads(row['inputs']) if 'inputs' in row_keys and row['inputs'] else [],
            artifacts=json.loads(row['artifacts']) if 'artifacts' in row_keys and row['artifacts'] else [],
            dependencies=json.loads(row['dependencies']) if 'dependencies' in row_keys and row['dependencies'] else [],
            budget_tokens=row['budget_tokens'] if 'budget_tokens' in row_keys and row['budget_tokens'] is not None else 0,
            priority=row['priority'] if 'priority' in row_keys and row['priority'] is not None else 5,
            acceptance_tests=json.loads(row['acceptance_tests']) if 'acceptance_tests' in row_keys and row['acceptance_tests'] else [],
            cost_tokens=row['cost_tokens'] if 'cost_tokens' in row_keys and row['cost_tokens'] is not None else 0,
            console_logs=json.loads(row['console_logs']) if 'console_logs' in row_keys and row['console_logs'] else [],
            metrics=json.loads(row['metrics']) if 'metrics' in row_keys and row['metrics'] else {},
        )

    def _task_payload(self, task: Task) -> str:
        return json.dumps(asdict(task))

    def _purge_task_from_queues(self, task: Task):
        payloads = {self._task_payload(task)}

        pending_variant = replace(task, status='pending')
        payloads.add(self._task_payload(pending_variant))

        anonymous_variant = replace(task, worker_id=None)
        payloads.add(self._task_payload(anonymous_variant))

        pending_anonymous = replace(task, status='pending', worker_id=None)
        payloads.add(self._task_payload(pending_anonymous))

        queues = [
            self.task_queue_key,
            self._type_queue(task.type),
            self._feature_queue(task.feature_id, task.type),
        ]

        for queue in queues:
            for payload in payloads:
                try:
                    self.redis_client.lrem(queue, 0, payload)
                except Exception:
                    continue

    def _dependencies_satisfied(self, task: Task) -> bool:
        if not task.dependencies:
            return True

        for dep_id in task.dependencies:
            row = self.db.execute(
                "SELECT status FROM tasks WHERE id=?",
                (dep_id,),
            ).fetchone()
            if not row or row['status'] != 'completed':
                return False
        return True

    def _has_conflict(self, task: Task) -> Optional[str]:
        for artifact in task.artifacts:
            owner = self.active_artifacts.get(artifact)
            if owner and owner != task.id:
                return owner
        return None

    def _register_task_artifacts(self, task: Task):
        for artifact in task.artifacts:
            self.active_artifacts[artifact] = task.id

    def _release_task_artifacts(self, task_id: str):
        for artifact, owner in list(self.active_artifacts.items()):
            if owner == task_id:
                del self.active_artifacts[artifact]

    def _emit_event(self, event_type: str, data: Dict[str, Any]):
        event = {
            "type": event_type,
            "timestamp": datetime.now().isoformat(),
            **data,
        }
        payload = json.dumps(event)
        try:
            self.redis_client.lpush(self.event_queue_key, payload)
            ttl = self.config['redis'].get('message_ttl')
            if ttl:
                self.redis_client.expire(self.event_queue_key, ttl)
        except Exception:
            self.logger.debug("Failed emitting event %s", event_type)

    def _budget_available(self, task: Task) -> bool:
        remaining = self.feature_budgets.get(task.feature_id, self.default_budget)
        return remaining >= task.budget_tokens

    def _consume_budget(self, feature_id: str, tokens: int):
        if tokens <= 0:
            return
        remaining = self.feature_budgets.get(feature_id, self.default_budget)
        remaining = max(0, remaining - tokens)
        self.feature_budgets[feature_id] = remaining

    def _rate_limit_allow(self, key: str) -> bool:
        if self.rate_limit_max <= 0:
            return True

        window_start = time.time() - self.rate_limit_window
        bucket = self.rate_counters.setdefault(key, [])
        while bucket and bucket[0] < window_start:
            bucket.pop(0)
        if len(bucket) >= self.rate_limit_max:
            return False
        bucket.append(time.time())
        return True

    def decompose_feature(self, feature_request: str, feature_id: str) -> List[Task]:
        tasks = self.decomposer.decompose(feature_id, feature_request)
        self.logger.info(f"Decomposed feature into {len(tasks)} tasks")
        return tasks

    def _decompose_naive(self, feature_id: str, feature_request: str) -> List[Task]:
        """Baseline keyword-based decomposition."""
        tasks: List[Task] = []
        now = datetime.now().isoformat()
        task_id_base = f"task_{int(time.time())}"
        default_budget = max(0, self.config['agno'].get('max_tokens', 0) // 4)

        # Always create a backend task if it's a feature
        if any(word in feature_request.lower() for word in ['api', 'backend', 'database', 'service']):
            tasks.append(Task(
                id=f"{task_id_base}_backend",
                feature_id=feature_id,
                type="backend",
                prompt=f"Implement backend functionality: {feature_request}",
                status="pending",
                created_at=now,
                inputs=["api_spec.md"],
                artifacts=["backend/"],
                budget_tokens=default_budget,
                priority=3,
                acceptance_tests=["pytest backend"]
            ))

        # Frontend task if mentioned
        if any(word in feature_request.lower() for word in ['ui', 'frontend', 'interface', 'component']):
            tasks.append(Task(
                id=f"{task_id_base}_frontend",
                feature_id=feature_id,
                type="frontend",
                prompt=f"Implement frontend functionality: {feature_request}",
                status="pending",
                created_at=now,
                inputs=["design.md"],
                artifacts=["frontend/"],
                budget_tokens=default_budget,
                priority=2,
                acceptance_tests=["npm test"]
            ))

        # Always add tests depending on previous tasks
        dependencies = [task.id for task in tasks]
        tasks.append(Task(
            id=f"{task_id_base}_test",
            feature_id=feature_id,
            type="test",
            prompt=f"Write tests for: {feature_request}",
            status="pending",
            created_at=now,
            dependencies=dependencies,
            budget_tokens=default_budget,
            priority=1,
            acceptance_tests=["pytest", "npm test"]
        ))

        # Documentation task depends on everything
        if len(feature_request) > 50:
            doc_dependencies = [task.id for task in tasks]
            tasks.append(Task(
                id=f"{task_id_base}_docs",
                feature_id=feature_id,
                type="docs",
                prompt=f"Write documentation for: {feature_request}",
                status="pending",
                created_at=now,
                dependencies=doc_dependencies,
                budget_tokens=max(0, default_budget // 2),
                priority=4,
                acceptance_tests=["markdownlint"]
            ))

        return tasks

    def store_tasks(self, tasks: List[Task]):
        """Store tasks in database and queue"""
        ttl = self.config['redis'].get('message_ttl')

        for task in tasks:
            if not task.created_at:
                task.created_at = datetime.now().isoformat()
            if not task.feature_id:
                task.feature_id = "feature_unknown"
            self.feature_budgets.setdefault(task.feature_id, self.default_budget)
            if not task.budget_tokens:
                task.budget_tokens = self.type_budget.get(task.type, self.default_budget)
            # Store in database
            self.db.execute(
                """
                INSERT OR REPLACE INTO tasks (
                    id, feature_id, type, prompt, status, worker_id,
                    created_at, completed_at, retry_count, output, error,
                    inputs, artifacts, dependencies, budget_tokens, priority,
                    acceptance_tests, cost_tokens, console_logs, metrics
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._task_to_db_tuple(task),
            )

            # Add to Redis queue(s)
            task_payload = self._task_payload(task)
            self.redis_client.rpush(self._feature_queue(task.feature_id, task.type), task_payload)
            self.redis_client.rpush(self._type_queue(task.type), task_payload)
            self.redis_client.rpush(self.task_queue_key, task_payload)
            if ttl:
                self.redis_client.expire(self.task_queue_key, ttl)
                self.redis_client.expire(self._feature_queue(task.feature_id, task.type), ttl)
                self.redis_client.expire(self._type_queue(task.type), ttl)

            self._emit_event(
                "task.created",
                {
                    "task_id": task.id,
                    "feature_id": task.feature_id,
                    "task_type": task.type,
                    "priority": task.priority,
                    "dependencies": task.dependencies,
                },
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

        env = os.environ.copy()
        if self.config.get('security', {}).get('enable_sandbox'):
            env['SANDBOX_MODE'] = '1'

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=str(Path(__file__).parent.parent),
                close_fds=True,
                env=env
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
        dead_workers: List[str] = []
        now = datetime.now()
        heartbeat_timeout = timedelta(seconds=self.config['orchestrator']['health_check_interval'] * 3)
        task_timeout = self.config['orchestrator']['task_timeout']
        max_memory_mb = self.config['worker']['max_memory_mb']
        max_cpu_percent = self.config['worker']['max_cpu_percent']

        for worker_id, worker in list(self.workers.items()):
            if worker.pid <= 0:
                continue

            try:
                proc = psutil.Process(worker.pid)

                if not proc.is_running():
                    self.logger.warning(f"Worker {worker_id} process is dead")
                    dead_workers.append(worker_id)
                    continue

                if worker.last_heartbeat:
                    try:
                        last_heartbeat_dt = datetime.fromisoformat(worker.last_heartbeat)
                    except ValueError:
                        last_heartbeat_dt = now
                    if now - last_heartbeat_dt > heartbeat_timeout:
                        self.logger.warning(f"Worker {worker_id} heartbeat timeout")
                        dead_workers.append(worker_id)
                        continue

                if worker.current_task_id:
                    started = self.task_started_at.get(worker.current_task_id)
                    if started and (now - started).total_seconds() > task_timeout:
                        self.logger.error(f"Worker {worker_id} exceeded task timeout for {worker.current_task_id}")
                        dead_workers.append(worker_id)
                        continue

                rss_mb = proc.memory_info().rss / (1024 * 1024)
                if rss_mb > max_memory_mb:
                    self.logger.error(
                        f"Worker {worker_id} exceeded memory limit {rss_mb:.1f}MB > {max_memory_mb}MB"
                    )
                    dead_workers.append(worker_id)
                    continue

                cpu_percent = proc.cpu_percent(interval=None)
                if cpu_percent and cpu_percent > max_cpu_percent:
                    self.logger.error(
                        f"Worker {worker_id} exceeded CPU limit {cpu_percent:.1f}% > {max_cpu_percent}%"
                    )
                    dead_workers.append(worker_id)
                    continue

            except psutil.NoSuchProcess:
                self.logger.warning(f"Worker {worker_id} process not found")
                dead_workers.append(worker_id)
            except psutil.Error as exc:
                self.logger.error(f"psutil error for worker {worker_id}: {exc}")
                dead_workers.append(worker_id)

        # Clean up dead workers and spawn replacements
        for worker_id in dead_workers:
            self._handle_dead_worker(worker_id)

    def _handle_dead_worker(self, worker_id: str):
        """Handle a dead worker - clean up and potentially spawn replacement"""
        worker = self.workers.pop(worker_id, None)

        if not worker:
            row = self.db.execute(
                "SELECT id, pid, task_type FROM workers WHERE id=?",
                (worker_id,),
            ).fetchone()
            if not row:
                return
            worker = Worker(id=row['id'], pid=row['pid'], task_type=row['task_type'], status='dead')

        self.logger.info(f"Handling dead worker {worker_id}")

        # Kill process if still alive
        try:
            if worker.pid > 0:
                proc = psutil.Process(worker.pid)
                proc.terminate()
                proc.wait(timeout=5)
        except psutil.NoSuchProcess:
            pass
        except psutil.Error as exc:
            self.logger.warning(f"Failed terminating worker {worker_id}: {exc}")

        # Requeue any running tasks for this worker
        running_tasks = self.db.execute(
            "SELECT id FROM tasks WHERE worker_id=? AND status='running'",
            (worker_id,),
        ).fetchall()

        for (task_id,) in running_tasks:
            self._release_task_artifacts(task_id)
            self.db.execute(
                "UPDATE tasks SET status='pending', worker_id=NULL WHERE id=?",
                (task_id,),
            )
            self.task_started_at.pop(task_id, None)
            self._requeue_task(task_id)

        # Update database
        self.db.execute(
            "UPDATE workers SET status='dead', current_task_id=NULL WHERE id=?",
            (worker_id,),
        )
        self.db.commit()

        self._emit_event(
            "worker.dead",
            {
                "worker_id": worker_id,
                "task_type": worker.task_type,
            },
        )

        # Spawn replacement if needed
        queue_size = self.redis_client.llen(self.task_queue_key)
        active_workers = len([w for w in self.workers.values() if w.status in ['idle', 'working']])

        if queue_size > 0 and active_workers < self.config['orchestrator']['max_workers']:
            time.sleep(self.config['orchestrator']['replacement_delay'])
            self.spawn_worker(worker.task_type)

    def start_feature(self, feature_request: str, num_workers: int = None) -> Dict[str, Any]:
        """Start working on a feature"""
        if num_workers is None:
            num_workers = self.config['orchestrator']['min_workers']

        self.logger.info(f"Starting feature: {feature_request}")

        feature_id = f"feature_{int(time.time() * 1000)}"

        # Decompose feature into tasks
        tasks = self.decompose_feature(feature_request, feature_id)

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
            "feature_id": feature_id,
            "tasks_created": len(tasks),
            "workers_spawned": len(workers_spawned),
            "worker_ids": workers_spawned
        }

    def get_status(self) -> Dict[str, Any]:
        """Get current system status"""
        # Get task counts
        task_counts = {}
        for status in ['pending', 'running', 'completed', 'failed']:
            row = self.db.execute(
                "SELECT COUNT(*) as total FROM tasks WHERE status = ?",
                (status,),
            ).fetchone()
            task_counts[status] = row['total'] if row else 0

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

        worker_rows = self.db.execute(
            "SELECT id, pid, task_type FROM workers WHERE status != 'dead'"
        ).fetchall()

        for worker_id, pid, _task_type in worker_rows:
            try:
                if pid > 0:
                    proc = psutil.Process(pid)
                    proc.terminate()
                    proc.wait(timeout=5)
                    killed_count += 1
            except psutil.TimeoutExpired:
                try:
                    proc.kill()
                    killed_count += 1
                except psutil.Error:
                    pass
            except psutil.Error:
                pass

        running_tasks = self.db.execute(
            "SELECT id FROM tasks WHERE status='running'"
        ).fetchall()

        for (task_id,) in running_tasks:
            self._release_task_artifacts(task_id)
            self.db.execute(
                "UPDATE tasks SET status='pending', worker_id=NULL WHERE id=?",
                (task_id,),
            )
            self.task_started_at.pop(task_id, None)
            self._requeue_task(task_id)

        self.db.execute("UPDATE workers SET status='dead', current_task_id=NULL")
        self.db.commit()

        self.workers.clear()
        self.running = False

        return {
            "status": "stopped",
            "workers_killed": killed_count
        }

    def drain(self) -> Dict[str, Any]:
        """Gracefully stop accepting work and terminate workers"""
        self.logger.info("Draining orchestrator queues")
        snapshot = self.get_status()
        kill_info = self.kill_all()
        return {
            "status": "drained",
            "snapshot": snapshot,
            **kill_info,
        }

    def scale_workers(self, target: int) -> Dict[str, Any]:
        """Scale worker pool to a desired size (best-effort)"""
        if target <= 0:
            return {"status": "invalid", "message": "Target must be positive"}

        current = len(self.workers)
        if target <= current:
            return {
                "status": "noop",
                "current": current,
                "target": target,
            }

        task_types = list(self.config.get('task_types', {}).keys())
        if not task_types:
            task_types = ['backend', 'frontend', 'test', 'docs']

        spawned: List[str] = []
        for i in range(target - current):
            worker = self.spawn_worker(task_types[i % len(task_types)])
            spawned.append(worker.id)

        return {
            "status": "scaled",
            "current": len(self.workers),
            "target": target,
            "spawned": spawned,
        }

    def run(self):
        """Main orchestrator loop"""
        self.running = True
        self.logger.info("Orchestrator starting main loop")

        try:
            while self.running:
                # Process any events from workers
                self.process_once()

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
    parser.add_argument("--drain", action="store_true", help="Drain all workers and stop without new assignments")
    parser.add_argument("--scale", type=int, help="Scale total worker count to the provided value")
    parser.add_argument("--kill-all", action="store_true", help="Stop all workers and exit")
    parser.add_argument("--status", action="store_true", help="Print orchestrator status and exit")

    args = parser.parse_args()

    orchestrator = SimpleOrchestrator(args.config)

    if args.kill_all:
        result = orchestrator.kill_all()
        print(json.dumps(result))
        return

    if args.status:
        print(json.dumps(orchestrator.get_status()))
        return

    if args.drain:
        result = orchestrator.drain()
        print(json.dumps(result))
        return

    if args.scale is not None:
        result = orchestrator.scale_workers(args.scale)
        print(json.dumps(result))
        return

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
