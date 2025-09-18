import copy
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pytest
import yaml

sys.path.append(str(Path(__file__).parent.parent))

from orchestrator.main import SimpleOrchestrator, Task
from fake_redis import SimpleFakeRedis
from workers.agent import CodingWorker


@pytest.fixture(scope="module")
def base_config() -> dict:
    config_path = Path("config.yaml")
    with config_path.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    return config


@pytest.fixture
def config_factory(base_config, tmp_path):
    def _factory(**overrides):
        cfg = copy.deepcopy(base_config)
        cfg['storage']['path'] = str(tmp_path / "orchestrator.db")
        cfg.setdefault('development', {})['test_mode'] = True
        cfg['development']['mock_llm'] = overrides.get('mock_llm', True)
        cfg['worker']['heartbeat_interval'] = overrides.get('heartbeat_interval', 1)
        cfg['worker']['idle_timeout'] = overrides.get('idle_timeout', 2)
        cfg['orchestrator']['health_check_interval'] = overrides.get('health_check_interval', 1)
        cfg['orchestrator']['retry_limit'] = overrides.get('retry_limit', cfg['orchestrator']['retry_limit'])
        cfg['orchestrator']['task_timeout'] = overrides.get('task_timeout', cfg['orchestrator']['task_timeout'])
        return cfg

    return _factory


@pytest.fixture
def orchestrator(config_factory):
    config = config_factory()
    orchestrator = SimpleOrchestrator(config=config)
    try:
        yield orchestrator
    finally:
        orchestrator.kill_all()
        orchestrator.db.close()
        if hasattr(orchestrator.redis_client, "close"):
            orchestrator.redis_client.close()


def test_assignment_and_completion_flow(orchestrator):
    task = Task(
        id="task_backend_001",
        feature_id="feature_test_flow",
        type="backend",
        prompt="Implement backend endpoint",
        status="pending",
        created_at=datetime.now().isoformat(),
    )

    orchestrator.store_tasks([task])

    assignment = {
        "worker_id": "worker_backend_1",
        "task_id": task.id,
        "task_type": "backend",
        "feature_id": task.feature_id,
        "timestamp": datetime.now().isoformat(),
        "pid": 4242,
    }

    orchestrator.redis_client.lpush(
        orchestrator.config['redis']['queues']['assignment_queue'],
        json.dumps(assignment),
    )
    orchestrator.process_once()

    task_row = orchestrator.db.execute(
        "SELECT status, worker_id FROM tasks WHERE id=?",
        (task.id,),
    ).fetchone()
    assert task_row['status'] == 'running'
    assert task_row['worker_id'] == 'worker_backend_1'

    result_payload = {
        "task_id": task.id,
        "status": "completed",
        "output": "done",
        "worker_id": "worker_backend_1",
        "task_type": "backend",
        "pid": 4242,
        "completed_at": datetime.now().isoformat(),
    }

    orchestrator.redis_client.lpush(
        orchestrator.config['redis']['queues']['result_queue'],
        json.dumps(result_payload),
    )
    orchestrator.process_once()

    task_row = orchestrator.db.execute(
        "SELECT status, worker_id, output FROM tasks WHERE id=?",
        (task.id,),
    ).fetchone()
    assert task_row['status'] == 'completed'
    assert task_row['worker_id'] == 'worker_backend_1'
    assert task_row['output'] == 'done'

    worker_row = orchestrator.db.execute(
        "SELECT status FROM workers WHERE id=?",
        ("worker_backend_1",),
    ).fetchone()
    assert worker_row['status'] == 'idle'


def test_failed_task_requeues_and_hits_limit(config_factory):
    config = config_factory(retry_limit=1)
    orchestrator = SimpleOrchestrator(config=config)

    try:
        task = Task(
            id="task_backend_fail",
            feature_id="feature_fail",
            type="backend",
            prompt="Failing task",
            status="pending",
            created_at=datetime.now().isoformat(),
        )
        orchestrator.store_tasks([task])

        queues_cfg = orchestrator.config['redis']['queues']
        task_queue_key = queues_cfg['task_queue']
        type_queue_key = f"{queues_cfg.get('task_type_prefix', 'tasks:type')}:backend"
        orchestrator.redis_client.blpop([type_queue_key, task_queue_key])
        orchestrator.redis_client.delete(queues_cfg['event_queue'])

        assignment = {
            "worker_id": "worker_backend_fail",
            "task_id": task.id,
            "task_type": "backend",
            "feature_id": task.feature_id,
            "timestamp": datetime.now().isoformat(),
            "pid": 5555,
        }
        orchestrator.redis_client.lpush(
            orchestrator.config['redis']['queues']['assignment_queue'],
            json.dumps(assignment),
        )
        orchestrator.process_once()

        # First failure – task should be retried
        failure = {
            "task_id": task.id,
            "status": "failed",
            "error": "network glitch",
            "worker_id": "worker_backend_fail",
            "task_type": "backend",
            "pid": 5555,
            "completed_at": datetime.now().isoformat(),
        }
        orchestrator.redis_client.lpush(
            orchestrator.config['redis']['queues']['result_queue'],
            json.dumps(failure),
        )
        orchestrator.process_once()

        task_row = orchestrator.db.execute(
            "SELECT status, retry_count FROM tasks WHERE id=?",
            (task.id,),
        ).fetchone()
        assert task_row['status'] == 'pending'
        assert task_row['retry_count'] == 1
        assert orchestrator.redis_client.llen(task_queue_key) == 1
        assert orchestrator.redis_client.llen(type_queue_key) == 1

        # Reassign and fail again to exceed limit
        orchestrator.redis_client.lpush(
            orchestrator.config['redis']['queues']['assignment_queue'],
            json.dumps(assignment),
        )
        orchestrator.process_once()

        orchestrator.redis_client.lpush(
            orchestrator.config['redis']['queues']['result_queue'],
            json.dumps(failure),
        )
        orchestrator.process_once()

        task_row = orchestrator.db.execute(
            "SELECT status, retry_count FROM tasks WHERE id=?",
            (task.id,),
        ).fetchone()
        assert task_row['status'] == 'failed'
        assert task_row['retry_count'] == 2
        assert orchestrator.redis_client.llen(orchestrator.config['redis']['queues']['failure_queue']) == 1

    finally:
        orchestrator.kill_all()
        orchestrator.db.close()
        if hasattr(orchestrator.redis_client, "close"):
            orchestrator.redis_client.close()


def test_worker_task_filtering(config_factory):
    config = config_factory()
    fake_redis = SimpleFakeRedis()

    worker = CodingWorker(
        worker_id="worker_backend",
        task_type="backend",
        config_path="config.yaml",
        config=config,
        redis_client=fake_redis,
    )

    task_queue = config['redis']['queues']['task_queue']
    assign_queue = config['redis']['queues']['assignment_queue']

    # Push a task of the wrong type first
    fake_redis.rpush(
        task_queue,
        json.dumps({
            "id": "task_frontend",
            "type": "frontend",
            "prompt": "frontend stuff",
            "status": "pending",
        }),
    )

    assert worker.get_task() is None
    assert fake_redis.llen(task_queue) == 1  # Task was returned to the queue

    # Simulate a matching worker picking up the frontend task
    fake_redis.blpop(task_queue)

    # Push a backend task and ensure it is claimed
    fake_redis.rpush(
        task_queue,
        json.dumps({
            "id": "task_backend",
            "type": "backend",
            "prompt": "backend work",
            "status": "pending",
        }),
    )

    task = worker.get_task()
    assert task is not None
    assert task['id'] == 'task_backend'
    assert fake_redis.llen(assign_queue) == 1

    if hasattr(worker.redis_client, "close"):
        worker.redis_client.close()


def test_conflict_detection_requeues(config_factory):
    config = config_factory()
    orchestrator = SimpleOrchestrator(config=config)

    try:
        base_artifacts = ["services/api.py"]
        feature_id = "feature_conflict"

        task_one = Task(
            id="task_conflict_1",
            feature_id=feature_id,
            type="backend",
            prompt="Implement API",
            status="pending",
            artifacts=base_artifacts,
            created_at=datetime.now().isoformat(),
        )

        task_two = Task(
            id="task_conflict_2",
            feature_id=feature_id,
            type="backend",
            prompt="Modify same API",
            status="pending",
            artifacts=base_artifacts,
            created_at=datetime.now().isoformat(),
            dependencies=[task_one.id],
        )

        orchestrator.store_tasks([task_one, task_two])

        queues_cfg = orchestrator.config['redis']['queues']
        task_queue_key = queues_cfg['task_queue']
        type_queue_key = f"{queues_cfg.get('task_type_prefix', 'tasks:type')}:backend"

        # Drain initial queue entries as workers would
        orchestrator.redis_client.blpop([type_queue_key, task_queue_key])
        orchestrator.redis_client.blpop([type_queue_key, task_queue_key])

        # First assignment should succeed
        orchestrator.redis_client.lpush(
            queues_cfg['assignment_queue'],
            json.dumps({
                "worker_id": "worker_a",
                "task_id": task_one.id,
                "task_type": "backend",
                "feature_id": feature_id,
                "timestamp": datetime.now().isoformat(),
                "pid": 1001,
            }),
        )
        orchestrator.process_once()

        # Reset event queue to observe conflict event clearly
        orchestrator.redis_client.delete(queues_cfg['event_queue'])

        # Second assignment should be deferred due to conflict (dependency + artifact)
        orchestrator.redis_client.lpush(
            queues_cfg['assignment_queue'],
            json.dumps({
                "worker_id": "worker_b",
                "task_id": task_two.id,
                "task_type": "backend",
                "feature_id": feature_id,
                "timestamp": datetime.now().isoformat(),
                "pid": 1002,
            }),
        )
        orchestrator.process_once()

        task_two_row = orchestrator.db.execute(
            "SELECT status FROM tasks WHERE id=?",
            (task_two.id,),
        ).fetchone()
        assert task_two_row['status'] == 'pending'

        event_payload = orchestrator.redis_client.rpop(queues_cfg['event_queue'])
        assert event_payload is not None
        event = json.loads(event_payload)
        assert event['type'] in {"task.deferred.dependencies", "task.deferred.conflict"}
        assert event['task_id'] == task_two.id

    finally:
        orchestrator.kill_all()
        orchestrator.db.close()
        if hasattr(orchestrator.redis_client, "close"):
            orchestrator.redis_client.close()


def test_budget_enforcement(config_factory):
    config = config_factory()
    config['budgets']['default_tokens'] = 5
    config['budgets']['per_task_type']['backend'] = 5
    orchestrator = SimpleOrchestrator(config=config)

    try:
        task = Task(
            id="task_budget_1",
            feature_id="feature_budget",
            type="backend",
            prompt="Expensive task",
            status="pending",
            created_at=datetime.now().isoformat(),
            budget_tokens=10,
        )

        orchestrator.store_tasks([task])

        queues_cfg = orchestrator.config['redis']['queues']
        task_queue_key = queues_cfg['task_queue']
        type_queue_key = f"{queues_cfg.get('task_type_prefix', 'tasks:type')}:backend"
        orchestrator.redis_client.blpop([type_queue_key, task_queue_key])
        orchestrator.redis_client.delete(queues_cfg['event_queue'])

        orchestrator.redis_client.lpush(
            queues_cfg['assignment_queue'],
            json.dumps({
                "worker_id": "worker_budget",
                "task_id": task.id,
                "task_type": "backend",
                "feature_id": task.feature_id,
                "timestamp": datetime.now().isoformat(),
                "pid": 2001,
            }),
        )

        orchestrator.process_once()

        task_row = orchestrator.db.execute(
            "SELECT status FROM tasks WHERE id=?",
            (task.id,),
        ).fetchone()
        assert task_row['status'] == 'pending'

        event_payload = orchestrator.redis_client.rpop(queues_cfg['event_queue'])
        assert event_payload is not None
        event = json.loads(event_payload)
        assert event['type'] == 'task.deferred.budget'
        assert event['task_id'] == task.id

    finally:
        orchestrator.kill_all()
        orchestrator.db.close()
        if hasattr(orchestrator.redis_client, "close"):
            orchestrator.redis_client.close()


def test_rate_limit_backpressure(config_factory):
    config = config_factory()
    config['rate_limits']['max_requests'] = 1
    config['rate_limits']['window_seconds'] = 60
    orchestrator = SimpleOrchestrator(config=config)

    try:
        feature_id = "feature_rate"
        tasks = [
            Task(
                id=f"task_rate_{i}",
                feature_id=feature_id,
                type="backend",
                prompt="Task",
                status="pending",
                created_at=datetime.now().isoformat(),
            )
            for i in range(2)
        ]

        orchestrator.store_tasks(tasks)

        queues_cfg = orchestrator.config['redis']['queues']
        task_queue_key = queues_cfg['task_queue']
        type_queue_key = f"{queues_cfg.get('task_type_prefix', 'tasks:type')}:backend"
        orchestrator.redis_client.blpop([type_queue_key, task_queue_key])
        orchestrator.redis_client.blpop([type_queue_key, task_queue_key])

        # First assignment allowed
        orchestrator.redis_client.lpush(
            queues_cfg['assignment_queue'],
            json.dumps({
                "worker_id": "worker_rate_a",
                "task_id": tasks[0].id,
                "task_type": "backend",
                "feature_id": feature_id,
                "timestamp": datetime.now().isoformat(),
                "pid": 3001,
            }),
        )
        orchestrator.process_once()

        orchestrator.redis_client.delete(queues_cfg['event_queue'])

        orchestrator.redis_client.lpush(
            queues_cfg['assignment_queue'],
            json.dumps({
                "worker_id": "worker_rate_b",
                "task_id": tasks[1].id,
                "task_type": "backend",
                "feature_id": feature_id,
                "timestamp": datetime.now().isoformat(),
                "pid": 3002,
            }),
        )
        orchestrator.process_once()

        task_row = orchestrator.db.execute(
            "SELECT status FROM tasks WHERE id=?",
            (tasks[1].id,),
        ).fetchone()
        assert task_row['status'] == 'pending'

        event_payload = orchestrator.redis_client.rpop(queues_cfg['event_queue'])
        assert event_payload is not None
        event = json.loads(event_payload)
        assert event['type'] == 'task.deferred.rate_limit'
        assert event['task_id'] == tasks[1].id

    finally:
        orchestrator.kill_all()
        orchestrator.db.close()
        if hasattr(orchestrator.redis_client, "close"):
            orchestrator.redis_client.close()
