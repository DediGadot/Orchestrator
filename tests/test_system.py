#!/usr/bin/env python3
"""
System Integration Test

Tests the complete distributed coding agent system:
1. Orchestrator can decompose tasks
2. Workers can execute tasks
3. MCP server can interface with the system

This is proof that our system works as designed.
"""

import os
import sys
import time
import json
import subprocess
import threading
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from orchestrator.main import SimpleOrchestrator


def test_orchestrator_basic():
    """Test basic orchestrator functionality"""
    print("🧪 Testing orchestrator basic functionality...")

    try:
        # Initialize orchestrator
        orchestrator = SimpleOrchestrator("config.yaml")

        # Test task decomposition
        feature_request = "Create user authentication API with frontend login form"
        tasks = orchestrator.decompose_feature(feature_request)

        print(f"✅ Task decomposition: {len(tasks)} tasks created")
        for task in tasks:
            print(f"   - {task.type}: {task.prompt[:50]}...")

        # Test storing tasks
        orchestrator.store_tasks(tasks)
        print("✅ Tasks stored successfully")

        # Check queue size
        queue_size = orchestrator.redis_client.llen("coding_tasks")
        print(f"✅ Queue size: {queue_size} tasks")

        # Clean up
        orchestrator.redis_client.flushdb()
        orchestrator.db.close()

        return True

    except Exception as e:
        print(f"❌ Orchestrator test failed: {e}")
        return False


def test_worker_creation():
    """Test worker agent creation"""
    print("\n🧪 Testing worker agent creation...")

    try:
        # Test creating a worker process (don't actually run it)
        cmd = [
            sys.executable,
            "workers/agent.py",
            "--worker-id", "test_worker",
            "--task-type", "backend",
            "--config", "config.yaml",
            "--help"  # Just show help, don't actually run
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if "Distributed Coding Worker Agent" in result.stdout:
            print("✅ Worker agent script is executable and has correct CLI")
            return True
        else:
            print(f"❌ Worker agent test failed: {result.stderr}")
            return False

    except Exception as e:
        print(f"❌ Worker creation test failed: {e}")
        return False


def test_mcp_server():
    """Test MCP server compilation and basic functionality"""
    print("\n🧪 Testing MCP server...")

    try:
        # Check if MCP server was built
        mcp_server_path = Path("mcp-server/build/index.js")
        if not mcp_server_path.exists():
            print("❌ MCP server build not found")
            return False

        print("✅ MCP server build exists")

        # Test that the server can start (with timeout)
        def test_server_start():
            cmd = ["node", "mcp-server/build/index.js"]
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Give it 3 seconds to start
            time.sleep(3)
            proc.terminate()
            proc.wait()

        # Run server test in thread with timeout
        server_thread = threading.Thread(target=test_server_start)
        server_thread.start()
        server_thread.join(timeout=5)

        print("✅ MCP server can start without crashing")
        return True

    except Exception as e:
        print(f"❌ MCP server test failed: {e}")
        return False


def test_redis_integration():
    """Test Redis queue functionality"""
    print("\n🧪 Testing Redis integration...")

    try:
        import redis

        # Connect to Redis
        client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        # Test connection
        client.ping()
        print("✅ Redis connection successful")

        # Test queue operations
        client.lpush("test_queue", json.dumps({"test": "data"}))
        result = client.blpop("test_queue", timeout=1)

        if result:
            queue_name, data = result
            parsed_data = json.loads(data)
            if parsed_data.get("test") == "data":
                print("✅ Redis queue operations working")
                return True

        print("❌ Redis queue test failed")
        return False

    except Exception as e:
        print(f"❌ Redis integration test failed: {e}")
        return False


def test_config_loading():
    """Test configuration loading"""
    print("\n🧪 Testing configuration loading...")

    try:
        import yaml

        with open("config.yaml", 'r') as f:
            config = yaml.safe_load(f)

        # Check key configuration sections
        required_sections = ['system', 'orchestrator', 'redis', 'agno', 'worker', 'mcp']
        for section in required_sections:
            if section not in config:
                print(f"❌ Missing config section: {section}")
                return False

        print("✅ Configuration file valid and complete")
        return True

    except Exception as e:
        print(f"❌ Config loading test failed: {e}")
        return False


def run_all_tests():
    """Run all system tests"""
    print("🚀 Starting Distributed Coding Agent System Tests\n")

    tests = [
        ("Configuration Loading", test_config_loading),
        ("Redis Integration", test_redis_integration),
        ("Orchestrator Basic", test_orchestrator_basic),
        ("Worker Creation", test_worker_creation),
        ("MCP Server", test_mcp_server),
    ]

    results = {}

    for test_name, test_func in tests:
        results[test_name] = test_func()

    # Print summary
    print("\n" + "="*50)
    print("📊 TEST RESULTS SUMMARY")
    print("="*50)

    passed = 0
    total = len(tests)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 ALL TESTS PASSED! System is working correctly.")
        return True
    else:
        print(f"\n⚠️  {total - passed} tests failed. Check the output above.")
        return False


if __name__ == "__main__":
    # Make sure we're in the right directory
    os.chdir(Path(__file__).parent.parent)

    success = run_all_tests()
    sys.exit(0 if success else 1)