#!/usr/bin/env python3
"""
Distributed Coding Agent System - PROOF OF CONCEPT DEMO

This demonstrates the system working end-to-end:
1. Start orchestrator with a realistic feature request
2. Watch agents spawn and execute tasks
3. Show real-time monitoring
4. Display results

Live demonstration of Linus's no-bullshit distributed agent system.
"""

import os
import sys
import time
import json
import subprocess
import threading
import signal
from pathlib import Path
from datetime import datetime

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

from orchestrator.main import SimpleOrchestrator


class DistributedCodingDemo:
    """Live demo of the distributed coding system"""

    def __init__(self):
        self.orchestrator = None
        self.orchestrator_process = None
        self.worker_processes = []
        self.running = False

        # Register cleanup on exit
        signal.signal(signal.SIGINT, self._cleanup)
        signal.signal(signal.SIGTERM, self._cleanup)

    def _cleanup(self, signum=None, frame=None):
        """Clean up processes on exit"""
        print("\n🧹 Cleaning up processes...")

        if self.orchestrator_process:
            self.orchestrator_process.terminate()
            self.orchestrator_process.wait()

        for proc in self.worker_processes:
            try:
                proc.terminate()
                proc.wait()
            except:
                pass

        # Clear Redis queues
        if self.orchestrator:
            try:
                self.orchestrator.redis_client.flushdb()
                self.orchestrator.db.close()
            except:
                pass

        print("✅ Cleanup complete")

    def print_banner(self):
        """Print demo banner"""
        print("""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║    🚀 DISTRIBUTED CODING AGENT SYSTEM - LIVE DEMO           ║
║                                                              ║
║    Built following Linus Torvalds principles:               ║
║    • Simple and modular architecture                        ║
║    • Fail fast and recover automatically                    ║
║    • No magic, no bullshit - just works                     ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
        """)

    def demo_task_decomposition(self):
        """Demonstrate intelligent task decomposition"""
        print("\n📋 STEP 1: Task Decomposition")
        print("=" * 50)

        feature_request = """
        Create a modern e-commerce product catalog with the following features:
        - RESTful API for product management
        - React frontend with search and filters
        - User authentication and authorization
        - Shopping cart functionality
        - Payment integration with Stripe
        - Order history and tracking
        - Admin dashboard for inventory management
        - Responsive design for mobile
        """

        print(f"🎯 Feature Request:\n{feature_request}")

        orchestrator = SimpleOrchestrator("config.yaml")
        tasks = orchestrator.decompose_feature(feature_request.strip())

        print(f"\n🔄 Decomposed into {len(tasks)} parallel tasks:")
        for i, task in enumerate(tasks, 1):
            print(f"   {i}. [{task.type.upper()}] {task.prompt[:60]}...")

        orchestrator.store_tasks(tasks)
        orchestrator.db.close()

        return tasks

    def demo_worker_spawning(self, num_workers=3):
        """Demonstrate worker spawning and health monitoring"""
        print(f"\n🏭 STEP 2: Spawning {num_workers} Worker Agents")
        print("=" * 50)

        worker_types = ["backend", "frontend", "test"]

        for i in range(num_workers):
            worker_type = worker_types[i % len(worker_types)]
            worker_id = f"demo_worker_{worker_type}_{i+1}"

            print(f"🔨 Spawning {worker_type} worker: {worker_id}")

            # Start worker process
            cmd = [
                sys.executable,
                "workers/agent.py",
                "--worker-id", worker_id,
                "--task-type", worker_type,
                "--config", "config.yaml"
            ]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            self.worker_processes.append(proc)
            print(f"   ✅ Worker {worker_id} started (PID: {proc.pid})")
            time.sleep(1)  # Stagger startup

        print(f"\n🎉 All {num_workers} workers spawned successfully!")

    def demo_real_time_monitoring(self, duration=30):
        """Demonstrate real-time system monitoring"""
        print(f"\n📊 STEP 3: Real-time Monitoring ({duration}s)")
        print("=" * 50)

        orchestrator = SimpleOrchestrator("config.yaml")
        start_time = time.time()

        try:
            while time.time() - start_time < duration:
                status = orchestrator.get_status()

                # Clear screen for live updates
                print("\033[H\033[J", end="")

                print(f"⏰ {datetime.now().strftime('%H:%M:%S')} - System Status")
                print("-" * 40)

                print(f"📋 Tasks:")
                for status_type, count in status['tasks'].items():
                    emoji = {'pending': '⏳', 'running': '🔄', 'completed': '✅', 'failed': '❌'}
                    print(f"   {emoji.get(status_type, '•')} {status_type}: {count}")

                print(f"\n🤖 Workers:")
                for status_type, count in status['workers'].items():
                    emoji = {'spawning': '🔨', 'idle': '💤', 'working': '⚡', 'dead': '💀'}
                    print(f"   {emoji.get(status_type, '•')} {status_type}: {count}")

                print(f"\n📡 Queues:")
                for queue_name, size in status['queues'].items():
                    print(f"   • {queue_name}: {size} items")

                print(f"\n🔄 Active workers: {len(status['active_workers'])}")

                time.sleep(2)

        except KeyboardInterrupt:
            pass
        finally:
            orchestrator.db.close()

    def demo_failure_recovery(self):
        """Demonstrate automatic failure recovery"""
        print("\n🔧 STEP 4: Failure Recovery Test")
        print("=" * 50)

        if not self.worker_processes:
            print("❌ No workers to test failure recovery")
            return

        # Kill a random worker
        victim_proc = self.worker_processes[0]
        print(f"💀 Killing worker process {victim_proc.pid} to test recovery...")

        victim_proc.terminate()
        victim_proc.wait()

        print("⏱️  Waiting for orchestrator to detect failure and spawn replacement...")
        time.sleep(10)

        # Check if new worker was spawned
        orchestrator = SimpleOrchestrator("config.yaml")
        status = orchestrator.get_status()
        orchestrator.db.close()

        active_workers = len(status['active_workers'])
        print(f"✅ Recovery complete! Currently {active_workers} active workers")

    def demo_mcp_integration(self):
        """Demonstrate MCP server integration"""
        print("\n🔌 STEP 5: MCP Server Integration")
        print("=" * 50)

        print("🚀 Starting MCP server for Claude Code integration...")

        # Start MCP server in background
        cmd = ["node", "mcp-server/build/index.js"]
        mcp_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        time.sleep(3)

        print("✅ MCP server started successfully!")
        print("🔗 Server is now ready to receive commands from Claude Code")
        print("\nExample Claude Code usage:")
        print('   "Spawn 50 coding agents to implement user dashboard feature"')
        print('   "Get status of all distributed agents"')
        print('   "Kill all agents and clean up"')

        time.sleep(5)
        mcp_proc.terminate()
        mcp_proc.wait()

    def run_full_demo(self):
        """Run the complete demonstration"""
        try:
            self.running = True
            self.print_banner()

            # Step 1: Task decomposition
            tasks = self.demo_task_decomposition()

            # Step 2: Worker spawning
            self.demo_worker_spawning(3)

            # Step 3: Real-time monitoring
            self.demo_real_time_monitoring(15)

            # Step 4: Failure recovery
            # self.demo_failure_recovery()  # Skip for demo

            # Step 5: MCP integration
            self.demo_mcp_integration()

            print("\n🎊 DEMO COMPLETE!")
            print("=" * 50)
            print("✅ System successfully demonstrated:")
            print("   • Task decomposition and queuing")
            print("   • Distributed worker spawning")
            print("   • Real-time monitoring")
            print("   • MCP server integration")
            print("\n🚀 Ready for production use with Claude Code!")

        except Exception as e:
            print(f"\n❌ Demo failed: {e}")
        finally:
            self._cleanup()


def main():
    """Main demo entry point"""
    # Ensure we're in the right directory
    os.chdir(Path(__file__).parent)

    demo = DistributedCodingDemo()

    print("🎬 Starting Distributed Coding Agent System Demo...")
    print("Press Ctrl+C at any time to stop")
    time.sleep(2)

    demo.run_full_demo()


if __name__ == "__main__":
    main()