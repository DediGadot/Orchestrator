#!/usr/bin/env node

/**
 * Distributed Coding Orchestrator MCP Server
 *
 * Simple MCP server that interfaces with Claude Code to orchestrate
 * hundreds of coding agents for distributed feature development.
 *
 * Does three things:
 * 1. spawn_workers - Start the distributed agent swarm
 * 2. get_status - Real-time status of all agents
 * 3. kill_all - Stop everything
 *
 * No bullshit, just works.
 */

import { FastMCP } from "fastmcp";
import { z } from "zod";
import { readFileSync } from "fs";
import { spawn, exec } from "child_process";
import { promisify } from "util";
import * as path from "path";
import * as yaml from "yaml";
import Redis from "ioredis";

const execAsync = promisify(exec);

interface Config {
  system: {
    name: string;
    version: string;
    debug: boolean;
  };
  redis: {
    host: string;
    port: number;
    db: number;
    password?: string;
  };
  orchestrator: {
    max_workers: number;
    min_workers: number;
  };
  mcp: {
    server: {
      name: string;
      version: string;
      description: string;
    };
  };
}

class DistributedCodingMCP {
  private config: Config;
  private redis: Redis;
  private orchestratorProcess: any = null;

  constructor() {
    this.config = this.loadConfig();
    this.redis = new Redis({
      host: this.config.redis.host,
      port: this.config.redis.port,
      db: this.config.redis.db,
      password: this.config.redis.password,
    });
  }

  private loadConfig(): Config {
    try {
      // Look for config.yaml in parent directory
      const configPath = path.join(__dirname, "../../config.yaml");
      const configContent = readFileSync(configPath, "utf8");
      return yaml.parse(configContent) as Config;
    } catch (error) {
      console.error("Failed to load config:", error);
      process.exit(1);
    }
  }

  private async startOrchestrator(feature: string, workers: number): Promise<{ status: string; message: string; pid?: number }> {
    try {
      // Kill existing orchestrator if running
      if (this.orchestratorProcess) {
        this.orchestratorProcess.kill();
        this.orchestratorProcess = null;
      }

      // Start new orchestrator process
      const orchestratorPath = path.join(__dirname, "../../orchestrator/main.py");
      const configPath = path.join(__dirname, "../../config.yaml");

      this.orchestratorProcess = spawn("python3", [
        orchestratorPath,
        "--config", configPath,
        "--feature", feature,
        "--workers", workers.toString()
      ], {
        cwd: path.join(__dirname, "../.."),
        stdio: ["ignore", "pipe", "pipe"],
        detached: false
      });

      // Log output for debugging
      this.orchestratorProcess.stdout?.on("data", (data: Buffer) => {
        console.log(`[Orchestrator] ${data.toString()}`);
      });

      this.orchestratorProcess.stderr?.on("data", (data: Buffer) => {
        console.error(`[Orchestrator Error] ${data.toString()}`);
      });

      this.orchestratorProcess.on("error", (error: Error) => {
        console.error("Orchestrator process error:", error);
        this.orchestratorProcess = null;
      });

      this.orchestratorProcess.on("exit", (code: number | null) => {
        console.log(`Orchestrator exited with code ${code}`);
        this.orchestratorProcess = null;
      });

      // Wait a moment for the process to start
      await new Promise(resolve => setTimeout(resolve, 2000));

      return {
        status: "started",
        message: `Orchestrator started for feature: ${feature}`,
        pid: this.orchestratorProcess.pid
      };

    } catch (error) {
      return {
        status: "error",
        message: `Failed to start orchestrator: ${error}`
      };
    }
  }

  private async getOrchestratorStatus(): Promise<any> {
    try {
      // Get status from Redis queues and database
      const taskQueue = await this.redis.llen("coding_tasks");
      const resultQueue = await this.redis.llen("completed_tasks");
      const heartbeatQueue = await this.redis.llen("worker_heartbeats");

      return {
        orchestrator_running: this.orchestratorProcess !== null,
        orchestrator_pid: this.orchestratorProcess?.pid || null,
        queues: {
          tasks_pending: taskQueue,
          results_ready: resultQueue,
          heartbeats: heartbeatQueue
        },
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        status: "error",
        message: `Failed to get status: ${error}`
      };
    }
  }

  private async killAllProcesses(): Promise<{ status: string; message: string }> {
    try {
      let killed = 0;

      // Kill orchestrator
      if (this.orchestratorProcess) {
        this.orchestratorProcess.kill("SIGTERM");
        this.orchestratorProcess = null;
        killed++;
      }

      // Delegate kill logic to orchestrator CLI for precise cleanup
      const orchestratorPath = path.join(__dirname, "../../orchestrator/main.py");
      const configPath = path.join(__dirname, "../../config.yaml");

      await execAsync(`python3 "${orchestratorPath}" --config "${configPath}" --kill-all`);

      // Clear Redis queues
      await this.redis.del("coding_tasks");
      await this.redis.del("completed_tasks");
      await this.redis.del("worker_heartbeats");
      await this.redis.del("failed_tasks");
      await this.redis.del("system_events");

      return {
        status: "stopped",
        message: `Killed ${killed} processes and cleared queues`
      };

    } catch (error) {
      return {
        status: "error",
        message: `Failed to kill processes: ${error}`
      };
    }
  }

  public createServer(): FastMCP {
    const server = new FastMCP({
      name: this.config.mcp.server.name,
      version: "1.0.0",
    });

    // Tool: spawn_workers
    server.addTool({
      name: "spawn_workers",
      description: "Start distributed coding agents to implement a feature",
      parameters: z.object({
        feature: z.string().describe("Feature description to implement"),
        workers: z.number()
          .min(1)
          .max(this.config.orchestrator.max_workers)
          .default(this.config.orchestrator.min_workers)
          .describe("Number of workers to spawn")
      }),
      execute: async (args) => {
        console.log(`Spawning ${args.workers} workers for feature: ${args.feature}`);
        const result = await this.startOrchestrator(args.feature, args.workers);
        return JSON.stringify(result, null, 2);
      }
    });

    // Tool: get_status
    server.addTool({
      name: "get_status",
      description: "Get real-time status of the distributed coding system",
      parameters: z.object({}),
      execute: async () => {
        console.log("Getting system status");
        const status = await this.getOrchestratorStatus();
        return JSON.stringify(status, null, 2);
      }
    });

    // Tool: kill_all
    server.addTool({
      name: "kill_all",
      description: "Stop all running agents and clear the system",
      parameters: z.object({}),
      execute: async () => {
        console.log("Killing all processes");
        const result = await this.killAllProcesses();
        return JSON.stringify(result, null, 2);
      }
    });

    // Tool: drain
    server.addTool({
      name: "drain",
      description: "Drain current workers and stop accepting new tasks",
      parameters: z.object({}),
      execute: async () => {
        console.log("Draining orchestrator");
        const orchestratorPath = path.join(__dirname, "../../orchestrator/main.py");
        const configPath = path.join(__dirname, "../../config.yaml");
        const { stdout } = await execAsync(`python3 "${orchestratorPath}" --config "${configPath}" --drain`);
        return stdout ? stdout.trim() : JSON.stringify({ status: "drained" }, null, 2);
      }
    });

    // Tool: scale_workers
    server.addTool({
      name: "scale_workers",
      description: "Scale worker pool to the desired size",
      parameters: z.object({
        workers: z.number()
          .min(1)
          .max(this.config.orchestrator.max_workers)
          .describe("Total number of workers desired")
      }),
      execute: async (args) => {
        console.log(`Scaling orchestrator to ${args.workers} workers`);
        const orchestratorPath = path.join(__dirname, "../../orchestrator/main.py");
        const configPath = path.join(__dirname, "../../config.yaml");
        const { stdout } = await execAsync(`python3 "${orchestratorPath}" --config "${configPath}" --scale ${args.workers}`);
        return stdout ? stdout.trim() : JSON.stringify({ status: "scaled", target: args.workers }, null, 2);
      }
    });

    // Tool: monitor_agent
    server.addTool({
      name: "monitor_agent",
      description: "Get detailed information about a specific agent",
      parameters: z.object({
        agent_id: z.string().describe("ID of the agent to monitor")
      }),
      execute: async (args) => {
        // This would query the database for specific agent info
        // For now, return a simple response
        const result = {
          agent_id: args.agent_id,
          status: "not_implemented",
          message: "Agent monitoring not yet implemented"
        };
        return JSON.stringify(result, null, 2);
      }
    });

    // TODO: Add resources once we figure out the correct FastMCP API
    // For now, we have the essential tools working

    return server;
  }
}

// Main execution
async function main() {
  try {
    const mcp = new DistributedCodingMCP();
    const server = mcp.createServer();

    console.log("Starting Distributed Coding Orchestrator MCP Server...");

    // Start the server
    await server.start({
      transportType: "stdio"
    });

  } catch (error) {
    console.error("Failed to start MCP server:", error);
    process.exit(1);
  }
}

// Handle process termination
process.on("SIGINT", () => {
  console.log("Shutting down MCP server...");
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("Shutting down MCP server...");
  process.exit(0);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error("Unhandled error:", error);
    process.exit(1);
  });
}
