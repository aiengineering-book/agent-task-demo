# Agent Task Demo

Runnable companion to the article **Your AI Agent Doesn't Belong Inside an HTTP Request**.

It runs the same simulated Agent two ways and shows what happens when the browser disconnects:

| Scenario | How the Agent runs | Browser disconnects |
|---|---|---|
| A. Request-bound | Inside `POST /api/bound-agent`. The connection owns it. | The Agent is aborted partway through. The work is lost. |
| B. Task-based | In a task created by `POST /api/agents`. The browser only subscribes to its events. | The subscription ends. The task keeps running. A reconnect resumes from the last event it saw. |
| C. Explicit cancel | `POST /api/agents/:id/cancel` | The task stops, and nothing after that point happens. |

The Agent does not call an LLM. Each step waits a few hundred milliseconds, so the demo is free and deterministic. The lifecycle is the point, not the model.

## Run it

Requires Node.js 20 or later.

```bash
npm install     # or pnpm install
npm run demo
```

The demo starts the server, runs the three scenarios, and checks each outcome with assertions. It exits non-zero if any check fails. It takes about 5 seconds.

To poke at the server yourself:

```bash
npm run server        # listens on http://127.0.0.1:3000

curl -X POST localhost:3000/api/agents
# {"taskId":"task_1a2b3c4d"}

curl -N localhost:3000/api/agents/task_1a2b3c4d/events
# Ctrl-C to disconnect. The task keeps running.

curl -N -H 'Last-Event-ID: 5' localhost:3000/api/agents/task_1a2b3c4d/events
# resumes after event 5
```

## What to read

- `src/agent.ts`: the simulated Agent. It checks an `AbortSignal` between and during steps, and records a side effect (`report.md written`) that no HTTP cancel can undo.
- `src/tasks.ts`: the task registry. A task owns its `AbortController` and an append-only event list, and connections subscribe to that list.
- `src/server.ts`: the routes. Compare the `bound-agent` handler, which aborts on disconnect, with the `/events` handler, which only removes a subscriber.
- `src/demo.ts`: the three scenarios and their assertions.

## Two details worth knowing

- **Detect a dropped connection on the response, not the request.** `res.on('close')` fires when the connection drops. `req.on('close')` fires once the request body has been read, which says nothing about the browser still being there. The request-bound handler checks `res.writableEnded` to tell a drop from a normal finish.
- **Event ids make reconnecting safe.** Every event has a sequential id, sent as the SSE `id`. A client that reconnects sends `Last-Event-ID`, and the server replays only what it missed. The demo checks that the ids across both connections have no gaps and no duplicates.

## Limits

This is a teaching demo, not a template for production.

- Tasks and events live in memory in one process. A server restart loses them. A real system stores tasks in a database and runs the Agent in a worker, so work also survives a restart.
- The event list grows without bound.
- There is no authentication, no per-user task ownership, and no retry or recovery after a worker crash.
- Idle subscribers are kept alive with heartbeat comments, but the demo has no server-side idle timeout.
- The side effect is an entry in an in-memory array. Real side effects (emails, API calls) need idempotency keys so a retry doesn't repeat them.
