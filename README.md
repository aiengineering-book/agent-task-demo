# Agent Task Demo

Runnable companion to the article **Your AI Agent Doesn't Belong Inside an HTTP Request**.

It runs the same simulated Agent two ways and shows what happens when the browser disconnects:

| Scenario | How the Agent runs | Browser disconnects |
|---|---|---|
| A. Request-bound | Inside `POST /api/bound-agent`. The connection owns it. | The Agent is aborted partway through. The work is lost. |
| B. Task-based | In a task created by `POST /api/agents`. The browser only subscribes to its events. | The subscription ends. The task keeps running. A reconnect resumes from the last event it saw. |
| C. Explicit cancel | `POST /api/agents/:id/cancel` | The task stops, and nothing after that point happens. The response says `step_cancelled`. |
| D. Cancel after a side effect | `POST /api/agents/:id/cancel`, sent after `write_file` finished | The task stops, but the file is already written. The response says `side_effect_committed`. |
| E. Subscriber stops reading | A client connects to the event stream and never reads | The server evicts that subscription. The task keeps running and completes. |

The Agent does not call an LLM. Each step waits a few hundred milliseconds, so the demo is free and deterministic. The lifecycle is the point, not the model.

## Run it

Requires Node.js 20 or later.

```bash
npm install     # or pnpm install
npm run demo
```

The demo starts the server, runs the five scenarios, and checks each outcome with assertions. It exits non-zero if any check fails. It takes about 10 seconds.

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
- `src/demo.ts`: the five scenarios and their assertions.

## Three details worth knowing

- **Detect a dropped connection on the response, not the request.** `res.on('close')` fires when the connection drops. `req.on('close')` fires once the request body has been read, which says nothing about the browser still being there. The request-bound handler checks `res.writableEnded` to tell a drop from a normal finish.
- **A cancel should say what it did.** `POST /cancel` returns an `outcome`. `step_cancelled` means the task had changed nothing yet. `side_effect_committed` means a step had already finished its side effect, which the cancel cannot undo. A bare `cancelled: true` hides that difference.
- **Evict stuck readers on backpressure, not on an idle timer.** An SSE stream is one-way, so the server never hears from the client, and its own heartbeats keep the socket looking active. A client that stopped reading shows up differently: its socket stops draining and `res.write()` returns `false`. The `/events` handler gives it `drainTimeoutMs` to catch up, then destroys that response. Scenario E uses padded heartbeats so a non-reading client fills its buffer in under a second.
- **Event ids make reconnecting safe.** Every event has a sequential id, sent as the SSE `id`. A client that reconnects sends `Last-Event-ID`, and the server replays only what it missed. The demo checks that the ids across both connections have no gaps and no duplicates.

## Limits

This is a teaching demo, not a template for production.

- Tasks and events live in memory in one process. A server restart loses them. A real system stores tasks in a database and runs the Agent in a worker, so work also survives a restart.
- The event list grows without bound.
- There is no authentication, no per-user task ownership, and no retry or recovery after a worker crash.
- Scenario E tests a client that stays connected but stops reading on loopback. A client whose network vanished without closing the connection (a half-open TCP connection) is not tested. It should look the same once the socket buffers fill, and TCP keepalive can detect it sooner, but neither is exercised here.
- Heartbeats are sent, and the demo checks they appear on an idle stream. It does not check that they stop a proxy or load balancer from closing an idle connection, because there is no proxy in front of it.
- There is no queue, so "cancelled before it started" cannot occur.
- The side effect is an entry in an in-memory array. Real side effects (emails, API calls) need idempotency keys so a retry doesn't repeat them.
