import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { pathToFileURL } from 'node:url';
import { runAgent, type Emit } from './agent.js';
import { TaskRegistry, isTerminalEvent, type TaskEvent } from './tasks.js';

export type ServerOptions = {
  port?: number;
  stepMs?: number;
  heartbeatMs?: number;
  /**
   * How long a subscriber may leave a write unflushed before it is dropped.
   * A client that stops reading makes `res.write()` return false; if the
   * socket does not drain within this time, the subscription is evicted.
   */
  drainTimeoutMs?: number;
  /** Pads each heartbeat so a non-reading client fills its socket buffer quickly. Demo only. */
  heartbeatPadBytes?: number;
};

export type BoundOutcome = {
  finished: boolean;
  lastStep?: string;
  reason?: string;
};

const SSE_HEADERS = {
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache',
  Connection: 'keep-alive',
};

function formatEvent(event: TaskEvent): string {
  return `id: ${event.id}\nevent: ${event.type}\ndata: ${JSON.stringify(event.data ?? null)}\n\n`;
}

function json(res: http.ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

export async function startServer(options: ServerOptions = {}) {
  const stepMs = options.stepMs ?? 400;
  const heartbeatMs = options.heartbeatMs ?? 15_000;
  const drainTimeoutMs = options.drainTimeoutMs ?? 5_000;
  const heartbeatPad = '.'.repeat(options.heartbeatPadBytes ?? 0);

  const registry = new TaskRegistry({ stepMs });

  // What the request-bound endpoint did, for the demo to inspect.
  const bound: { last?: BoundOutcome; ledger: string[] } = { ledger: [] };

  // Subscriptions dropped because the client stopped reading.
  const stats = { evictedSubscribers: 0 };

  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const route = `${req.method} ${url.pathname}`;

    // ── Request-bound: the connection owns the Agent ─────────────────
    if (route === 'POST /api/bound-agent') {
      const controller = new AbortController();

      // Use the response's 'close', not the request's. The request's 'close'
      // fires once the request body has been read, which says nothing about
      // the browser still being there. A response that closes before it has
      // ended means the connection dropped.
      res.on('close', () => {
        if (!res.writableEnded) controller.abort(new Error('client disconnected'));
      });

      res.writeHead(200, SSE_HEADERS);

      let sequence = 0;
      let lastStep: string | undefined;
      const emit: Emit = (type, data) => {
        if (type === 'step.started') lastStep = (data as { step: string }).step;
        res.write(formatEvent({ id: ++sequence, type, data }));
      };

      runAgent(emit, controller.signal, {
        stepMs,
        onSideEffect: (effect) => bound.ledger.push(effect),
      })
        .then((result) => {
          emit('task.completed', { result });
          bound.last = { finished: true, lastStep };
          res.end();
        })
        .catch((error: unknown) => {
          bound.last = {
            finished: false,
            lastStep,
            reason: error instanceof Error ? error.message : String(error),
          };
          res.end();
        });
      return;
    }

    // ── Task-based: the task owns the Agent ──────────────────────────
    if (route === 'POST /api/agents') {
      const task = registry.create();
      return json(res, 202, { taskId: task.id });
    }

    const match = url.pathname.match(/^\/api\/agents\/([\w-]+)(\/events|\/cancel)?$/);
    if (match) {
      const [, id, suffix] = match;
      const task = registry.get(id!);
      if (!task) return json(res, 404, { error: 'unknown task' });

      if (req.method === 'GET' && !suffix) {
        return json(res, 200, {
          id: task.id,
          status: task.status,
          eventCount: task.events.length,
          result: task.result ?? null,
        });
      }

      if (req.method === 'POST' && suffix === '/cancel') {
        return json(res, 200, registry.cancel(task.id));
      }

      if (req.method === 'GET' && suffix === '/events') {
        // A reconnecting client sends the id of the last event it saw
        // (`Last-Event-ID`, which EventSource sets automatically).
        const after = Number(req.headers['last-event-id'] ?? url.searchParams.get('after') ?? 0);

        res.writeHead(200, SSE_HEADERS);
        let drainTimer: NodeJS.Timeout | undefined;

        // A one-way stream cannot tell a quiet client from a dead one by
        // listening, and heartbeats keep the socket looking active. What gives
        // a stuck reader away is backpressure: its socket stops draining, so
        // `write()` returns false and stays that way. Give it a moment, then
        // drop the subscription. The task is not touched.
        const send = (chunk: string) => {
          if (res.write(chunk) || drainTimer) return;
          drainTimer = setTimeout(() => {
            stats.evictedSubscribers++;
            res.destroy();
          }, drainTimeoutMs);
          res.once('drain', () => {
            clearTimeout(drainTimer);
            drainTimer = undefined;
          });
        };

        for (const event of task.events) if (event.id > after) send(formatEvent(event));
        if (task.status !== 'running') return void res.end();

        const heartbeat = setInterval(() => send(`: heartbeat${heartbeatPad}\n\n`), heartbeatMs);
        const listener = (event: TaskEvent) => {
          send(formatEvent(event));
          if (isTerminalEvent(event)) {
            cleanup();
            res.end();
          }
        };
        const cleanup = () => {
          clearInterval(heartbeat);
          clearTimeout(drainTimer);
          task.listeners.delete(listener);
        };
        task.listeners.add(listener);

        // A disconnect ends this subscription. It does not touch the task.
        res.on('close', cleanup);
        return;
      }
    }

    json(res, 404, { error: 'not found' });
  });

  await new Promise<void>((resolve) => server.listen(options.port ?? 0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;

  return {
    url: `http://127.0.0.1:${port}`,
    registry,
    bound,
    stats,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

// `pnpm server` starts it standalone for poking with curl.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { url } = await startServer({ port: Number(process.env.PORT ?? 3000) });
  console.log(`listening on ${url}`);
  console.log('  POST /api/bound-agent        request-bound Agent (connection owns it)');
  console.log('  POST /api/agents             create a task, returns { taskId }');
  console.log('  GET  /api/agents/:id         task status');
  console.log('  GET  /api/agents/:id/events  subscribe (resume with Last-Event-ID)');
  console.log('  POST /api/agents/:id/cancel  explicit cancel');
}
