import assert from 'node:assert/strict';
import { startServer } from './server.js';

const STEP_MS = 300;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type SseEvent = { id?: number; type: string; data: any };
type Subscription = { events: SseEvent[]; heartbeats: number; endedBy: 'server' | 'client' };

/**
 * Reads a Server-Sent Events response. If `disconnectWhen` returns true for an
 * event, the client drops the connection, the way a closed tab does.
 */
async function readSse(
  start: (signal: AbortSignal) => Promise<Response>,
  disconnectWhen: (event: SseEvent) => boolean = () => false
): Promise<Subscription> {
  const controller = new AbortController();
  const result: Subscription = { events: [], heartbeats: 0, endedBy: 'server' };

  try {
    const response = await start(controller.signal);
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const blocks = buffer.split('\n\n');
      buffer = blocks.pop() ?? '';

      for (const block of blocks) {
        const fields: Record<string, string> = {};
        for (const line of block.split('\n')) {
          if (line.startsWith(':')) result.heartbeats++;
          else if (line.includes(': ')) {
            const [key, ...rest] = line.split(': ');
            fields[key!] = rest.join(': ');
          }
        }
        if (!fields.event) continue;

        const event: SseEvent = {
          id: fields.id ? Number(fields.id) : undefined,
          type: fields.event,
          data: JSON.parse(fields.data ?? 'null'),
        };
        result.events.push(event);

        if (disconnectWhen(event)) {
          result.endedBy = 'client';
          controller.abort();
          return result;
        }
      }
    }
  } catch (error) {
    if (!(error instanceof Error && error.name === 'AbortError')) throw error;
    result.endedBy = 'client';
  }
  return result;
}

const afterReadingDocuments = (e: SseEvent) =>
  e.type === 'step.completed' && e.data?.step === 'read_documents';

const { url, registry, bound, close } = await startServer({
  stepMs: STEP_MS,
  heartbeatMs: 100,
});

const rows: [string, string][] = [];

try {
  // ── A. Request-bound: closing the tab kills the Agent ─────────────
  console.log('\nA. Request-bound Agent, client disconnects after two steps');
  await readSse(
    (signal) => fetch(`${url}/api/bound-agent`, { method: 'POST', signal }),
    afterReadingDocuments
  );
  await sleep(150); // let the server notice the disconnect

  console.log('   server outcome:', bound.last);
  console.log('   side effects:  ', bound.ledger);
  assert.equal(bound.last?.finished, false);
  assert.equal(bound.last?.lastStep, 'write_file');
  assert.deepEqual(bound.ledger, []);
  rows.push(['A. request-bound, tab closed', 'Agent aborted in write_file, report lost']);

  // ── B. Task-based: the Agent survives, the client resumes ─────────
  console.log('\nB. Task-based Agent, client disconnects after two steps, then reconnects');
  const created = await fetch(`${url}/api/agents`, { method: 'POST' });
  const { taskId } = (await created.json()) as { taskId: string };
  console.log('   task:', taskId);

  const first = await readSse(
    (signal) => fetch(`${url}/api/agents/${taskId}/events`, { signal }),
    afterReadingDocuments
  );
  const lastSeen = first.events.at(-1)!.id!;
  console.log(`   first connection saw events 1..${lastSeen}, then dropped`);

  await sleep(80);
  const task = registry.get(taskId)!;
  assert.equal(task.status, 'running', 'disconnect must not stop the task');
  assert.equal(task.listeners.size, 0, 'the dropped subscription must be removed');
  console.log(`   after the drop: status=${task.status}, subscribers=${task.listeners.size}`);

  const second = await readSse((signal) =>
    fetch(`${url}/api/agents/${taskId}/events`, {
      headers: { 'Last-Event-ID': String(lastSeen) },
      signal,
    })
  );
  console.log(`   second connection resumed at event ${second.events[0]!.id}, ` +
    `saw ${second.events.length} events and ${second.heartbeats} heartbeats`);

  const ids = [...first.events, ...second.events].map((e) => e.id);
  assert.deepEqual(ids, ids.map((_, i) => i + 1), 'no gaps and no duplicates across the reconnect');
  assert.equal(second.events.at(-1)!.type, 'task.completed');
  assert.ok(second.heartbeats >= 1, 'idle stream should carry heartbeats');

  const status = (await (await fetch(`${url}/api/agents/${taskId}`)).json()) as any;
  console.log('   final status:', status.status, '|', status.result);
  assert.equal(status.status, 'completed');
  assert.deepEqual(registry.ledger, ['report.md written'], 'side effect happened exactly once');
  rows.push(['B. task, tab closed, reconnected', 'Agent completed, events 1..N with no gaps']);

  // ── C. Cancel is explicit, and separate from disconnect ───────────
  console.log('\nC. Explicit cancel');
  const ledgerBefore = registry.ledger.length;
  const c = (await (await fetch(`${url}/api/agents`, { method: 'POST' })).json()) as { taskId: string };
  await sleep(STEP_MS * 1.5);

  const cancelled = (await (await fetch(`${url}/api/agents/${c.taskId}/cancel`, { method: 'POST' })).json()) as any;
  await registry.get(c.taskId)!.done;
  const cTask = registry.get(c.taskId)!;
  console.log('   cancel response:', cancelled, '| status:', cTask.status);
  assert.equal(cancelled.cancelled, true);
  assert.equal(cTask.status, 'cancelled');
  assert.equal(cTask.events.at(-1)!.type, 'task.cancelled');
  assert.equal(registry.ledger.length, ledgerBefore, 'cancelled before write_file: no side effect');
  rows.push(['C. task, POST /cancel', 'Agent cancelled before write_file, no side effect']);

  console.log('\nSummary');
  for (const [scenario, outcome] of rows) console.log(`  ${scenario.padEnd(36)} ${outcome}`);
  console.log('\nAll checks passed.');
} finally {
  await close();
}
