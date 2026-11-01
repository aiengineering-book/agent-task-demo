import { randomUUID } from 'node:crypto';
import { runAgent, type AgentOptions } from './agent.js';

export type TaskStatus = 'running' | 'completed' | 'failed' | 'cancelled';

export type TaskEvent = {
  /** Sequential per task, starting at 1. Doubles as the SSE `id`, so a client can resume from it. */
  id: number;
  type: string;
  data?: unknown;
};

export type Task = {
  id: string;
  status: TaskStatus;
  events: TaskEvent[];
  result?: string;
  error?: string;
  /** What this task has already changed in the outside world. */
  sideEffects: string[];
  controller: AbortController;
  listeners: Set<(event: TaskEvent) => void>;
  done: Promise<void>;
};

const TERMINAL = new Set(['task.completed', 'task.cancelled', 'task.failed']);
export const isTerminalEvent = (event: TaskEvent) => TERMINAL.has(event.type);

/**
 * What a cancel actually did. "Cancelled" alone hides the difference between
 * stopping work that had changed nothing and stopping work that had already
 * committed a side effect.
 */
export type CancelOutcome = 'step_cancelled' | 'side_effect_committed';
export type CancelResult = { cancelled: boolean; outcome?: CancelOutcome };

/**
 * In-memory task registry. The task, not the HTTP connection, owns the Agent:
 * connections subscribe to `task.events`, and dropping one changes nothing here.
 *
 * In-memory keeps the demo to one file. A real system persists tasks in a
 * database and runs the Agent in a worker, so it also survives a server restart.
 */
export class TaskRegistry {
  readonly tasks = new Map<string, Task>();
  /** Everything the Agent changed in the outside world. */
  readonly ledger: string[] = [];

  constructor(private readonly options: Pick<AgentOptions, 'stepMs'>) {}

  create(): Task {
    const task: Task = {
      id: `task_${randomUUID().slice(0, 8)}`,
      status: 'running',
      events: [],
      sideEffects: [],
      controller: new AbortController(),
      listeners: new Set(),
      done: Promise.resolve(),
    };
    this.tasks.set(task.id, task);

    const emit = (type: string, data?: unknown) => {
      const event: TaskEvent = { id: task.events.length + 1, type, data };
      task.events.push(event);
      for (const listener of task.listeners) listener(event);
    };

    task.done = (async () => {
      emit('task.started');
      try {
        task.result = await runAgent(emit, task.controller.signal, {
          stepMs: this.options.stepMs,
          onSideEffect: (effect) => {
            task.sideEffects.push(effect);
            this.ledger.push(effect);
          },
        });
        task.status = 'completed';
        emit('task.completed', { result: task.result });
      } catch (error) {
        if (task.controller.signal.aborted) {
          task.status = 'cancelled';
          emit('task.cancelled');
        } else {
          task.status = 'failed';
          task.error = String(error);
          emit('task.failed', { error: task.error });
        }
      }
    })();

    return task;
  }

  get(id: string): Task | undefined {
    return this.tasks.get(id);
  }

  /** An explicit user decision, unlike a disconnect. */
  cancel(id: string): CancelResult {
    const task = this.tasks.get(id);
    if (!task || task.status !== 'running') return { cancelled: false };

    const outcome: CancelOutcome =
      task.sideEffects.length > 0 ? 'side_effect_committed' : 'step_cancelled';
    task.controller.abort(new Error('cancelled by user'));
    return { cancelled: true, outcome };
  }
}
