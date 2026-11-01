// A simulated multi-step Agent. No LLM is called: each step just waits, so the
// demo is free, fast, and deterministic. What matters is the lifecycle, not
// the model.

export type Emit = (type: string, data?: unknown) => void;

export type AgentOptions = {
  /** Duration of one step in milliseconds. */
  stepMs: number;
  /** Called when a step changes the outside world (writes a file, sends an email). */
  onSideEffect: (effect: string) => void;
};

const STEPS = ['search', 'read_documents', 'write_file', 'generate_report'] as const;

/** Resolves after `ms`, or rejects as soon as `signal` aborts. */
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);

    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);

    signal.addEventListener('abort', onAbort, { once: true });
  });
}

export async function runAgent(
  emit: Emit,
  signal: AbortSignal,
  options: AgentOptions
): Promise<string> {
  for (const step of STEPS) {
    emit('step.started', { step });

    if (step === 'generate_report') {
      // The LLM-streaming part of the run: ten deltas.
      for (let i = 1; i <= 10; i++) {
        await sleep(options.stepMs / 5, signal);
        emit('llm.delta', { text: `chunk-${i} ` });
      }
    } else {
      await sleep(options.stepMs, signal);
    }

    if (step === 'write_file') {
      // A side effect: once this happens, no HTTP cancel can take it back.
      options.onSideEffect('report.md written');
    }

    emit('step.completed', { step });
  }

  return 'research report: 20 documents summarized';
}
