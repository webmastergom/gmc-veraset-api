/**
 * In-process registry of AbortControllers per job for cooperative cancellation.
 * Cancel endpoint calls abort(); orchestrator passes signal to copier.
 */

const controllers = new Map<string, AbortController>();

export function registerAbortController(jobId: string): AbortController {
  const existing = controllers.get(jobId);
  if (existing) {
    existing.abort();
  }
  const controller = new AbortController();
  controllers.set(jobId, controller);
  return controller;
}

export function getAbortController(jobId: string): AbortController | undefined {
  return controllers.get(jobId);
}

export function abortSync(jobId: string): boolean {
  const controller = controllers.get(jobId);
  if (controller) {
    controller.abort();
    return true;
  }
  return false;
}

export function unregisterAbortController(jobId: string): void {
  controllers.delete(jobId);
}
