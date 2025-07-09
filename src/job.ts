/**
 * Represents the outcome of a job:
 * - `fulfilled` with a `value` if it succeeded
 * - `rejected` with a `reason` if it threw.
 */
export type JobResult<T, P> =
  | { status: 'fulfilled'; value: T; properties: P }
  | { status: 'rejected'; reason: any; properties: P }

export interface JobContext {
  workerId: number
}

/**
 * Represents a job that can be executed
 */
export interface Job<T = void, P = Record<string, unknown>> {
  /**
   * Execute the job with the given context.
   *
   * @param context - The context for the job execution, see @link JobContext
   * @returns A promise that is resolved by the job's worker
   */
  execute: (props: JobContext & { properties: P }) => Promise<T>

  /**
   * Properties associated with the job, useful for logging or tracking.
   */
  properties: P
}

export interface Logger {
  warn: (...args: unknown[]) => void
}
