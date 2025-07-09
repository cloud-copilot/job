import { Job, JobContext, JobResult, Logger } from './job.js'

/**
 * Runs the given jobs with up to `concurrency` tasks in flight at once.
 * Resolves with an array of results in the same order as the jobs.
 */
export async function runJobs<T = void, P = Record<string, unknown>>(
  jobs: Job<T, P>[],
  concurrency: number,
  logger: Logger
): Promise<JobResult<T, P>[]> {
  const results: JobResult<T, P>[] = []
  let nextIndex = 0
  if (concurrency == null || concurrency === undefined || concurrency <= 0) {
    throw new Error(`Invalid concurrency: ${concurrency}. Must be a positive integer.`)
  }

  // Each worker pulls the next available job, runs it, stores the result, then loops.
  async function worker(workerId: number) {
    while (true) {
      const i = nextIndex++
      if (i >= jobs.length) return

      const context: JobContext = {
        workerId
      }

      const startTime = Date.now()
      const interval = setInterval(() => {
        logger.warn(
          `Long-running job detected.`,
          { minutes: Math.floor((Date.now() - startTime) / 60000) },
          { ...context, ...jobs[i].properties }
        )
      }, 60_000)
      try {
        const value = await jobs[i].execute({ ...context, properties: jobs[i].properties })
        results[i] = { status: 'fulfilled', value, properties: jobs[i].properties }
      } catch (reason) {
        results[i] = { status: 'rejected', reason, properties: jobs[i].properties }
      } finally {
        clearInterval(interval)
      }
    }
  }

  // Create a pool of workers maxed at `concurrency` up to the number of jobs.
  const workers = Array(Math.min(concurrency, jobs.length))
    .fill(null)
    .map((_, idx) => worker(idx + 1))

  // Wait for all workers to finish
  await Promise.all(workers)

  return results
}
