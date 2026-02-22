import { type Job, type JobContext, type JobResult, type Logger } from './job.js'

interface JobWithId<T, P> extends Job<T, P> {
  concurrentJobId: number
}

/**
 * A worker pool that will run jobs concurrently using promises up to a specified limit.
 * If no work is being done, it will wait for jobs to be added and run them up to the
 * maximum concurrency.
 *
 * This is designed to create a central queue for jobs of many types to be processed
 * in a streaming fashion, allowing for efficient resource usage and managed network requests.
 *
 */
export class ConcurrentWorkerPool<T = void, P = Record<string, unknown>> {
  private jobCounter = 0
  private resolveMap: Record<number, (value: JobResult<T, P>) => void> = {}
  private queue: JobWithId<T, P>[] = []
  private activeJobs = 0
  private waitingResolvers: (() => void)[] = []
  private workers: Promise<void>[] = []
  private isAcceptingWork = true
  private workAvailablePromise: Promise<void> | null = null
  private resolveWorkAvailable: (() => void) | null = null

  /**
   * Create a new runner with the specified concurrency.
   *
   * @param concurrency - The maximum number of jobs to run concurrently.
   * @param logger - Logger instance for logging long-running jobs.
   */
  constructor(
    private concurrency: number,
    private logger: Logger
  ) {}

  private async worker(workerId: number): Promise<void> {
    while (this.isAcceptingWork || this.queue.length > 0) {
      const job = this.queue.shift()
      if (!job) {
        if (!this.isAcceptingWork) {
          // No longer accepting work and no jobs left, exit immediately
          return
        }
        // No work available, wait for new work to be added
        await this.waitForWorkAvailable()
        continue
      }

      this.activeJobs++
      const context: JobContext = { workerId }

      const startTime = Date.now()
      const interval = setInterval(() => {
        this.logger.warn(
          `Long-running job detected.`,
          { minutes: Math.floor((Date.now() - startTime) / 60000) },
          { ...context, ...job.properties }
        )
      }, 60_000)

      const resolve = this.resolveMap[job.concurrentJobId]
      try {
        const value = await job.execute({ ...context, properties: job.properties })
        resolve({ status: 'fulfilled', value, properties: job.properties })
      } catch (reason) {
        resolve({ status: 'rejected', reason, properties: job.properties })
      } finally {
        clearInterval(interval)
        this.activeJobs--
        this.checkIfIdle()
        delete this.resolveMap[job.concurrentJobId]
      }
    }
  }

  private waitForWorkAvailable(): Promise<void> {
    if (!this.workAvailablePromise) {
      this.workAvailablePromise = new Promise<void>((resolve) => {
        this.resolveWorkAvailable = resolve
      })
    }
    return this.workAvailablePromise
  }

  private ensureWorkers(): void {
    if (this.workers.length === 0 && this.isAcceptingWork) {
      for (let i = 0; i < this.concurrency; i++) {
        this.workers.push(this.worker(i + 1))
      }
    }
  }

  private notifyWorkersOfNewWork(): void {
    // Wake up waiting workers
    if (this.resolveWorkAvailable) {
      this.resolveWorkAvailable()
      this.workAvailablePromise = null
      this.resolveWorkAvailable = null
    }
  }

  private checkIfIdle(): void {
    if (this.activeJobs === 0 && this.queue.length === 0) {
      // Notify all waiting resolvers
      this.waitingResolvers.forEach((resolve) => resolve())
      this.waitingResolvers = []
    }
  }

  /**
   * Add a job to the queue
   */
  enqueue(job: Job<T, P>): Promise<JobResult<T, P>> {
    if (!this.isAcceptingWork) {
      throw new Error('Cannot enqueue jobs after shutdown')
    }

    return new Promise<JobResult<T, P>>((resolve) => {
      const jobId = this.jobCounter++
      const jobWithId: JobWithId<T, P> = {
        ...job,
        concurrentJobId: jobId
      }
      this.resolveMap[jobId] = resolve

      this.queue.push(jobWithId)
      this.ensureWorkers()
      this.notifyWorkersOfNewWork()
    })
  }

  /**
   * Add multiple jobs to the queue
   */
  enqueueAll(jobs: Job<T, P>[]): Promise<JobResult<T, P>>[] {
    return jobs.map((job) => this.enqueue(job))
  }

  /**
   * Returns a promise that resolves when all queued work is complete
   */
  waitForIdle(): Promise<void> {
    return new Promise((resolve) => {
      if (this.activeJobs === 0 && this.queue.length === 0) {
        resolve()
      } else {
        this.waitingResolvers.push(resolve)
      }
    })
  }

  /**
   * Shutdown the queue - no new jobs will be accepted, but existing jobs will complete.
   *
   * Returns when a promise that resolves when all jobs have been processed and
   * the onComplete callback has been called for each job.
   */
  async finishAllWork(): Promise<void> {
    this.isAcceptingWork = false
    // Wake up any sleeping workers so they can process remaining jobs or exit
    this.notifyWorkersOfNewWork()
    // Check if we're already idle and notify any waiting resolvers
    await Promise.all(this.workers)
    this.workers = []
  }

  /**
   * Get the current queue length
   */
  get queueLength(): number {
    return this.queue.length
  }

  /**
   * Get the number of currently active jobs
   */
  get activeJobCount(): number {
    return this.activeJobs
  }
}
