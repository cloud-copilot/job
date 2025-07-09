import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ConcurrentJobQueue } from './ConcurrentJobQueue.js'
import { Job, JobContext, Logger } from './job.js'

describe('ConcurrentJobQueue', () => {
  let mockLogger: Logger

  beforeEach(() => {
    mockLogger = {
      warn: vi.fn()
    }
    vi.clearAllTimers()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should execute jobs and record results', async () => {
    const queue = new ConcurrentJobQueue<string, Record<string, unknown>>(2, mockLogger)

    const jobs: Job<string>[] = [
      { execute: async () => 'result1', properties: { id: 1 } },
      { execute: async () => 'result2', properties: { id: 2 } },
      { execute: async () => 'result3', properties: { id: 3 } }
    ]

    queue.enqueueAll(jobs)
    await queue.waitForIdle()

    const results = queue.getResults()
    expect(results).toHaveLength(3)
    expect(results.map((r) => r.status)).toEqual(['fulfilled', 'fulfilled', 'fulfilled'])
    expect(results.map((r) => (r.status === 'fulfilled' ? r.value : null))).toEqual([
      'result1',
      'result2',
      'result3'
    ])
  })

  it('should respect concurrency limits', async () => {
    vi.useRealTimers()
    const queue = new ConcurrentJobQueue<void, Record<string, unknown>>(2, mockLogger)

    let activeJobs = 0
    let maxConcurrentJobs = 0

    const jobs: Job<void, Record<string, unknown>>[] = Array(5)
      .fill(null)
      .map((_, i) => ({
        execute: async () => {
          activeJobs++
          maxConcurrentJobs = Math.max(maxConcurrentJobs, activeJobs)
          await new Promise((resolve) => setTimeout(resolve, 50))
          activeJobs--
        },
        properties: { id: i }
      }))

    queue.enqueueAll(jobs)
    await queue.waitForIdle()

    expect(maxConcurrentJobs).toBe(2)
  })

  it('should handle job failures without stopping other jobs', async () => {
    const queue = new ConcurrentJobQueue<string, Record<string, unknown>>(2, mockLogger)

    const jobs: Job<string>[] = [
      { execute: async () => 'success1', properties: { id: 1 } },
      {
        execute: async () => {
          throw new Error('test error')
        },
        properties: { id: 2 }
      },
      { execute: async () => 'success2', properties: { id: 3 } }
    ]

    queue.enqueueAll(jobs)
    await queue.waitForIdle()

    const results = queue.getResults()
    expect(results).toHaveLength(3)
    expect(results[0]).toEqual({ status: 'fulfilled', value: 'success1', properties: { id: 1 } })
    expect(results[1]).toEqual({
      status: 'rejected',
      reason: expect.any(Error),
      properties: { id: 2 }
    })
    expect(results[2]).toEqual({ status: 'fulfilled', value: 'success2', properties: { id: 3 } })
  })

  it('should allow adding jobs incrementally', async () => {
    vi.useRealTimers()
    const queue = new ConcurrentJobQueue<string, Record<string, unknown>>(1, mockLogger)

    // Add first job
    queue.enqueue({ execute: async () => 'first', properties: { id: 1 } })

    // Wait a bit then add more jobs
    await new Promise<void>((resolve) => {
      setTimeout(() => {
        queue.enqueue({ execute: async () => 'second', properties: { id: 2 } })
        queue.enqueue({ execute: async () => 'third', properties: { id: 3 } })
        resolve()
      }, 10)
    })

    await queue.waitForIdle()

    const results = queue.getResults()
    expect(results).toHaveLength(3)
    expect(results.map((r) => (r.status === 'fulfilled' ? r.value : null))).toEqual([
      'first',
      'second',
      'third'
    ])
  })

  it('should pass correct context to jobs', async () => {
    const queue = new ConcurrentJobQueue(2, mockLogger)
    vi.useRealTimers()

    const contextCaptures: JobContext[] = []
    const jobs: Job<void>[] = [
      {
        execute: async (context) => {
          contextCaptures.push(context)
          // Simulate a delay so the next worker picks up the next job
          await new Promise((resolve) => setTimeout(resolve, 10))
        },
        properties: { id: 1 }
      },
      {
        execute: async (context) => {
          contextCaptures.push(context)
        },
        properties: { id: 2 }
      }
    ]

    queue.enqueueAll(jobs)
    await queue.waitForIdle()

    expect(contextCaptures).toHaveLength(2)
    expect(contextCaptures[0]).toEqual({ workerId: expect.any(Number), properties: { id: 1 } })
    expect(contextCaptures[1]).toEqual({ workerId: expect.any(Number), properties: { id: 2 } })

    // Worker IDs should be different for concurrent jobs
    const workerIds = contextCaptures.map((c) => c.workerId)
    expect(new Set(workerIds).size).toBeGreaterThan(1)
  })

  it('should track queue length and active job count', async () => {
    vi.useRealTimers()
    const queue = new ConcurrentJobQueue<string, Record<string, unknown>>(1, mockLogger)

    expect(queue.queueLength).toBe(0)
    expect(queue.activeJobCount).toBe(0)

    // Add jobs but don't wait
    queue.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
        return 'job1'
      },
      properties: {}
    })
    queue.enqueue({ execute: async () => 'job2', properties: {} })
    queue.enqueue({ execute: async () => 'job3', properties: {} })

    // Give a moment for first job to start
    await new Promise((resolve) => setTimeout(resolve, 10))

    expect(queue.activeJobCount).toBe(1)
    expect(queue.queueLength).toBe(2)

    await queue.waitForIdle()

    expect(queue.queueLength).toBe(0)
    expect(queue.activeJobCount).toBe(0)
  })

  it('should handle finishAllWork correctly', async () => {
    const queue = new ConcurrentJobQueue<string, Record<string, unknown>>(2, mockLogger)

    const jobs: Job<string>[] = [
      { execute: async () => 'result1', properties: { id: 1 } },
      { execute: async () => 'result2', properties: { id: 2 } }
    ]

    queue.enqueueAll(jobs)
    await queue.finishAllWork()

    const results = queue.getResults()
    expect(results).toHaveLength(2)
    expect(results.every((r) => r.status === 'fulfilled')).toBe(true)
  })

  it('should reject new jobs after finishAllWork', async () => {
    const queue = new ConcurrentJobQueue<string, Record<string, unknown>>(1, mockLogger)

    await queue.finishAllWork()

    expect(() => {
      queue.enqueue({ execute: async () => 'test', properties: {} })
    }).toThrow('Cannot enqueue jobs after shutdown')
  })

  it('should warn about long-running jobs', async () => {
    const queue = new ConcurrentJobQueue(1, mockLogger)

    const job: Job<void> = {
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 70000))
      },
      properties: { id: 'long-job' }
    }

    queue.enqueue(job)

    // Fast-forward time to trigger warning
    vi.advanceTimersByTime(61000)

    expect(mockLogger.warn).toHaveBeenCalledWith(
      'Long-running job detected.',
      { minutes: 1 },
      { workerId: 1, id: 'long-job' }
    )

    // Complete the job
    vi.advanceTimersByTime(10000)
    await queue.waitForIdle()
  })

  it('should handle empty queue gracefully', async () => {
    const queue = new ConcurrentJobQueue(2, mockLogger)

    await queue.waitForIdle()
    expect(queue.getResults()).toEqual([])
    expect(queue.queueLength).toBe(0)
    expect(queue.activeJobCount).toBe(0)
  })

  it('should handle multiple waitForIdle calls', async () => {
    vi.useRealTimers()
    const queue = new ConcurrentJobQueue<string, Record<string, unknown>>(1, mockLogger)

    queue.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
        return 'done'
      },
      properties: {}
    })

    const promise1 = queue.waitForIdle()
    const promise2 = queue.waitForIdle()

    await Promise.all([promise1, promise2])

    expect(queue.getResults()).toHaveLength(1)
  })
})
