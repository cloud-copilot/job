import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ConcurrentWorkerPool } from './ConcurrentWorkerPool.js'
import type { Job, JobContext, Logger } from './job.js'

describe('ConcurrentWorkerPool', () => {
  let mockLogger: Logger
  let pool: ConcurrentWorkerPool<string>

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

  it('should resolve individual job promises with correct values', async () => {
    pool = new ConcurrentWorkerPool(2, mockLogger)

    const job1Promise = pool.enqueue({ execute: async () => 'result1', properties: { id: 1 } })
    const job2Promise = pool.enqueue({ execute: async () => 'result2', properties: { id: 2 } })
    const job3Promise = pool.enqueue({ execute: async () => 'result3', properties: { id: 3 } })

    const [result3, result2, result1] = await Promise.all([job3Promise, job2Promise, job1Promise])

    expect(result1).toEqual({ status: 'fulfilled', value: 'result1', properties: { id: 1 } })
    expect(result2).toEqual({ status: 'fulfilled', value: 'result2', properties: { id: 2 } })
    expect(result3).toEqual({ status: 'fulfilled', value: 'result3', properties: { id: 3 } })
  })

  it('should respect concurrency limits', async () => {
    vi.useRealTimers()
    pool = new ConcurrentWorkerPool(2, mockLogger)

    let activeJobs = 0
    let maxConcurrentJobs = 0

    const jobs: Promise<any>[] = Array(5)
      .fill(null)
      .map((_, i) =>
        pool.enqueue({
          execute: async () => {
            activeJobs++
            maxConcurrentJobs = Math.max(maxConcurrentJobs, activeJobs)
            await new Promise((resolve) => setTimeout(resolve, 50))
            activeJobs--
            return `result${i}`
          },
          properties: { id: i }
        })
      )

    await Promise.all(jobs)

    expect(maxConcurrentJobs).toBe(2)
  })

  it('should handle job failures in individual promises', async () => {
    pool = new ConcurrentWorkerPool(2, mockLogger)

    const successPromise = pool.enqueue({ execute: async () => 'success', properties: { id: 1 } })
    const failurePromise = pool.enqueue({
      execute: async () => {
        throw new Error('test error')
      },
      properties: { id: 2 }
    })

    const [successResult, failureResult] = await Promise.all([successPromise, failurePromise])

    expect(successResult).toEqual({ status: 'fulfilled', value: 'success', properties: { id: 1 } })
    expect(failureResult).toEqual({
      status: 'rejected',
      reason: expect.any(Error),
      properties: { id: 2 }
    })
    expect((failureResult as any).reason.message).toBe('test error')
  })

  it('should resolve promises in completion order, not enqueue order', async () => {
    vi.useRealTimers()
    pool = new ConcurrentWorkerPool(2, mockLogger)

    const slowJobPromise = pool.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 100))
        return 'slow'
      },
      properties: { id: 1 }
    })

    const fastJobPromise = pool.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
        return 'fast'
      },
      properties: { id: 2 }
    })

    // Fast job should complete first
    const fastResult = await fastJobPromise
    if (fastResult.status !== 'fulfilled') {
      throw new Error('Fast job should have been fulfilled')
    }
    expect(fastResult.value).toBe('fast')

    const slowResult = await slowJobPromise
    if (slowResult.status !== 'fulfilled') {
      throw new Error('Slow job should have been fulfilled')
    }
    expect(slowResult.value).toBe('slow')
  })

  it('should pass correct context to jobs', async () => {
    vi.useRealTimers()
    pool = new ConcurrentWorkerPool(2, mockLogger)

    const contextCaptures: JobContext[] = []
    const promises = [
      pool.enqueue({
        execute: async (context) => {
          await new Promise((resolve) => setTimeout(resolve, 50))
          contextCaptures.push(context)
          return 'job1'
        },
        properties: { id: 1 }
      }),
      pool.enqueue({
        execute: async (context) => {
          await new Promise((resolve) => setTimeout(resolve, 50))
          contextCaptures.push(context)
          return 'job2'
        },
        properties: { id: 2 }
      })
    ]

    await Promise.all(promises)

    expect(contextCaptures).toHaveLength(2)
    expect(contextCaptures[0]).toEqual({ workerId: expect.any(Number), properties: { id: 1 } })
    expect(contextCaptures[1]).toEqual({ workerId: expect.any(Number), properties: { id: 2 } })

    // Worker IDs should be different for concurrent jobs
    const workerIds = contextCaptures.map((c) => c.workerId)
    expect(new Set(workerIds).size).toBeGreaterThan(1)
  })

  it('should track queue length and active job count', async () => {
    vi.useRealTimers()
    pool = new ConcurrentWorkerPool(1, mockLogger)

    expect(pool.queueLength).toBe(0)
    expect(pool.activeJobCount).toBe(0)

    // Add jobs but don't await them immediately
    const promise1 = pool.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
        return 'job1'
      },
      properties: {}
    })
    const promise2 = pool.enqueue({ execute: async () => 'job2', properties: {} })
    const promise3 = pool.enqueue({ execute: async () => 'job3', properties: {} })

    // Give a moment for first job to start
    await new Promise((resolve) => setTimeout(resolve, 10))

    expect(pool.activeJobCount).toBe(1)
    expect(pool.queueLength).toBe(2)

    await Promise.all([promise1, promise2, promise3])

    expect(pool.queueLength).toBe(0)
    expect(pool.activeJobCount).toBe(0)
  })

  it('should handle enqueueAll and return individual promises', async () => {
    pool = new ConcurrentWorkerPool(2, mockLogger)

    const jobs: Job<string>[] = [
      { execute: async () => 'result1', properties: { id: 1 } },
      { execute: async () => 'result2', properties: { id: 2 } },
      { execute: async () => 'result3', properties: { id: 3 } }
    ]

    const promises = pool.enqueueAll(jobs)
    const results = await Promise.all(promises)

    expect(promises).toHaveLength(3)
    expect(results).toHaveLength(3)
    expect(results.map((r) => (r as any).value)).toEqual(['result1', 'result2', 'result3'])
  })

  it('should handle waitForIdle correctly', async () => {
    vi.useRealTimers()
    pool = new ConcurrentWorkerPool(2, mockLogger)

    // Queue some jobs
    const promises = [
      pool.enqueue({
        execute: async () => {
          await new Promise((resolve) => setTimeout(resolve, 30))
          return 'job1'
        },
        properties: {}
      }),
      pool.enqueue({
        execute: async () => {
          await new Promise((resolve) => setTimeout(resolve, 50))
          return 'job2'
        },
        properties: {}
      })
    ]

    // Wait for idle should resolve when all jobs complete
    await pool.waitForIdle()

    // All jobs should be completed
    const results = await Promise.all(promises)
    expect(results).toHaveLength(2)
    expect(pool.queueLength).toBe(0)
    expect(pool.activeJobCount).toBe(0)
  })

  it('should handle finishAllWork correctly', async () => {
    pool = new ConcurrentWorkerPool(2, mockLogger)

    const promises = [
      pool.enqueue({ execute: async () => 'result1', properties: { id: 1 } }),
      pool.enqueue({ execute: async () => 'result2', properties: { id: 2 } })
    ]

    await pool.finishAllWork()

    const results = await Promise.all(promises)
    expect(results).toHaveLength(2)
    expect(results.every((r) => r.status === 'fulfilled')).toBe(true)
  })

  it('should reject new jobs after finishAllWork', async () => {
    pool = new ConcurrentWorkerPool(1, mockLogger)

    await pool.finishAllWork()

    expect(() => {
      pool.enqueue({ execute: async () => 'test', properties: {} })
    }).toThrow('Cannot enqueue jobs after shutdown')
  })

  it('should warn about long-running jobs', async () => {
    pool = new ConcurrentWorkerPool(1, mockLogger)

    const jobPromise = pool.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 70000))
        return 'done'
      },
      properties: { id: 'long-job' }
    })

    // Fast-forward time to trigger warning
    vi.advanceTimersByTime(61000)

    expect(mockLogger.warn).toHaveBeenCalledWith(
      'Long-running job detected.',
      { minutes: 1 },
      { workerId: 1, id: 'long-job' }
    )

    // Complete the job
    vi.advanceTimersByTime(10000)
    const result = await jobPromise

    if (result.status !== 'fulfilled') {
      throw new Error('Job should have been fulfilled')
    }
    expect(result.value).toBe('done')
  })

  it('should handle multiple waitForIdle calls', async () => {
    vi.useRealTimers()
    pool = new ConcurrentWorkerPool(1, mockLogger)

    const jobPromise = pool.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
        return 'done'
      },
      properties: {}
    })

    const idle1 = pool.waitForIdle()
    const idle2 = pool.waitForIdle()

    await Promise.all([idle1, idle2, jobPromise])

    expect(pool.queueLength).toBe(0)
    expect(pool.activeJobCount).toBe(0)
  })

  it('should handle concurrent job additions during processing', async () => {
    vi.useRealTimers()
    pool = new ConcurrentWorkerPool(1, mockLogger)

    const results: string[] = []

    // Start first job
    const promise1 = pool.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
        return 'first'
      },
      properties: {}
    })

    // Add more jobs while first is processing
    setTimeout(() => {
      results.push('adding-second')
    }, 25)

    const promise2 = pool.enqueue({ execute: async () => 'second', properties: {} })
    const promise3 = pool.enqueue({ execute: async () => 'third', properties: {} })

    const [result1, result2, result3] = await Promise.all([promise1, promise2, promise3])

    if (result1.status !== 'fulfilled') {
      throw new Error('First job should have been fulfilled')
    }
    expect(result1.value).toBe('first')

    if (result2.status !== 'fulfilled') {
      throw new Error('Second job should have been fulfilled')
    }
    expect(result2.value).toBe('second')
    if (result3.status !== 'fulfilled') {
      throw new Error('Third job should have been fulfilled')
    }
    expect(result3.value).toBe('third')
  })

  it('should clean up job resolvers after completion', async () => {
    pool = new ConcurrentWorkerPool(1, mockLogger)

    const promise = pool.enqueue({ execute: async () => 'test', properties: {} })

    // Access private property to verify cleanup (this is a bit of a hack for testing)
    const resolveMapSize = Object.keys((pool as any).resolveMap).length
    expect(resolveMapSize).toBe(1)

    await promise

    const resolveMapSizeAfter = Object.keys((pool as any).resolveMap).length
    expect(resolveMapSizeAfter).toBe(0)
  })
})
