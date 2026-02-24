import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Job, JobContext, Logger } from './job.js'
import { runJobs } from './runJobs.js'

describe('runJobs', () => {
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

  it('should run jobs and return results in order', async () => {
    const jobs: Job<string>[] = [
      { execute: async () => 'result1', properties: { id: 1 } },
      { execute: async () => 'result2', properties: { id: 2 } },
      { execute: async () => 'result3', properties: { id: 3 } }
    ]

    const results = await runJobs(jobs, 2, mockLogger)

    expect(results).toHaveLength(3)
    expect(results[0]).toEqual({ status: 'fulfilled', value: 'result1', properties: { id: 1 } })
    expect(results[1]).toEqual({ status: 'fulfilled', value: 'result2', properties: { id: 2 } })
    expect(results[2]).toEqual({ status: 'fulfilled', value: 'result3', properties: { id: 3 } })
  })

  it('should respect concurrency limits', async () => {
    vi.useRealTimers() // Use real timers for this test

    let activeJobs = 0
    let maxConcurrentJobs = 0

    const jobs: Job<void>[] = Array(5)
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

    await runJobs(jobs, 2, mockLogger)

    expect(maxConcurrentJobs).toBe(2)
  })

  it('should handle job failures and continue processing', async () => {
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

    const results = await runJobs(jobs, 2, mockLogger)

    expect(results[0]).toEqual({ status: 'fulfilled', value: 'success1', properties: { id: 1 } })
    expect(results[1]).toEqual({
      status: 'rejected',
      reason: expect.any(Error),
      properties: { id: 2 }
    })
    expect(results[2]).toEqual({ status: 'fulfilled', value: 'success2', properties: { id: 3 } })
  })

  it('should pass correct context to jobs', async () => {
    const contextCaptures: JobContext[] = []

    const jobs: Job<void>[] = [
      {
        execute: async (context) => {
          contextCaptures.push(context)
        },
        properties: { id: 1 }
      }
    ]

    await runJobs(jobs, 1, mockLogger)

    expect(contextCaptures[0]).toEqual({
      workerId: 1,
      properties: { id: 1 }
    })
  })

  it('should handle empty job array', async () => {
    const results = await runJobs([], 2, mockLogger)
    expect(results).toEqual([])
  })

  it('should throw error for zero concurrency', async () => {
    const jobs: Job<string>[] = [{ execute: async () => 'result', properties: {} }]

    await expect(runJobs(jobs, 0, mockLogger)).rejects.toThrow(
      'Invalid concurrency: 0. Must be a positive integer.'
    )
  })

  it('should warn about long-running jobs', async () => {
    const jobs: Job<void>[] = [
      {
        execute: async () => {
          // Job that takes longer than 1 minute
          await new Promise((resolve) => setTimeout(resolve, 70000))
        },
        properties: { id: 'long-job' }
      }
    ]

    const promise = runJobs(jobs, 1, mockLogger)

    // Fast-forward time to trigger warning
    vi.advanceTimersByTime(61000)

    expect(mockLogger.warn).toHaveBeenCalledWith(
      'Long-running job detected.',
      { minutes: 1 },
      { workerId: 1, id: 'long-job' }
    )

    // Complete the job
    vi.advanceTimersByTime(10000)
    await promise
  })

  it('should assign different worker IDs', async () => {
    vi.useRealTimers()
    const workerIds: number[] = []

    const jobs: Job<void>[] = Array(3)
      .fill(null)
      .map(() => ({
        execute: async (context) => {
          workerIds.push(context.workerId)
          await new Promise((resolve) => setTimeout(resolve, 10))
        },
        properties: {}
      }))

    await runJobs(jobs, 3, mockLogger)

    expect(new Set(workerIds)).toEqual(new Set([1, 2, 3]))
  })

  it('should not create more workers than jobs', async () => {
    const workerIds: number[] = []

    const jobs: Job<void>[] = [
      {
        execute: async (context) => {
          workerIds.push(context.workerId)
        },
        properties: {}
      }
    ]

    await runJobs(jobs, 5, mockLogger)

    expect(workerIds).toHaveLength(1)
    expect(workerIds[0]).toBe(1)
  })
})
