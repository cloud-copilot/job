import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Job, JobContext, JobResult, Logger } from './job.js'
import { StreamingJobQueue } from './StreamingJobQueue.js'

describe('StreamingJobQueue', () => {
  let mockLogger: Logger

  // let onComplete: (result: JobResult<any, any>) => Promise<void>
  // let queue: StreamingJobQueue<string>

  beforeEach(() => {
    mockLogger = {
      warn: vi.fn()
    }
    // completedResults = []
    // const onComplete = vi.fn(async (result) => {
    //   completedResults.push(result)
    // })
    vi.clearAllTimers()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

  it('should process jobs and call onComplete for each result', async () => {
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })

    const queue = new StreamingJobQueue<string, Record<string, unknown>>(2, mockLogger, onComplete)

    const jobs: Job<string>[] = [
      { execute: async () => 'result1', properties: { id: 1 } },
      { execute: async () => 'result2', properties: { id: 2 } },
      { execute: async () => 'result3', properties: { id: 3 } }
    ]

    queue.enqueueAll(jobs)
    await queue.waitForIdle()

    expect(completedResults).toHaveLength(3)
    expect(completedResults[0]).toEqual({
      status: 'fulfilled',
      value: 'result1',
      properties: { id: 1 }
    })
    expect(completedResults[1]).toEqual({
      status: 'fulfilled',
      value: 'result2',
      properties: { id: 2 }
    })
    expect(completedResults[2]).toEqual({
      status: 'fulfilled',
      value: 'result3',
      properties: { id: 3 }
    })
    expect(onComplete).toHaveBeenCalledTimes(3)
  })

  it('should respect concurrency limits', async () => {
    vi.useRealTimers()
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue(2, mockLogger, onComplete)

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

    queue.enqueueAll(jobs)
    await queue.waitForIdle()

    expect(maxConcurrentJobs).toBe(2)
    expect(completedResults).toHaveLength(5)
  })

  it('should handle job failures and continue processing', async () => {
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })

    const queue = new StreamingJobQueue<string, Record<string, unknown>>(2, mockLogger, onComplete)

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

    expect(completedResults).toHaveLength(3)
    expect(completedResults[0]).toEqual({
      status: 'fulfilled',
      value: 'success1',
      properties: { id: 1 }
    })
    expect(completedResults[1]).toEqual({
      status: 'rejected',
      reason: expect.any(Error),
      properties: { id: 2 }
    })
    expect(completedResults[2]).toEqual({
      status: 'fulfilled',
      value: 'success2',
      properties: { id: 3 }
    })
  })

  it('should process jobs added after queue creation', async () => {
    vi.useRealTimers()
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue<string, Record<string, unknown>>(1, mockLogger, onComplete)

    // Add first job
    queue.enqueue({ execute: async () => 'first', properties: { id: 1 } })

    // Wait a bit then add more jobs
    await new Promise<void>((resolve) =>
      setTimeout(() => {
        queue.enqueue({ execute: async () => 'second', properties: { id: 2 } })
        queue.enqueue({ execute: async () => 'third', properties: { id: 3 } })
        resolve()
      }, 10)
    )

    await queue.waitForIdle()

    expect(completedResults).toHaveLength(3)
    expect(completedResults.map((r) => (r.status === 'fulfilled' ? r.value : null))).toEqual([
      'first',
      'second',
      'third'
    ])
  })

  it('should stream results as they complete', async () => {
    vi.useRealTimers()

    const completedResults: JobResult<any, any>[] = []
    const completionOrder: string[] = []
    const onCompleteWithOrder = vi.fn(async (result: JobResult<string, any>) => {
      completionOrder.push(result.status === 'fulfilled' ? result.value : 'error')
      completedResults.push(result)
    })

    const queue = new StreamingJobQueue(2, mockLogger, onCompleteWithOrder)

    const jobs: Job<string>[] = [
      {
        execute: async () => {
          await new Promise((resolve) => setTimeout(resolve, 100))
          return 'slow'
        },
        properties: { id: 1 }
      },
      {
        execute: async () => {
          await new Promise((resolve) => setTimeout(resolve, 50))
          return 'fast'
        },
        properties: { id: 2 }
      }
    ]

    queue.enqueueAll(jobs)
    await queue.waitForIdle()

    // Fast job should complete first
    expect(completionOrder).toEqual(['fast', 'slow'])
    expect(onCompleteWithOrder).toHaveBeenCalledTimes(2)
  })

  it('should pass correct context to jobs', async () => {
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue(2, mockLogger, onComplete)

    const contextCaptures: JobContext[] = []
    const jobs: Job<void>[] = [
      {
        execute: async (context) => {
          contextCaptures.push(context)
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
  })

  it('should track queue length and active job count', async () => {
    vi.useRealTimers()

    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue<void | string, Record<string, unknown>>(
      1,
      mockLogger,
      onComplete
    )

    expect(queue.queueLength).toBe(0)
    expect(queue.activeJobCount).toBe(0)

    // Add jobs but don't wait
    queue.enqueue({
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50))
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
    vi.useRealTimers()
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue<string, Record<string, unknown>>(2, mockLogger, onComplete)

    const jobs: Job<string>[] = [
      {
        execute: async () => {
          await sleep(100)
          return 'result1'
        },
        properties: { id: 1 }
      },
      {
        execute: async () => {
          await sleep(150)
          return 'result2'
        },
        properties: { id: 2 }
      }
    ]

    queue.enqueueAll(jobs)
    await queue.finishAllWork()

    expect(completedResults).toHaveLength(2)
    expect(completedResults.every((r) => r.status === 'fulfilled')).toBe(true)
  })

  it('should reject new jobs after finishAllWork', async () => {
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue<string, Record<string, unknown>>(1, mockLogger, onComplete)

    await queue.finishAllWork()

    expect(() => {
      queue.enqueue({ execute: async () => 'test', properties: {} })
    }).toThrow('Cannot enqueue jobs after shutdown')
  })

  it('should warn about long-running jobs', async () => {
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue(1, mockLogger, onComplete)

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

    expect(completedResults).toHaveLength(1)
  })

  it('should handle multiple waitForIdle calls', async () => {
    vi.useRealTimers()
    const completedResults: JobResult<any, any>[] = []
    const onComplete = vi.fn(async (result) => {
      completedResults.push(result)
    })
    const queue = new StreamingJobQueue<string, Record<string, unknown>>(1, mockLogger, onComplete)

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

    expect(completedResults).toHaveLength(1)
    expect(completedResults[0].status).toBe('fulfilled')
  })
})
