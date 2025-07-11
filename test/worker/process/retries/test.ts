import {
  after,
  afterEach,
  before,
  beforeEach,
  describe,
  it,
  mock,
} from 'node:test'
import { Queue } from '../../../../src/queue'
import { Worker } from '../../../../src/worker'
import { connect, NatsConnection } from '@nats-io/transport-node'
import {
  jetstream,
  JetStreamClient,
  JetStreamManager,
  JsMsg,
} from '@nats-io/jetstream'
import { Job } from '../../../../src/job'
import assert from 'assert'
import { sleep } from '../../../../src/utils'

describe('Worker.process(): retries', () => {
  let nc: NatsConnection
  let js: JetStreamClient
  let jsm: JetStreamManager
  const queueName = 'queue'
  const queueMaxPriority = 3
  const maxRetries = 2
  let queue: Queue | undefined = undefined
  let worker: Worker | undefined = undefined
  let processorMock: ReturnType<
    typeof mock.fn<(job: JsMsg, timeout: number) => Promise<void>>
  >

  before(async () => {
    nc = await connect({ servers: '127.0.0.1:4222' })
    js = jetstream(nc)
    jsm = await js.jetstreamManager()
    await jsm.streams.delete(queueName).catch(() => {})
  })

  beforeEach(async () => {
    // Create a mock function for the processor
    processorMock = mock.fn<(job: JsMsg, timeout: number) => Promise<void>>(
      async () => {},
    )

    queue = new Queue({
      name: queueName,
      client: js,
      connection: nc,
      priorities: queueMaxPriority,
      duplicateWindow: 2000,
    })
    await queue.setup()

    worker = new Worker({
      name: queueName,
      client: js,
      processor: processorMock,
      maxRetries,
    })

    await worker.setup()
  })

  afterEach(async () => {
    await worker!.stop()
    await jsm.streams.delete(queueName).catch(() => {})
  })

  after(async () => {
    await nc.close()
  })

  it('should retry job', async () => {
    await worker!.start()
    const job = new Job({
      id: 'job1',
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    processorMock.mock.mockImplementationOnce(async () => {
      throw new Error('Processing failed')
    })

    await queue!.addJob(job)

    await sleep(500) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, 2)

    const processedJob: Job = processorMock.mock.calls[0].arguments[0].json()
    assert(processedJob.id === job.id)
    assert(processedJob.name === job.name)
    assert(processedJob.queueName === job.queueName)
    assert.deepStrictEqual(processedJob.data, job.data)
  })

  it('should retry job not more than maxAttempts times', async () => {
    await worker!.start()
    const job = new Job({
      id: 'job1',
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    processorMock.mock.mockImplementation(async () => {
      throw new Error('Processing failed')
    })

    await queue!.addJob(job)

    await sleep(500) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, maxRetries)

    const processedJob: Job = processorMock.mock.calls[0].arguments[0].json()
    assert(processedJob.id === job.id)
    assert(processedJob.name === job.name)
    assert(processedJob.queueName === job.queueName)
    assert.deepStrictEqual(processedJob.data, job.data)
  })
})
