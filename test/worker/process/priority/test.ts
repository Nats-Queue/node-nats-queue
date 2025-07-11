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

describe('Worker.process(): priority', () => {
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
      priorities: queueMaxPriority,
      priorityQuota: new Map([
        [1, { quota: 2 }],
        [2, { quota: 1 }],
        [3, { quota: 1 }],
      ]),
      rateLimit: {
        duration: 1000,
        max: 1,
      },
      fetchInterval: 50,
      fetchTimeout: 1000,
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

  it('should process jobs from priority quota first', async () => {
    const job = new Job({
      id: 'job1',
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job2 = new Job({
      id: 'job2',
      name: 'job2',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job3 = new Job({
      id: 'job3',
      name: 'job3',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    await queue!.addJobs([job2, job3], 1)
    await queue!.addJobs([job], 3)

    await worker!.start()
    await sleep(200) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, 2)

    const job2Data: Job = processorMock.mock.calls[0].arguments[0].json()
    assert.strictEqual(job2Data.name, job2.name)

    const job3Data: Job = processorMock.mock.calls[1].arguments[0].json()
    assert.strictEqual(job3Data.name, job3.name)
  })

  it('should process no more jobs than available in quota', async () => {
    const job = new Job({
      id: 'job1',
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job2 = new Job({
      id: 'job2',
      name: 'job2',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job3 = new Job({
      id: 'job3',
      name: 'job3',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    await queue!.addJobs([job2, job3, job], 1)

    await worker!.start()
    await sleep(200) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, 2)

    const job2Data: Job = processorMock.mock.calls[0].arguments[0].json()
    assert.strictEqual(job2Data.name, job2.name)

    const job3Data: Job = processorMock.mock.calls[1].arguments[0].json()
    assert.strictEqual(job3Data.name, job3.name)
  })

  it('should process from next quota if previous does not have enough jobs', async () => {
    const job = new Job({
      id: 'job1',
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job2 = new Job({
      id: 'job2',
      name: 'job2',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    await queue!.addJobs([job], 1)
    await queue!.addJobs([job2], 2)

    await worker!.start()
    await sleep(2000) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, 2)

    const jobData: Job = processorMock.mock.calls[0].arguments[0].json()
    assert.strictEqual(jobData.name, job.name)

    const job2Data: Job = processorMock.mock.calls[1].arguments[0].json()
    assert.strictEqual(job2Data.name, job2.name)
  })
})
