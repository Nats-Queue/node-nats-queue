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
      async (job, timeout) => {
        console.log('old processor called')
      },
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
      priorities: 3,
      // priorities: 1,
      // priorityQuota: new Map([
      //   [1, { quota: 2 }],
      //   [2, { quota: 1 }],
      //   [3, { quota: 1 }],
      // ]),
      rateLimit: {
        duration: 100,
        max: 22,
      },
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
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job3 = new Job({
      id: 'job3',
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    // await queue!.addJobs([job3, job2], 3)
    // await queue!.addJobs([job2], 2)
    await queue!.addJobs([job], 1)

    await worker!.start()

    await sleep(5000) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, 2)

    // const processedJob: Job = processorMock.mock.calls[0].arguments[0].json()
    // assert(processedJob.id === job.id)
    // assert(processedJob.name === job.name)
    // assert(processedJob.queueName === job.queueName)
    // assert.deepStrictEqual(processedJob.data, job.data)
  })
})
