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

describe('Worker.process(): rateLimit', () => {
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
      async (job, timeout) => {},
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
      rateLimit: {
        duration: 1000,
        max: 2,
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

  it('should process no more than rate limit', async () => {
    await worker!.start()
    const job1 = new Job({
      name: 'job1',
      id: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job2 = new Job({
      name: 'job1',
      id: 'job2',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const job3 = new Job({
      name: 'job1',
      id: 'job3',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    await queue!.addJobs([job1, job2, job3])

    await sleep(200)

    assert.strictEqual(processorMock.mock.calls.length, 2)
  })
})
