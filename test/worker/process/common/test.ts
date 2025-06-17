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
import { deleteAllKV } from '../../../helpers/deleteAllKV'
import { deleteAllStreams } from '../../../helpers/deleteAllStreams'

describe('Worker.process(): common', () => {
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
  })

  beforeEach(async () => {
    await deleteAllKV(nc)
    await deleteAllStreams(jsm)

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
    })

    await worker.setup()
  })

  afterEach(async () => {
    await worker!.stop()
  })

  after(async () => {
    await nc.close()
  })

  it('should process job', async () => {
    await worker!.start()
    const job = new Job({
      id: 'job1',
      name: 'job1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    await queue!.addJob(job)

    await sleep(100) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, 1)

    const processedJob: Job = processorMock.mock.calls[0].arguments[0].json()
    assert(processedJob.id === job.id)
    assert(processedJob.name === job.name)
    assert(processedJob.queueName === job.queueName)
    assert.deepStrictEqual(processedJob.data, job.data)
  })
})
