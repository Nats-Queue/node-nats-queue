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
import { FlowJob } from '../../../../src/flowJob'
import { FlowQueue } from '../../../../src/flowQueue'

describe('Worker.process(): flowJob', () => {
  let nc: NatsConnection
  let js: JetStreamClient
  let jsm: JetStreamManager
  const queueName = 'queue'
  const queueMaxPriority = 3
  const maxRetries = 2
  let queue: FlowQueue | undefined = undefined
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

    queue = new FlowQueue({
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

  it('should notify process parent after all children were processed', async () => {
    await worker!.start()
    const child1 = new Job({
      id: 'child1',
      name: 'child1',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const child2 = new Job({
      id: 'child2',
      name: 'child2',
      queueName: queueName,
      data: {
        message: 42,
      },
    })
    const parentJob = new Job({
      id: 'parentJob',
      name: 'parentJob',
      queueName: queueName,
      data: {
        message: 42,
      },
    })

    const flowJobChild1 = new FlowJob({
      job: child1,
      children: [],
    })
    const flowJobChild2 = new FlowJob({
      job: child2,
      children: [],
    })
    const flowJobParent = new FlowJob({
      job: parentJob,
      children: [flowJobChild1, flowJobChild2],
    })

    await queue!.addFlowJob(flowJobParent)

    await sleep(2000) // Wait for job to be processed

    assert.strictEqual(processorMock.mock.calls.length, 3)

    const processedParent: Job = processorMock.mock.calls[2].arguments[0].json()
    assert(processedParent.id === flowJobParent.job.id)
  })
})
