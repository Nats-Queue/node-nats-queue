import {
  describe,
  it,
  before,
  after,
  beforeEach,
  afterEach,
  mock,
} from 'node:test'
import {
  jetstream,
  JetStreamClient,
  JetStreamManager,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { NatsConnection } from '@nats-io/nats-core'
import assert from 'node:assert'
import { Queue } from '../../src/queue'
import { FlowJob } from '../../src/flowJob'
import { Job } from '../../src/job'
import { Kvm } from '@nats-io/kv'

describe('Queue.addFlowJob()', () => {
  let nc: NatsConnection
  let js: JetStreamClient
  let jsm: JetStreamManager
  const queueName = 'queue'
  const queueMaxPriority = 3
  let queue: Queue | undefined = undefined

  before(async () => {
    nc = await connect({ servers: '127.0.0.1:4222' })
    js = jetstream(nc)
    jsm = await js.jetstreamManager()
    await jsm.streams.delete(queueName).catch(() => {})
  })

  beforeEach(async () => {
    queue = new Queue({
      name: queueName,
      client: js,
      connection: nc,
      priorities: queueMaxPriority,
      duplicateWindow: 2000,
    })
    await queue.setup()
  })

  afterEach(async () => {
    await jsm.streams.delete(queueName).catch(() => {})
  })

  after(async () => {
    await nc.close()
  })

  it('should add flowJob without children', async () => {
    const job = new FlowJob({
      job: new Job({
        name: 'test',
        queueName: queueName,
        data: {},
      }),
    })
    await queue!.addFlowJob(job)

    const stream = await jsm.streams.get(queueName)
    const streamInfo = await stream.info()
    assert.strictEqual(streamInfo.state.messages, 1)
  })

  it('should add parent info to KV', async () => {
    mock.timers.enable({
      apis: ['Date'],
      now: new Date('2023-05-14T11:01:58.135Z'),
    })
    const childJob1 = new FlowJob({
      job: new Job({
        name: 'test-child',
        queueName,
        data: {},
      }),
    })
    const job = new FlowJob({
      job: new Job({
        id: 'id-test',
        name: 'test',
        queueName: queueName,
        data: {},
      }),
      children: [childJob1],
    })
    await queue!.addFlowJob(job)

    const kv = new Kvm(js)

    const bucketName = `${queueName}_parent_id`
    const bucket = await kv.open(bucketName)
    const keyValue = await bucket.get('id-test')
    const data = keyValue?.json()
    assert.deepStrictEqual(data, {
      id: 'id-test',
      name: 'test',
      meta: {
        retryCount: 0,
        startTime: new Date('2023-05-14T11:01:58.135Z').getTime(),
        failed: false,
        timeout: 0,
      },
      data: {},
      queueName: 'queue',
      childrenCount: 1,
    })

    mock.timers.reset()
  })

  it('should set parentId for child', async () => {
    const childJob1 = new FlowJob({
      job: new Job({
        id: 'id-child1',
        name: 'test-child',
        queueName,
        data: {},
      }),
    })
    const job = new FlowJob({
      job: new Job({
        id: 'id-test',
        name: 'test',
        queueName: queueName,
        data: {},
      }),
      children: [childJob1],
    })
    await queue!.addFlowJob(job)

    const message = await jsm.streams.getMessage(queueName, {
      seq: 1,
    })
    const parsedData: Job = JSON.parse(new TextDecoder().decode(message?.data))
    assert.strictEqual(parsedData.id, 'id-child1')
    assert.strictEqual(parsedData.meta.parentId, 'id-test')
  })
})
