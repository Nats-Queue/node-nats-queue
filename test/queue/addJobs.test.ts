import { describe, it, before, after, beforeEach, afterEach } from 'node:test'
import {
  jetstream,
  JetStreamClient,
  JetStreamManager,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { NatsConnection } from '@nats-io/nats-core'
import assert from 'node:assert'
import { Queue } from '../../src/queue'
import { Job } from '../../src/job'

describe('Queue.addJobs()', () => {
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

  it('should add multiple jobs', async () => {
    const job1 = new Job({
      name: 'test',
      queueName: queueName,
      data: {},
    })
    const job2 = new Job({
      name: 'test',
      queueName: queueName,
      data: {},
    })
    const job3 = new Job({
      name: 'test',
      queueName: queueName,
      data: {},
    })
    await queue!.addJobs([job1, job2, job3], 2)

    const stream = await jsm.streams.get(queueName)
    const streamInfo = await stream.info()
    assert.strictEqual(streamInfo.state.messages, 3)
  })
})
