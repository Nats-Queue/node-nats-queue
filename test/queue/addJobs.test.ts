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
    await queue!.addJobs(
      [
        {
          name: 'test',
          queueName: queueName,
          data: {},
        },
        {
          name: 'test-2',
          queueName: queueName,
          data: {},
        },
        {
          name: 'test-3',
          queueName: queueName,
          data: {},
        },
      ],
      2,
    )

    const stream = await jsm.streams.get(queueName)
    const streamInfo = await stream.info()
    assert.strictEqual(streamInfo.state.messages, 3)
  })
})
