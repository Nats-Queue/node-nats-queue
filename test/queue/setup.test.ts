import { describe, it, before, after, afterEach } from 'node:test'
import {
  jetstream,
  JetStreamClient,
  JetStreamManager,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { NatsConnection } from '@nats-io/nats-core'
import assert from 'node:assert'
import { Queue } from '../../src/queue'

describe('Queue.setup()', () => {
  let nc: NatsConnection
  let js: JetStreamClient
  let jsm: JetStreamManager
  const queueName = 'queue'

  before(async () => {
    nc = await connect({ servers: '127.0.0.1:4222' })
    js = jetstream(nc)
    jsm = await js.jetstreamManager()
    await jsm.streams.delete(queueName).catch(() => {})
  })

  afterEach(async () => {
    await jsm.streams.delete(queueName)
  })

  after(async () => {
    await nc.close()
  })

  it('should create queue', async () => {
    const queue = new Queue({
      name: queueName,
      client: js,
      connection: nc,
      priorities: 1,
      duplicateWindow: 2000,
    })

    await queue.setup()

    const streamInfo = await jsm.streams.get(queueName)
    assert.strictEqual(streamInfo.name, queueName)
  })

  it('should update queue', async () => {
    await jsm.streams.add({
      name: queueName,
    })

    const queue = new Queue({
      name: queueName,
      client: js,
      connection: nc,
      priorities: 1,
      duplicateWindow: 2000,
    })

    await queue.setup()

    const streamInfo = await jsm.streams.get(queueName)
    assert.strictEqual(streamInfo.name, queueName)
  })
})
