import { describe, it, before, after, beforeEach, afterEach } from 'node:test'
import {
  jetstream,
  JetStreamClient,
  JetStreamManager,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { NatsConnection } from '@nats-io/nats-core'
import { Worker } from '../../src/worker'
import { Kvm } from '@nats-io/kv'
import assert from 'node:assert'

describe('Worker.start(), Worker.stop()', () => {
  let nc: NatsConnection
  let js: JetStreamClient
  let jsm: JetStreamManager
  let kvm: Kvm
  const queueName = 'queue'

  before(async () => {
    nc = await connect({ servers: '127.0.0.1:4222' })
    js = jetstream(nc)
    jsm = await js.jetstreamManager()
    kvm = await new Kvm(nc)
  })

  beforeEach(async () => {
    await jsm.streams.add({
      name: queueName,
      subjects: [`${queueName}.*.*`],
    })
  })

  afterEach(async () => {
    await jsm.streams.delete(queueName)
  })

  after(async () => {
    await nc.close()
  })

  it('should start and stop successfully', async () => {
    const worker = new Worker({
      client: js,
      name: queueName,
      processor: async () => {},
      concurrency: 1,
      maxRetries: 1,
      priorities: 1,
    })

    await worker.setup()

    worker.start()

    await worker.stop()
  })

  it('should fail if called before setup()', async () => {
    const worker = new Worker({
      client: js,
      name: queueName,
      processor: async () => {},
      concurrency: 1,
      maxRetries: 1,
      priorities: 1,
    })

    try {
      worker.start()
      assert.fail('Expected error')
    } catch (e) {
      assert.strictEqual((e as Error).message, 'call setup() before start()')
    }
  })
})
