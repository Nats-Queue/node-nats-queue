import { describe, it, before, after, beforeEach, afterEach } from 'node:test'
import {
  jetstream,
  JetStreamClient,
  JetStreamManager,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { NatsConnection } from '@nats-io/nats-core'
import { Kvm } from '@nats-io/kv'
import assert from 'node:assert'
import { Queue } from '../../src/queue'

describe('Queue.constructor()', () => {
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

  after(async () => {
    await nc.close()
  })

  it('should fail if duplicateWindow < 100ms', async () => {
    try {
      new Queue({
        name: queueName,
        client: js,
        connection: nc,
        priorities: 1,
        duplicateWindow: 99,
      })
      assert.fail('Expected error')
    } catch (e) {
      assert.strictEqual(
        (e as Error).message,
        "Parameter 'duplicateWindow' must be more than or equal to 100",
      )
    }
  })

  it('should fail if priorites <= 0', async () => {
    try {
      new Queue({
        name: queueName,
        client: js,
        connection: nc,
        priorities: 0,
        duplicateWindow: 100,
      })
      assert.fail('Expected error')
    } catch (e) {
      assert.strictEqual(
        (e as Error).message,
        "Parameter 'priorities' must be greater than 0",
      )
    }
  })

  it('should fail if name is empty', async () => {
    try {
      new Queue({
        name: '',
        client: js,
        connection: nc,
        priorities: 0,
        duplicateWindow: 100,
      })
      assert.fail('Expected error')
    } catch (e) {
      assert.strictEqual(
        (e as Error).message,
        "Parameter 'name' cannot be empty",
      )
    }
  })
})
