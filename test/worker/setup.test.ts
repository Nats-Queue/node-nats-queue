import { describe, it, before, after, beforeEach, afterEach } from 'node:test'
import {
  jetstream,
  JetStreamApiError,
  JetStreamClient,
  JetStreamManager,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { NatsConnection } from '@nats-io/nats-core'
import { Worker } from '../../src/worker'
import assert from 'node:assert'
import { Kvm } from '@nats-io/kv'

describe('Worker.setup()', () => {
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

  it('should initialize successfully', async () => {
    const worker = new Worker({
      client: js,
      name: queueName,
      processor: async () => {},
      concurrency: 1,
      maxRetries: 1,
      priorities: 1,
    })

    await worker.setup()
  })

  it('should create 1 consumer for each priority', async () => {
    const worker = new Worker({
      client: js,
      name: queueName,
      processor: async () => {},
      concurrency: 1,
      maxRetries: 1,
      priorities: 3,
    })

    await worker.setup()

    const consumerName1 = `worker_group_1`
    const consumerName2 = `worker_group_2`
    const consumerName3 = `worker_group_3`

    try {
      await js.consumers.get(queueName, consumerName1)
      await js.consumers.get(queueName, consumerName2)
      await js.consumers.get(queueName, consumerName3)
    } catch {
      assert.fail('Consumer not found')
    }
  })

  it('should create parent_id bucket for stream', async () => {
    const worker = new Worker({
      client: js,
      name: queueName,
      processor: async () => {},
      concurrency: 1,
      maxRetries: 1,
      priorities: 1,
    })

    await worker.setup()

    const bucketName = `${queueName}_parent_id`

    try {
      await kvm.open(bucketName)
    } catch {
      assert.fail('Bucket not found')
    }
  })

  it('should fail if stream not found', async () => {
    const worker = new Worker({
      client: js,
      name: 'non_existent_stream',
      processor: async () => {},
      concurrency: 1,
      maxRetries: 1,
      priorities: 1,
    })

    try {
      await worker.setup()
    } catch (e) {
      if (e instanceof JetStreamApiError) {
        assert(e.name === 'StreamNotFoundError', 'StreamNotFoundError expected')
      } else {
        assert.fail('Unexpected error')
      }
    }
  })
})
