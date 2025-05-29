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
import { Job } from '../../src/types'

describe('Queue.addJob()', () => {
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

  it('should add job', async () => {
    await queue!.addJob({
      name: 'test',
      queueName: queueName,
      data: {},
    })

    const stream = await jsm.streams.get(queueName)
    const streamInfo = await stream.info()
    assert.strictEqual(streamInfo.state.messages, 1)
  })

  it('should add job with priority', async () => {
    await queue!.addJob(
      {
        name: 'test',
        queueName: queueName,
        data: {},
      },
      2,
    )

    const message = await jsm.streams.getMessage(queueName, {
      seq: 1,
    })
    assert.strictEqual(message?.subject, `${queueName}.test.2`)
  })

  it('should limit job priority to queues max priority', async () => {
    await queue!.addJob(
      {
        name: 'test',
        queueName: queueName,
        data: {},
      },
      queueMaxPriority + 1,
    )

    const message = await jsm.streams.getMessage(queueName, {
      seq: 1,
    })
    assert.strictEqual(
      message?.subject,
      `${queueName}.test.${queueMaxPriority}`,
    )
  })

  it('should add job with data', async () => {
    const data = {
      test1: 25,
      test2: {
        test: 'test42',
      },
    }
    await queue!.addJob(
      {
        name: 'test',
        queueName: queueName,
        data: data,
      },
      queueMaxPriority + 1,
    )

    const message = await jsm.streams.getMessage(queueName, {
      seq: 1,
    })
    const parsedData: Job = JSON.parse(new TextDecoder().decode(message?.data))
    assert.deepStrictEqual(parsedData.data, data)
  })

  it('should deduplicate jobs', async () => {
    await queue!.addJob({
      id: 'test1',
      name: 'test',
      queueName: queueName,
      data: {},
    })
    await queue!.addJob(
      {
        id: 'test1',
        name: 'test',
        queueName: queueName,
        data: {},
      },
      queueMaxPriority,
    )

    const stream = await jsm.streams.get(queueName)
    const streamInfo = await stream.info()
    assert.strictEqual(streamInfo.state.messages, 1)
  })

  it('should add job with correct meta', async () => {
    const data = {
      test1: 25,
      test2: {
        test: 'test42',
      },
    }
    await queue!.addJob(
      {
        name: 'test',
        queueName: queueName,
        data: data,
        timeout: 1000,
      },
      queueMaxPriority + 1,
    )

    const message = await jsm.streams.getMessage(queueName, {
      seq: 1,
    })
    const parsedData: Job = JSON.parse(new TextDecoder().decode(message?.data))
    assert.strictEqual(parsedData.meta.retryCount, 0)
    assert.strictEqual(parsedData.meta.failed, false)
    assert.strictEqual(parsedData.meta.timeout, 1000)
  })
})
