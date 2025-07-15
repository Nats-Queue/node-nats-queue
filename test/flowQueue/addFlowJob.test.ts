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
import { FlowQueue } from '../../src/flowQueue'
import { FlowJob } from '../../src/flowJob'
import { Job } from '../../src/job'
import { KV, Kvm } from '@nats-io/kv'
import { ChildToParentsKVValue, DependenciesKVValue } from '../../src/types'

describe('FlowQueue.addFlowJob()', () => {
  let nc: NatsConnection
  let js: JetStreamClient
  let jsm: JetStreamManager
  const queueName = 'queue'
  const queueMaxPriority = 3
  let queue: FlowQueue | undefined = undefined
  let parentToChildrenKV: KV
  let childToParentsKV: KV

  before(async () => {
    nc = await connect({ servers: '127.0.0.1:4222' })
    js = jetstream(nc)
    jsm = await js.jetstreamManager()
    await jsm.streams.delete(queueName).catch(() => {})
  })

  beforeEach(async () => {
    queue = new FlowQueue({
      name: queueName,
      client: js,
      connection: nc,
      priorities: queueMaxPriority,
      duplicateWindow: 2000,
    })
    await queue.setup()
    const kvm = new Kvm(js)
    parentToChildrenKV = await kvm.open(`${queueName}_parent_id`)
    childToParentsKV = await kvm.open(`${queueName}_parents`)
  })

  afterEach(async () => {
    await jsm.streams.delete(queueName).catch(() => {})
    const kvm = new Kvm(js)
    parentToChildrenKV = await kvm.open(`${queueName}_parent_id`)
    if (parentToChildrenKV) await parentToChildrenKV.destroy()
    childToParentsKV = await kvm.open(`${queueName}_parents`)
    if (childToParentsKV) await childToParentsKV.destroy()
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

    const keyValue = await parentToChildrenKV.get('id-test')
    if (!keyValue) throw new Error('Parent not found')

    const data: DependenciesKVValue = keyValue.json()
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
    if (!message) throw new Error('Message not found')

    const parsedData: Job = message.json()
    assert.strictEqual(parsedData.id, 'id-child1')
    assert.strictEqual(parsedData.meta.parentId, 'id-test')
  })

  it('should set parents for child in KV', async () => {
    mock.timers.enable({
      apis: ['Date'],
      now: new Date('2023-05-14T11:01:58.135Z'),
    })
    const childJob1 = new FlowJob({
      job: new Job({
        id: 'test-child',
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

    const childParentsValue = await childToParentsKV.get('test-child')
    if (!childParentsValue?.value)
      throw new Error('Child parents value not found')

    const childParentsData: ChildToParentsKVValue = childParentsValue.json()
    assert.deepStrictEqual(childParentsData, {
      parentIds: ['id-test'],
    })

    mock.timers.reset()
  })

  it('should add new parent to child if child has multiple parents info to KV', async () => {
    mock.timers.enable({
      apis: ['Date'],
      now: new Date('2023-05-14T11:01:58.135Z'),
    })
    const childJob1 = new FlowJob({
      job: new Job({
        id: 'child1',
        name: 'child1',
        queueName,
        data: {},
      }),
    })
    const parent1 = new FlowJob({
      job: new Job({
        id: 'parent1',
        name: 'parent1',
        queueName: queueName,
        data: {},
      }),
      children: [childJob1],
    })
    const parent2 = new FlowJob({
      job: new Job({
        id: 'parent2',
        name: 'parent2',
        queueName: queueName,
        data: {},
      }),
      children: [childJob1],
    })
    await queue!.addFlowJob(parent1)
    await queue!.addFlowJob(parent2)

    const childParentsValue = await childToParentsKV.get('child1')
    if (!childParentsValue?.value)
      throw new Error('Child parents value not found')

    const childParentsData: ChildToParentsKVValue = childParentsValue.json()
    assert.deepStrictEqual(childParentsData, {
      parentIds: ['parent1', 'parent2'],
    })

    mock.timers.reset()
  })

  it('should add new child to parent', async () => {
    mock.timers.enable({
      apis: ['Date'],
      now: new Date('2023-05-14T11:01:58.135Z'),
    })
    const childJob1 = new FlowJob({
      job: new Job({
        id: 'child1',
        name: 'child1',
        queueName,
        data: {},
      }),
    })
    const parent1 = new FlowJob({
      job: new Job({
        id: 'parent1',
        name: 'parent1',
        queueName: queueName,
        data: {},
      }),
      children: [childJob1],
    })
    const childJob2 = new FlowJob({
      job: new Job({
        id: 'child2',
        name: 'child2',
        queueName: queueName,
        data: {},
      }),
      children: [],
    })
    const parent2 = new FlowJob({
      job: new Job({
        id: 'parent1',
        name: 'parent1',
        queueName: queueName,
        data: {},
      }),
      children: [childJob2],
    })
    await queue!.addFlowJob(parent1)
    await queue!.addFlowJob(parent2)

    const parentValue = await parentToChildrenKV.get('parent1')
    if (!parentValue) throw new Error('Parent not found')
    const parentData: DependenciesKVValue = parentValue.json()
    assert.deepStrictEqual(parentData, {
      id: 'parent1',
      name: 'parent1',
      meta: {
        retryCount: 0,
        startTime: new Date('2023-05-14T11:01:58.135Z').getTime(),
        failed: false,
        timeout: 0,
      },
      data: {},
      queueName: 'queue',
      childrenCount: 2,
    })

    mock.timers.reset()
  })
})
