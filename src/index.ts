import EventEmitter from 'events'

import { AckPolicy } from '@nats-io/jetstream'
import { nanos, NatsError } from '@nats-io/nats-core'
import type { JetStreamClient, Consumer, JsMsg } from '@nats-io/jetstream'
import type { MsgHdrs } from '@nats-io/nats-core'

import { createSubject, sleep } from './utils'
import { FixedWindowLimiter, IntervalLimiter, type Limiter } from './limiter'

export type QueueOpts = {
  client: JetStreamClient
  name: string

  /**
   * ms
   */
  deduplicateWindow?: number
}

export type Job = {
  id?: number | string
  name: string
  data: unknown
}

export type AddOptions = {
  id?: string
  headers?: MsgHdrs
}

export const DEFAULT_DEDUPLICATE_WINDOW = 2_000

export class Queue {
  protected readonly client: JetStreamClient
  protected readonly name: string

  protected readonly deduplicateWindow: number

  constructor(opts: QueueOpts) {
    this.client = opts.client
    this.name = opts.name

    this.deduplicateWindow =
      opts.deduplicateWindow || DEFAULT_DEDUPLICATE_WINDOW
  }

  async setup() {
    const manager = await this.client.jetstreamManager()

    try {
      await manager.streams.add({
        name: this.name,
        subjects: [`${this.name}.*`],
        duplicate_window: nanos(this.deduplicateWindow),
      })
    } catch (e) {
      // TODO smart error handling
      if (!(e instanceof NatsError)) {
        throw e
      }
      await manager.streams.update(this.name, {
        subjects: [`${this.name}.*`],
        duplicate_window: nanos(this.deduplicateWindow),
      })
    }
  }

  async add(name: string, data?: unknown, options?: AddOptions) {
    const payload = JSON.stringify(data)
    return this.client.publish(
      `${this.name}.${name}`,
      payload,
      options && {
        msgID: options.id,
        headers: options.headers,
      },
    )
  }
}

export type RateLimit = {
  duration: number
  max: number
}

export type WorkerOpts = {
  client: JetStreamClient
  name: string
  processor: (job: JsMsg, timeout: number) => Promise<void>
  concurrency?: number
  rateLimit?: RateLimit
  priorityQuota?: Map<
    number,
    {
      quota: number
    }
  >
  maxRetries?: number
  priorities?: number
}

export class Worker extends EventEmitter {
  protected readonly client: JetStreamClient
  protected readonly name: string
  protected readonly processor: (job: JsMsg) => Promise<void>
  protected readonly concurrency: number
  protected readonly limiter: Limiter
  protected readonly fetchInterval: number
  protected readonly fetchTimeout: number

  protected consumer: Consumer | null = null
  protected running = false
  protected processingNow = 0
  protected loopPromise: Promise<void> | null = null

  constructor(opts: WorkerOpts) {
    super()

    this.client = opts.client
    this.name = opts.name
    this.processor = opts.processor
    this.concurrency = opts.concurrency || 1

    this.fetchInterval = 150
    this.fetchTimeout = 3_000
    this.limiter = opts.rateLimit
      ? new FixedWindowLimiter(
          opts.rateLimit.max,
          opts.rateLimit.duration,
          this.fetchInterval,
        )
      : new IntervalLimiter(this.fetchInterval)
  }

  async setup() {
    const manager = await this.client.jetstreamManager()

    try {
      await manager.streams.add({
        name: this.name,
        subjects: [createSubject(this.name)],
      })
    } catch (e) {
      if (!(e instanceof NatsError && e.api_error?.err_code === 10058)) {
        throw e
      }
    }

    await manager.consumers.add(this.name, {
      durable_name: this.name,
      ack_policy: AckPolicy.All,
    })

    this.consumer = await this.client.consumers.get(this.name, this.name)
  }

  async stop() {
    this.running = false

    if (this.loopPromise) {
      await this.loopPromise
    }
    while (this.processingNow > 0) {
      await sleep(this.fetchInterval)
    }
  }

  start() {
    if (!this.consumer) {
      throw new Error('call setup() before start()')
    }

    if (!this.loopPromise) {
      this.running = true
      this.loopPromise = this.loop()
    }
  }

  protected async loop() {
    while (this.running) {
      const max = this.limiter.get(this.concurrency - this.processingNow)
      const jobs = await this.fetch(max)

      for await (const j of jobs) {
        this.limiter.inc()
        this.process(j) // without await!
      }

      await sleep(this.limiter.timeout())
    }
  }

  protected async process(j: JsMsg) {
    this.processingNow += 1
    try {
      this.process(j)
      await j.ackAck()
    } catch (e) {
      await j.term()
    } finally {
      this.processingNow -= 1
    }
  }

  protected async fetch(count: number) {
    try {
      return this.consumer!.fetch({
        max_messages: count,
        expires: this.fetchTimeout,
      })
    } catch (e) {
      // TODO
      return []
    }
  }
}
