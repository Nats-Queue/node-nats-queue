import {
  JetStreamClient,
  JsMsg,
  Consumer,
  AckPolicy,
  JetStreamManager,
  ConsumerMessages,
} from '@nats-io/jetstream'
import { KV, Kvm } from '@nats-io/kv'
import { errors } from '@nats-io/nats-core'
import EventEmitter from 'events'
import { WorkerOpts } from '.'
import { Limiter, FixedWindowLimiter, IntervalLimiter } from './limiter'
import { createSubject, sleep } from './utils'

type JobData = {
  id: string
  name: string
  parentId: string
  meta: {
    failed: boolean
    startTime: number
    retryCount: number
    timeout: number
  }
  data: unknown
  // Why does job need to know about the queue name?
  queueName: string
}
export class Worker extends EventEmitter {
  protected readonly client: JetStreamClient
  protected readonly name: string
  protected readonly processor: (job: JsMsg) => Promise<void>
  protected readonly concurrency: number
  protected readonly limiter: Limiter
  protected readonly fetchInterval: number
  protected readonly fetchTimeout: number
  protected readonly maxRetries: number
  protected readonly priorities: number

  protected manager: JetStreamManager | null = null
  protected consumers: Consumer[] = []
  protected consumer: Consumer | null = null
  protected running = false
  protected processingNow = 0
  protected loopPromise: Promise<void> | null = null
  //   TODO: What is this?
  protected kv: KV | null = null
  protected priorityQuota?: Map<
    number,
    {
      quota: number
      counter: number
    }
  > = new Map()

  constructor(opts: WorkerOpts) {
    super()

    this.client = opts.client
    this.name = opts.name
    this.processor = opts.processor
    this.concurrency = opts.concurrency || 1
    this.maxRetries = opts.maxRetries || 3
    this.priorities = opts.priorities || 1

    this.fetchInterval = 150
    this.fetchTimeout = 3_000
    this.limiter = opts.rateLimit
      ? new FixedWindowLimiter(
          opts.rateLimit.max,
          opts.rateLimit.duration,
          this.fetchInterval,
        )
      : new IntervalLimiter(this.fetchInterval)

    this.priorityQuota = opts.priorityQuota
      ? new Map(
          Array.from(opts.priorityQuota.entries()).map(([priority, item]) => {
            return [
              priority,
              {
                quota: item.quota,
                counter: 0,
              },
            ]
          }),
        )
      : undefined
  }

  public async setup() {
    try {
      this.manager = await this.client.jetstreamManager()
      const kvm = await new Kvm(this.client)
      this.kv = await kvm.create(`${this.name}_parent_id`)
      this.consumers = await this.setupConsumers()
    } catch (e) {
      throw new Error()
    }
  }

  private async setupConsumers(): Promise<Consumer[]> {
    const consumers: Consumer[] = []
    for (let i = 1; i <= this.priorities; i++) {
      // TODO: Naming might be wrong, independent of the queue name
      const consumerName = `worker_group_${i}`
      const subject = `${this.name}.*.${i}`
      await this.manager?.consumers.add(this.name, {
        filter_subject: subject,
        name: consumerName,
        durable_name: consumerName,
        ack_policy: AckPolicy.All,
      })
      const consumer = await this.client.consumers.get(consumerName)
      console.log(
        `Consumer: name=${this.name} successfully subscribed to topic ${subject}.`,
      )
      consumers.push(consumer)
    }
    return consumers
  }

  public async stop() {
    this.running = false

    if (this.loopPromise) {
      await this.loopPromise
    }
    while (this.processingNow > 0) {
      await sleep(this.fetchInterval)
    }
  }

  start() {
    if (this.consumers.length === 0) {
      throw new Error('call setup() before start()')
    }

    if (!this.loopPromise) {
      this.running = true
      this.loopPromise = this.loop()
    }
  }

  private resetQuotesCounter() {
    for (const [priority, item] of this.priorityQuota!.entries()) {
      item.counter = 0
    }
  }

  private handleQuota(consumerPriority: number) {
    const priorityItem = this.priorityQuota!.get(consumerPriority)
    const currentQuota = priorityItem!.quota
    const currentCounter = priorityItem!.counter

    if (currentQuota - currentCounter <= 0) {
      if (consumerPriority < this.priorities) {
        console.debug(
          `Skip consumer_priority=${consumerPriority} due to quota overruns`,
        )
        return true
      } else {
        console.debug(
          `Reset counters when quota is exceeded for consumer_priority=${consumerPriority}`,
        )
        this.resetQuotesCounter()
        return false
      }
    }
    return null
  }

  protected async loop() {
    while (this.running) {
      let jobs: ConsumerMessages | never[] = []
      let consumerPriority = 0
      for (let i = 0; i < this.consumers.length; i++) {
        consumerPriority = i + 1

        if (this.priorityQuota) {
          const isQuotaMet = this.handleQuota(consumerPriority)
          if (isQuotaMet !== null) {
            if (isQuotaMet) continue
            else break
          }
        }

        const maxJobs = this.limiter.get(this.concurrency - this.processingNow)
        if (maxJobs <= 0) break

        jobs = await this.fetch(this.consumers[i], maxJobs)
      }

      for await (const j of jobs) {
        if (this.priorityQuota)
          this.priorityQuota.get(consumerPriority)!.counter += 1

        this.limiter.inc()
        this.process(j)
      }

      await sleep(this.limiter.timeout())
    }
  }

  protected async processTask(j: JsMsg) {
    try {
      this.processingNow += 1
      const data: JobData = JSON.parse(new TextDecoder().decode(j.data))
      if (data.meta.failed) {
        await j.term()
      }

      const jobStartTime = data.meta.startTime
      const now = Date.now()
      if (jobStartTime > now) {
        const delay = jobStartTime - now
        await j.nak(delay)
        console.debug(
          `Job: name=${data.name} id=${data.id} is scheduled later. Requeueing in ${delay} seconds`,
        )
        return
      }

      if (data.meta.retryCount >= this.maxRetries) {
        await j.term()
        console.error(
          `Job: name=${data.name} id=${data.id} failed max retries exceeded`,
        )

        // TODO: Mark parents failed
        // await tihs.markParentsFailed(data)
        return
      }

      console.log(
        `Job: name=${data.name} id=${data.id} is started with data=${data.data} in queue=${data.queueName}`,
      )

      // TODO: Process timeout
      const timeout = data.meta.timeout
    } catch (e) {}
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

  protected async fetch(
    consumer: Consumer,
    count: number,
  ): Promise<ConsumerMessages | never[]> {
    try {
      const msgs = await consumer.fetch({
        max_messages: count,
        expires: this.fetchTimeout,
      })
      // TODO: Fetch consumer info
      const consumerInfo = await consumer.info()
      console.debug(
        `Consumer: name=${
          consumerInfo.name
          // TODO: How to count fetched messages?
        } fetched ${'msgs.length'} messages from queue=${this.name}`,
      )

      return msgs
    } catch (e) {
      // TODO
      return []
    }
  }
}
