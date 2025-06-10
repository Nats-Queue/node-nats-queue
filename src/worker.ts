import {
  JetStreamClient,
  JsMsg,
  Consumer,
  AckPolicy,
  JetStreamManager,
} from '@nats-io/jetstream'
import { KV, Kvm } from '@nats-io/kv'
import { RateLimit } from './types'
import { Limiter, FixedWindowLimiter, IntervalLimiter } from './limiter'
import { sleep } from './utils'
import { headers, TimeoutError } from '@nats-io/nats-core'
import { Job } from './job'
import { ParentJob } from './types'
import {
  JobChildCompletedEvent,
  JobCompletedEvent,
  JobFailedEvent,
} from './jobEvent'

// TODO: Maybe add Pino logger

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
  fetchInterval?: number
  fetchTimeout?: number
}

export class Worker {
  protected readonly client: JetStreamClient
  protected readonly name: string
  protected readonly processor: (job: JsMsg, timeout: number) => Promise<void>
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
  protected jobEventLoopPromise: Promise<void> | null = null
  protected kv: KV | null = null
  protected priorityQuota?: Map<
    number,
    {
      quota: number
      counter: number
    }
  > = new Map()
  protected jobCompletedConsumer: Consumer | null = null
  protected jobFailedConsumer: Consumer | null = null
  protected parentNotificationConsumer: Consumer | null = null

  constructor(opts: WorkerOpts) {
    this.client = opts.client
    this.name = opts.name
    this.processor = opts.processor
    this.concurrency = opts.concurrency || 1
    this.maxRetries = opts.maxRetries || 3
    this.priorities = opts.priorities || 1

    this.fetchInterval = opts.fetchInterval ?? 150
    this.fetchTimeout = opts.fetchTimeout ?? 3_000
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
      this.kv = await kvm.open(`${this.name}_parent_id`)
      this.consumers = await this.setupConsumers()

      await this.setupInternalQueues(this.manager)
      await this.setupInternalQueuesConsumers(this.manager)
    } catch (e) {
      // TODO: Error handling?
      console.error(
        `Error while setting up worker: ${this.name} with error: ${e}`,
      )
      throw e
    }
  }

  private async setupInternalQueues(manager: JetStreamManager) {
    // TODO: What about deduplication
    // Create streams for completed jobs and failed jobs

    // Completed queue
    await manager.streams.add({
      name: `${this.name}_completed`,
      subjects: [`${this.name}_completed`],
    })

    // Failed queue
    await manager.streams.add({
      name: `${this.name}_failed`,
      subjects: [`${this.name}_failed`],
    })

    // Parent notification
    await manager.streams.add({
      name: `${this.name}_parent_notification`,
      subjects: [`${this.name}_parent_notification`],
    })
  }

  private async setupInternalQueuesConsumers(manager: JetStreamManager) {
    // TODO: What about deduplication
    // Create streams for completed jobs and failed jobs

    await this.manager!.consumers.add(`${this.name}_completed`, {
      filter_subject: `${this.name}_completed`,
      name: `job_completed_consumer`,
      durable_name: `job_completed_consumer`,
      ack_policy: AckPolicy.All,
    })

    await this.manager!.consumers.add(`${this.name}_failed`, {
      filter_subject: `${this.name}_failed`,
      name: `job_failed_consumer`,
      durable_name: `job_failed_consumer`,
      ack_policy: AckPolicy.All,
    })

    await this.manager!.consumers.add(`${this.name}_parent_notification`, {
      filter_subject: `${this.name}_parent_notification`,
      name: `parent_notification_consumer`,
      durable_name: `parent_notification_consumer`,
      ack_policy: AckPolicy.All,
    })

    this.jobCompletedConsumer = await this.client.consumers.get(
      `${this.name}_completed`,
      `job_completed_consumer`,
    )
    this.jobFailedConsumer = await this.client.consumers.get(
      `${this.name}_failed`,
      `job_failed_consumer`,
    )
    this.parentNotificationConsumer = await this.client.consumers.get(
      `${this.name}_parent_notification`,
      `parent_notification_consumer`,
    )
  }

  private async publishJobCompletedEvent(job: Job) {
    const subject = `${this.name}_completed`
    const messageHeaders = headers()
    messageHeaders.set('Nats-Msg-Id', crypto.randomUUID())
    const event: JobCompletedEvent = {
      event: 'JOB_COMPLETED',
      data: {
        jobId: job.id,
      },
    }
    await this.client.publish(
      subject,
      new TextEncoder().encode(JSON.stringify(event)),
      {
        headers: messageHeaders,
      },
    )
    console.log(`Job completed event published to subject=${subject}`)
  }

  private async publishJobFailedEvent(job: Job) {
    const subject = `${this.name}_failed`
    const messageHeaders = headers()
    messageHeaders.set('Nats-Msg-Id', crypto.randomUUID())
    const event: JobFailedEvent = {
      event: 'JOB_FAILED',
      data: {
        jobId: job.id,
      },
    }
    await this.client.publish(
      subject,
      new TextEncoder().encode(JSON.stringify(event)),
      {
        headers: messageHeaders,
      },
    )
    console.log(`Job failed event published to subject=${subject}`)
  }

  private async publishChildJobCompletedEvent(job: Job) {
    if (job.meta.parentId === undefined) return

    const subject = `${this.name}_parent_notification`
    const messageHeaders = headers()
    messageHeaders.set('Nats-Msg-Id', crypto.randomUUID())
    const event: JobChildCompletedEvent = {
      event: 'JOB_CHILD_COMPLETED',
      data: {
        childId: job.id,
        parentId: job.meta?.parentId,
      },
    }
    await this.client.publish(
      subject,
      new TextEncoder().encode(JSON.stringify(event)),
      {
        headers: messageHeaders,
      },
    )
    console.log(
      `Child job completed event published to subject=${subject} for job id=${job.id}`,
    )
  }

  private async setupConsumers(): Promise<Consumer[]> {
    const consumers: Consumer[] = []
    for (let i = 1; i <= this.priorities; i++) {
      // TODO: Naming might be wrong, independent of the queue name
      const consumerName = `worker_group_${i}`
      const subject = `${this.name}.${i}`
      try {
        await this.manager!.consumers.add(this.name, {
          filter_subject: subject,
          name: consumerName,
          durable_name: consumerName,
          ack_policy: AckPolicy.All,
        })
        try {
          const consumer = await this.client.consumers.get(
            this.name,
            consumerName,
          )
          console.log(
            `Consumer: name=${consumerName} successfully subscribed to topic ${subject} in queue ${this.name}`,
          )
          consumers.push(consumer)
        } catch (e) {
          console.error('Error while getting consumer:', e)
          throw e
        }
      } catch (e) {
        console.error(
          `Consumer: name=${this.name} error while subscribing to topic ${subject}: ${e}`,
        )
        throw e
      }
    }
    return consumers
  }

  public async stop() {
    this.running = false

    if (this.loopPromise) {
      await this.loopPromise
      await this.jobEventLoopPromise
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
      // this.jobEventLoopPromise = this.jobEventsLoop()
    }
  }

  private resetQuotesCounter() {
    for (const [, item] of this.priorityQuota!.entries()) {
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
      let jobs: JsMsg[] = []
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
        if (jobs.length > 0) break
      }

      for (const j of jobs) {
        if (this.priorityQuota)
          this.priorityQuota.get(consumerPriority)!.counter += 1

        this.limiter.inc()
        this.processTask(j)
      }

      await sleep(this.limiter.timeout())
    }
  }

  protected async jobEventsLoop() {
    while (this.running) {
      const [jobCompletedEvents, jobFailedEvents, childJobCompletedEvents] =
        await Promise.all([
          this.fetch(this.jobCompletedConsumer!, 1),
          this.fetch(this.jobFailedConsumer!, 1),
          this.fetch(this.parentNotificationConsumer!, 1),
        ])

      jobCompletedEvents.forEach((j) => this.processJobCompletedEvent(j))
      jobFailedEvents.forEach((j) => this.processJobFailedEvent(j))
      childJobCompletedEvents.forEach((j) =>
        this.processChildJobCompletedEvent(j),
      )
      await sleep(100)
    }
  }

  // TODO: Implement these methods to handle job events
  protected async processJobCompletedEvent(j: JsMsg) {}

  // TODO: Implement these methods to handle job events
  protected async processJobFailedEvent(j: JsMsg) {}

  // TODO: Implement these methods to handle job events
  protected async processChildJobCompletedEvent(j: JsMsg) {}

  protected async processTask(j: JsMsg) {
    this.processingNow += 1
    const data: Job = JSON.parse(new TextDecoder().decode(j.data))
    try {
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

        await this.publishJobFailedEvent(data)
        await this.markParentsFailed(data)
        return
      }

      console.log(
        `Job: name=${data.name} id=${data.id} is started with data=${data.data} in queue=${data.queueName}`,
      )

      const timeout = data.meta.timeout
      await this.processor(j, timeout)

      await j.ackAck()
      console.log(`Job: name=${data.name} id=${data.id} is completed`)

      await this.publishJobCompletedEvent(data)

      const parentId = data.meta?.parentId
      if (parentId) {
        const parentJob = await this.kv!.get(parentId)
        await this.publishChildJobCompletedEvent(data)
        if (parentJob) {
          const parentJobData: ParentJob = JSON.parse(
            new TextDecoder().decode(parentJob.value),
          )
          parentJobData.childrenCount -= 1

          // TODO: Race condition?
          await this.kv!.put(
            parentId,
            new TextEncoder().encode(JSON.stringify(parentJobData)),
          )

          if (parentJobData.childrenCount === 0) {
            await this.kv!.delete(parentId)
            await this.publishParentJob(parentJobData)
          }
        }
      }
    } catch (e) {
      if (e instanceof TimeoutError) {
        console.error(
          `Job: name=${data.name} id=${data.id} TimeoutError start retry`,
        )
      } else {
        console.error(
          `Error while processing job id=${data.id}: "${e}" start retry`,
        )
      }

      const newId = `${crypto.randomUUID()}_${Date.now()}`
      data.meta.retryCount += 1
      data.id = newId

      const jobBytes = new TextEncoder().encode(JSON.stringify(data))
      await j.term()
      const messageHeaders = headers()
      messageHeaders.set('Nats-Msg-Id', newId)
      await this.client.publish(j.subject, jobBytes, {
        headers: messageHeaders,
      })
    } finally {
      this.processingNow -= 1
    }
  }

  protected async markParentsFailed(jobData: Job): Promise<void> {
    const parentId = jobData.meta?.parentId
    if (!parentId) {
      return
    }

    const parentJob = await this.kv!.get(parentId)
    if (!parentJob) {
      console.log(`ParentJob with id=${parentId} not found in KV store.`)
      return
    }

    const parentJobData = JSON.parse(new TextDecoder().decode(parentJob.value))
    parentJobData.meta.failed = true

    await this.publishParentJob(parentJobData)
    await this.markParentsFailed(parentJobData)
  }

  protected async publishParentJob(parentJobData: Job): Promise<void> {
    const subject = `${parentJobData.queueName}.${parentJobData.name}.1`
    const jobBytes = new TextEncoder().encode(JSON.stringify(parentJobData))
    const msgHeaders = headers()
    msgHeaders.set('Nats-Msg-Id', parentJobData.id)
    await this.client!.publish(subject, jobBytes, {
      headers: msgHeaders,
    })
    console.log(
      `ParentJob: name=${parentJobData.name} id=${parentJobData.id} added to topic=${subject} successfully`,
    )
  }

  protected async fetch(consumer: Consumer, count: number): Promise<JsMsg[]> {
    // TODO: Maybe fail to fetch consumer info
    const consumerInfo = await consumer.info()
    try {
      const msgs = await consumer.fetch({
        max_messages: count,
        expires: this.fetchTimeout,
      })
      const awaitedMessages: JsMsg[] = []

      for await (const msg of msgs) {
        awaitedMessages.push(msg)
      }

      console.debug(
        `Consumer: name=${consumerInfo.name} fetched ${awaitedMessages.length} messages from queue=${this.name}`,
      )

      return awaitedMessages
    } catch (e) {
      if (e instanceof TimeoutError) {
        console.debug(
          `Consumer: name=${consumerInfo.name} timeout while fetching messages`,
        )
        return []
      }

      console.error(
        `Consumer: name=${consumerInfo.name} error while fetching messages from queue=${this.name}: ${e}`,
      )
      // TODO: Handle other errors
      throw e
    }
  }
}
