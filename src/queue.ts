import {
  jetstream,
  JetStreamApiError,
  JetStreamClient,
  JetStreamManager,
} from '@nats-io/jetstream'
import { Job } from './job'
import { headers, nanos, NatsConnection } from '@nats-io/nats-core'

const DEFAULT_DEDUPLICATE_WINDOW = 2000
const MIN_DUPLICATE_WINDOW = 100

type QueueOpts = {
  connection: NatsConnection
  name: string
  priorities: number
  duplicateWindow: number
  client: JetStreamClient
}

export class Queue {
  protected name: string
  private priorities: number
  protected connection: NatsConnection
  private client: JetStreamClient
  protected manager: JetStreamManager | null = null
  protected duplicateWindow: number

  constructor({
    client,
    name,
    priorities = 1,
    connection,
    duplicateWindow = DEFAULT_DEDUPLICATE_WINDOW,
  }: QueueOpts) {
    if (!name) {
      throw new Error("Parameter 'name' cannot be empty")
    }
    if (priorities <= 0) {
      throw new Error("Parameter 'priorities' must be greater than 0")
    }
    if (duplicateWindow < MIN_DUPLICATE_WINDOW) {
      throw new Error(
        `Parameter 'duplicateWindow' must be more than or equal to ${MIN_DUPLICATE_WINDOW}`,
      )
    }

    this.client = client
    this.name = name
    this.priorities = priorities
    this.connection = connection
    this.duplicateWindow = duplicateWindow

    console.log(
      `Queue initialized with name=${this.name}, priorities=${this.priorities}`,
    )
  }

  public async setup(): Promise<void> {
    try {
      const jetstreamClient = jetstream(this.connection)
      this.manager = await jetstreamClient.jetstreamManager()

      const subjects = [`${this.name}.*`]
      await this.manager.streams.add({
        name: this.name,
        subjects: subjects,
        duplicate_window: nanos(this.duplicateWindow),
      })
      console.log(`Stream '${this.name}' created successfully.`)
    } catch (e) {
      if (e instanceof JetStreamApiError) {
        const jsError = e.apiError()
        if (jsError.err_code !== 10058) {
          throw e
        }

        console.error(
          `Stream '${this.name}' already exists. Attempting to update`,
        )
        await this.manager!.streams.update(this.name, {
          subjects: [`${this.name}.*`],
          duplicate_window: nanos(this.duplicateWindow),
        })
        console.log(`Stream '${this.name}' updated successfully.`)
      } else {
        console.error(`Error connecting to JetStream: ${e}`)
        throw e
      }
    }
  }

  // TODO: I think this is not needed
  public async close(): Promise<void> {
    try {
      if (this.connection.isClosed()) {
        console.error('Connection to NATS already closed.')
        return
      }
      await this.connection.close()
      console.log('Connection to NATS closed.')
    } catch (e) {
      // TODO: Connection closed error
      if (e instanceof Error) {
        console.error('Connection to NATS already closed.')
      } else {
        throw e
      }
    }
  }

  public async addJob(job: Job, priority: number = 1): Promise<void> {
    if (this.connection.isClosed()) {
      throw new Error('Cannot add job when NATS connection is closed.')
    }
    if (!this.manager) {
      throw new Error('Call setup before creating a new job')
    }

    if (priority >= this.priorities) {
      priority = this.priorities
    } else if (priority <= 0) {
      priority = 1
    }

    try {
      const jobData = JSON.stringify(job)
      const msgHeaders = headers()
      msgHeaders.set('Nats-Msg-Id', job.id)

      await this.client.publish(`${job.queueName}.${priority}`, jobData, {
        headers: msgHeaders,
      })
      console.log(
        `JobData ID=${job.id} added successfully. Subject: ${job.queueName}.${priority}`,
      )
    } catch (e) {
      console.error(`Failed to add job ID=${job.id}: ${e}`)
      throw e
    }
  }

  public async addJobs(jobs: Job[], priority: number = 1): Promise<void> {
    for (const job of jobs) {
      await this.addJob(job, priority)
    }
  }
}
