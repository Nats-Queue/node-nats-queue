import { JobCreateData } from './types'

export class Job {
  id: string
  name: string
  meta: {
    failed: boolean
    startTime: number
    retryCount: number
    timeout: number
    parentId?: string
  }
  data: unknown
  queueName: string

  constructor(data: JobCreateData) {
    this.id = data.id ?? `${crypto.randomUUID()}_${Date.now()}`
    this.queueName = data.queueName
    this.name = data.name
    this.data = data.data
    this.meta = {
      retryCount: 0,
      startTime: Date.now() + (data.delay ?? 0),
      failed: false,
      // TODO: Is this correct?
      timeout: data.timeout ?? 0,
    }
  }
}
