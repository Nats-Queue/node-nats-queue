export type Job = {
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
  // Why does job need to know about the queue name?
  queueName: string
}

export type ParentJob = Job & {
  childrenCount: number
}

export type JobCreateData = {
  name: string
  data: unknown
  queueName: string
  id?: string
  delay?: number
  timeout?: number
}

export type FlowJob = {
  job: Job
  children?: FlowJob[]
}

export type FlowJobCreateData = {
  job: JobCreateData
  children?: FlowJobCreateData[]
}
