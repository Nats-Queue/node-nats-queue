import { Job } from './job'

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

export type FlowJobCreateData = {
  job: JobCreateData
  children?: FlowJobCreateData[]
}

export type RateLimit = {
  duration: number
  max: number
}

export type DependenciesKVValue = ParentJob

export type ChildToParentsKVValue = {
  parentIds: string[]
}
