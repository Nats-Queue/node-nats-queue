import { Job } from './job'
import { FlowJobCreateData } from './types'

export class FlowJob {
  job: Job
  children?: FlowJob[]

  constructor(data: FlowJobCreateData) {
    this.job = new Job(data.job)
    this.children = data.children
      ? data.children.map((j) => new FlowJob(j))
      : []
  }
}
