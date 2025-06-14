import { KV, Kvm } from '@nats-io/kv'
import { FlowJob } from './flowJob'
import { Job } from './job'
import { Queue } from './queue'
import { ChildToParentsKVValue } from './types'

export class FlowQueue extends Queue {
  private parentChildrenStore: KV | null = null
  private childParentsStore: KV | null = null

  public override async setup(): Promise<void> {
    try {
      await super.setup()
      const kvm = await new Kvm(this.connection)
      this.parentChildrenStore = await kvm.create(`${this.name}_parent_id`)
      this.childParentsStore = await kvm.create(`${this.name}_parents`)
    } catch (e) {
      console.error(`Error connecting to JetStream: ${e}`)
      throw e
    }
  }

  public async addFlowJob(tree: FlowJob, priority: number = 1): Promise<void> {
    const deepestJobs = await this.traverseJobTree(tree)
    await this.addJobs(deepestJobs, priority)
  }

  private async traverseJobTree(
    node: FlowJob,
    parentId: string | null = null,
  ): Promise<Job[]> {
    const currentJob = node.job
    if (parentId) {
      currentJob.meta.parentId = parentId
    }

    const children = node.children || []
    if (children.length === 0) {
      return [currentJob]
    }

    await this.parentChildrenStore!.put(
      currentJob.id,
      new TextEncoder().encode(
        JSON.stringify({
          ...currentJob,
          childrenCount: children.length,
        }),
      ),
    )

    // TODO: Race condition handling
    for (const child of children) {
      const parentIds = await this.childParentsStore!.get(child.job.id)
      if (!parentIds) {
        await this.childParentsStore!.put(
          child.job.id,
          new TextEncoder().encode(
            JSON.stringify({
              parentIds: [currentJob.id],
            }),
          ),
        )
        continue
      }

      const existingParentIds: ChildToParentsKVValue = JSON.parse(
        new TextDecoder().decode(parentIds.value),
      )

      existingParentIds.parentIds.push(currentJob.id)
      await this.childParentsStore!.put(
        child.job.id,
        new TextEncoder().encode(JSON.stringify(existingParentIds)),
      )
    }

    const deepestJobs: Job[] = []
    for (const child of children) {
      const traverseResult = await this.traverseJobTree(child, currentJob.id)
      deepestJobs.push(...traverseResult)
    }

    return deepestJobs
  }
}
