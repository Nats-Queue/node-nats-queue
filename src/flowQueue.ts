import { KV, Kvm } from '@nats-io/kv'
import { FlowJob } from './flowJob'
import { Job } from './job'
import { Queue } from './queue'
import { ChildToParentsKVValue, DependenciesKVValue, ParentJob } from './types'

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

    const newChildrenForCurrentJobCount: number =
      await this.updateChildDependencies({
        parentJob: currentJob,
        childJobs: children.map((child) => child.job),
      })

    await this.updateParentDependecies({
      parentJob: currentJob,
      newChildrentCount: newChildrenForCurrentJobCount,
    })

    const deepestJobs: Job[] = []
    for (const child of children) {
      const traverseResult = await this.traverseJobTree(child, currentJob.id)
      deepestJobs.push(...traverseResult)
    }

    return deepestJobs
  }

  private async updateChildDependencies({
    parentJob,
    childJobs,
  }: {
    parentJob: Job
    childJobs: Job[]
  }) {
    let newChildrenForCurrentJobCount: number = 0
    for (const childJob of childJobs) {
      const childParentsKVEntry = await this.childParentsStore!.get(childJob.id)
      if (!childParentsKVEntry) {
        await this.childParentsStore!.put(
          childJob.id,
          JSON.stringify({
            parentIds: [parentJob.id],
          }),
        )
        newChildrenForCurrentJobCount++
        continue
      }

      const parentsInfo: ChildToParentsKVValue = childParentsKVEntry.json()

      if (parentsInfo.parentIds.includes(parentJob.id)) continue

      parentsInfo.parentIds.push(parentJob.id)
      await this.childParentsStore!.put(
        childJob.id,
        JSON.stringify(parentsInfo),
        {
          previousSeq: childParentsKVEntry.revision,
        },
      )
      newChildrenForCurrentJobCount++
    }

    return newChildrenForCurrentJobCount
  }

  private async updateParentDependecies({
    parentJob,
    newChildrentCount,
  }: {
    parentJob: Job
    newChildrentCount: number
  }) {
    const existingParentDependencies = await this.parentChildrenStore!.get(
      parentJob.id,
    )
    if (!existingParentDependencies) {
      await this.parentChildrenStore!.put(
        parentJob.id,
        JSON.stringify({
          ...parentJob,
          childrenCount: newChildrentCount,
        }),
      )
    } else {
      const parentDependencies: DependenciesKVValue =
        existingParentDependencies.json()
      parentDependencies.childrenCount += newChildrentCount
      await this.parentChildrenStore!.put(
        parentJob.id,
        JSON.stringify(parentDependencies),
        {
          previousSeq: existingParentDependencies.revision,
        },
      )
    }
  }
}
