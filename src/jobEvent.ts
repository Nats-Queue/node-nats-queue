type Event<T, D> = {
  event: T
  data: D
}

export type JobCompletedEvent = Event<'JOB_COMPLETED', { jobId: string }>
export type JobFailedEvent = Event<'JOB_FAILED', { jobId: string }>
export type JobChildCompletedEvent = Event<
  'JOB_CHILD_COMPLETED',
  { childId: string; parentId: string }
>
export type JobChildFailedEvent = Event<
  'JOB_CHILD_FAILED',
  { childId: string; parentId: string }
>

export type JobEvent =
  | JobChildFailedEvent
  | JobChildCompletedEvent
  | JobCompletedEvent
  | JobFailedEvent
