import { JetStreamManager } from '@nats-io/jetstream'

export const deleteAllStreams = async (jsm: JetStreamManager) => {
  const streams = await jsm.streams.list()
  for await (const stream of streams) {
    try {
      await jsm.streams.delete(stream.config.name)
      console.log(`Deleted stream: ${stream.config.name}`)
    } catch {
      console.log(`Failed to delete stream: ${stream.config.name}`)
    }
  }
}
