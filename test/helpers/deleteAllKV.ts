import { Kvm } from '@nats-io/kv'
import { NatsConnection } from '@nats-io/nats-core'

export const deleteAllKV = async (nc: NatsConnection) => {
  const kvm = new Kvm(nc)
  const kvStores = await kvm.list()
  for await (const store of kvStores) {
    try {
      const kv = await kvm.open(store.bucket)
      await kv.destroy()
      console.log(`Deleted KV store: ${store.bucket}`)
    } catch {
      console.log(`Failed to delete KV store: ${store.bucket}`)
    }
  }
}
