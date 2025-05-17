import { jetstream } from '@nats-io/jetstream'
import { connect, Kvm } from '@nats-io/kv'

const js = jetstream()
const kv = await new Kvm(js)
