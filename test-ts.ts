import { connect } from '@nats-io/transport-node'

const client = await connect({
  servers: '',
})
