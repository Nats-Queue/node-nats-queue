import { equal, deepEqual } from "assert/strict";
import { describe, it, before, after, afterEach, beforeEach } from "node:test";

import { connect, nanos } from "@nats-io/transport-node";
import { jetstream } from "@nats-io/jetstream";
import { NatsConnection } from "@nats-io/nats-core";
import type {
  ConsumerInfo,
  JetStreamClient,
  JetStreamManager,
} from "@nats-io/jetstream";

import { Queue, Worker } from "../src";

describe("Worker start() & stop()", () => {
  let connection: NatsConnection;
  let client: JetStreamClient;
  let manager: JetStreamManager;
  let queue: Queue;

  const NAME_1 = "queue1";

  before(async () => {
    connection = await connect({
      servers: "127.0.0.1:4222",
    });
    client = jetstream(connection);
    manager = await client.jetstreamManager();
  });

  beforeEach(async () => {
    queue = new Queue({
      name: NAME_1,
      client,
    });
    await queue.setup();
  });

  afterEach(async () => {
    await manager.consumers.delete(NAME_1, NAME_1);
    await manager.streams.delete(NAME_1);
  });

  after(async () => {
    await connection.close();
  });

  it("start and stop", async () => {
    const worker = new Worker({
      client,
      name: NAME_1,
      processor: async () => {},
    });
    await worker.setup();

    worker.start();

    await worker.stop();
  });
});
