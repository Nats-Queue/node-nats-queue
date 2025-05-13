import { equal, deepEqual } from "assert/strict";
import { describe, it, before, after, afterEach } from "node:test";

import { connect, nanos } from "@nats-io/transport-node";
import { jetstream } from "@nats-io/jetstream";
import { NatsConnection } from "@nats-io/nats-core";
import type { JetStreamClient, JetStreamManager } from "@nats-io/jetstream";

import { DEFAULT_DEDUPLICATE_WINDOW, Queue, Worker } from "../src";

describe("Queue", () => {
  let connection: NatsConnection;
  let client: JetStreamClient;
  let manager: JetStreamManager;

  const NAME_1 = "queue1";

  before(async () => {
    connection = await connect({
      servers: "127.0.0.1:4222",
    });
    client = jetstream(connection);
    manager = await client.jetstreamManager();
  });

  afterEach(async () => {
    await manager.streams.delete(NAME_1);
  });

  after(async () => {
    await connection.close();
  });

  it("create", async () => {
    const queue1 = new Queue({
      client,
      name: NAME_1,
    });
    await queue1.setup();

    const stream = await client.streams.get(NAME_1);

    const {
      config: { name, subjects, duplicate_window },
      state: { messages },
    } = await stream.info();

    equal(name, NAME_1);
    deepEqual(subjects, [`${name}.*`]);
    equal(duplicate_window, nanos(DEFAULT_DEDUPLICATE_WINDOW));
    equal(messages, 0);
  });

  it("create multiple", async () => {
    const queue1 = new Queue({
      client,
      name: NAME_1,
    });
    await queue1.setup();

    const deduplicateWindow = 3_000;
    const queue2 = new Queue({
      client,
      name: NAME_1,
      deduplicateWindow,
    });
    await queue2.setup();

    const stream = await client.streams.get(NAME_1);

    const {
      config: { name, subjects, duplicate_window },
    } = await stream.info();

    equal(duplicate_window, nanos(deduplicateWindow));
  });

  it("create after Worker", async () => {
    const worker = new Worker({
      client,
      name: NAME_1,
      processor: async () => {},
    });
    await worker.setup();

    const queue = new Queue({
      client,
      name: NAME_1,
      deduplicateWindow: 3_000,
    });
    await queue.setup();
  });
});
