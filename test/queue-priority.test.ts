import { equal } from "assert/strict";
import { describe, it, before, after, afterEach, beforeEach } from "node:test";

import { connect } from "@nats-io/transport-node";
import { jetstream } from "@nats-io/jetstream";
import { NatsConnection } from "@nats-io/nats-core";
import type { JetStreamClient, JetStreamManager } from "@nats-io/jetstream";

import { Queue } from "../src";

describe("Queue.add() with priority", () => {
  let connection: NatsConnection;
  let client: JetStreamClient;
  let manager: JetStreamManager;
  let queue: Queue;

  const QUEUE_NAME_1 = "queue1";
  const JOB_NAME_1 = "job1";

  before(async () => {
    connection = await connect({
      servers: "127.0.0.1:4222",
    });
    client = jetstream(connection);
    manager = await client.jetstreamManager();
  });

  beforeEach(async () => {
    queue = new Queue({
      client,
      name: QUEUE_NAME_1,
    });

    await queue.setup();
  });

  afterEach(async () => {
    await manager.streams.delete(QUEUE_NAME_1);
  });

  after(async () => {
    await connection.close();
  });

  it("OK with priority", async () => {
    const ack = await queue.add(JOB_NAME_1, "data", {
      priority: 1,
    });
    equal(ack.duplicate, false);
    equal(ack.seq, 1);
    const stream = await client.streams.get(QUEUE_NAME_1);
    const {
      state: { messages },
    } = await stream.info();
    equal(messages, 1);
  });

  it("OK without priority", async () => {
    const ack = await queue.add(JOB_NAME_1, "data");
    equal(ack.duplicate, false);
    equal(ack.seq, 1);
    const stream = await client.streams.get(QUEUE_NAME_1);
    const {
      state: { messages },
    } = await stream.info();
    equal(messages, 1);
  });
});
