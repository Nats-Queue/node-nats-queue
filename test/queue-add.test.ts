import { equal } from "assert/strict";
import { describe, it, before, after, afterEach, beforeEach } from "node:test";

import { connect } from "@nats-io/transport-node";
import { jetstream } from "@nats-io/jetstream";
import { NatsConnection } from "@nats-io/nats-core";
import type { JetStreamClient, JetStreamManager } from "@nats-io/jetstream";

import { Queue, Worker } from "../src";

describe("Queue.add()", () => {
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

  it("OK", async () => {
    const ack = await queue.add(JOB_NAME_1);

    equal(ack.duplicate, false);
    equal(ack.seq, 1);

    const stream = await client.streams.get(QUEUE_NAME_1);

    const {
      state: { messages },
    } = await stream.info();
    equal(messages, 1);
  });

  it("OK with payload", async () => {
    const ack = await queue.add(JOB_NAME_1, { x: 1 });

    equal(ack.duplicate, false);
    equal(ack.seq, 1);
  });

  it("OK with different IDs", async () => {
    const ack1 = await queue.add(JOB_NAME_1, "data", {
      id: "id1",
    });

    equal(ack1.duplicate, false);
    equal(ack1.seq, 1);

    const ack2 = await queue.add(JOB_NAME_1, "data", {
      id: "id2",
    });

    equal(ack2.duplicate, false);
    equal(ack2.seq, 2);
  });

  it("OK duplicated IDs", async () => {
    const id = "12345";

    const ack1 = await queue.add(JOB_NAME_1, "data", {
      id,
    });

    equal(ack1.duplicate, false);
    equal(ack1.seq, 1);

    const ack2 = await queue.add(JOB_NAME_1, "data", {
      id,
    });

    equal(ack2.duplicate, true);
    equal(ack2.seq, 1);
  });
});
