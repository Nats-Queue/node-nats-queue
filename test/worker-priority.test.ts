import { equal } from "assert/strict";
import { describe, it, before, after, afterEach, beforeEach } from "node:test";

import { connect } from "@nats-io/transport-node";
import { jetstream } from "@nats-io/jetstream";
import { NatsConnection } from "@nats-io/nats-core";
import type { JetStreamClient, JetStreamManager } from "@nats-io/jetstream";

import { Queue, Worker } from "../src";
import { sleep } from "../src/utils";

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

  // TODO: Ask Sasha about priority processing distribution
  it("OK consume priority jobs first", async () => {
    let priority1Count = 0;
    let priority5Count = 0;
    const worker = new Worker({
      name: QUEUE_NAME_1,
      client,
      processor: async (job) => {
        console.log("job", job.subject);
        const priority = Number(job.subject.split(".")[2]);
        if (priority === 1) priority1Count++;
        if (priority === 5) priority5Count++;
      },
      concurrency: 5,
      rateLimit: {
        duration: 3000,
        max: 1,
      },
    });

    await queue.add("job1", "data", {
      priority: 1,
    });
    await queue.add("job2", "data", {
      priority: 1,
    });
    await queue.add("job3", "data", {
      priority: 5,
    });
    await queue.add("job4", "data", {
      priority: 5,
    });

    await worker.setup();
    worker.start();

    await sleep(3000);
    await worker.stop();

    equal(priority1Count, 2);
    equal(priority5Count, 1);
  });
});

// Option 1. Multiple consumers for different subjects
// orders.*.1, orders.*.2 ... orders.*.5
// Option 2. Single consumer with brute checking of priority and skipping if not needed (shit idea)
// Option 3. Multiple streams with priority ORDERS_1, ORDERS_2... ORDERS_3. Consumer checks each stream by himself in a loop. Also shitty idea :/

// Let's entertain option 1.
// We check each consumer every time, assign quota, control limits
// Then we check another consumer and so on
// On setup we create N consumers, for each priority 1...N. Holy fucking shift, it could be 65000 consumers PER 1 WORKER
