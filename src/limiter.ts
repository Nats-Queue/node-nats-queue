export type Limiter = {
  inc(): void;
  timeout(): number;
  get(max: number): number;
};

export class IntervalLimiter implements Limiter {
  constructor(protected readonly interval: number) {}

  timeout() {
    return this.interval;
  }

  inc() {
    // NOOP
  }

  get(max: number) {
    return max;
  }
}

export class FixedWindowLimiter implements Limiter {
  protected count = 0;
  protected timestamp = 0;

  constructor(
    protected readonly max: number,
    protected readonly duration: number,
    protected readonly interval: number
  ) {}

  timeout() {
    const now = Date.now();
    const timestamp = now - (now % this.duration);

    if (timestamp !== this.timestamp) {
      this.count = 0;
      this.timestamp = timestamp;
    }

    if (this.count >= this.max) {
      this.count = 0;
      this.timestamp = timestamp + this.duration;
      return Math.max(this.timestamp - now, this.interval);
    }
    return this.interval;
  }

  inc() {
    this.count += 1;
  }

  get(max: number) {
    return Math.min(max, this.max - this.count);
  }
}
