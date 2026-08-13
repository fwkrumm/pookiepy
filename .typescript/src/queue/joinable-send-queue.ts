interface PendingConsumer<T> {
  resolve: (value: T | undefined) => void;
}

export class JoinableSendQueue<T> {
  private readonly items: T[] = [];
  private readonly consumers: PendingConsumer<T>[] = [];
  private readonly drainWaiters: Array<() => void> = [];
  private closed = false;
  private unfinished = 0;

  enqueue(item: T): void {
    if (this.closed) {
      throw new Error("Cannot enqueue into closed queue");
    }

    this.unfinished += 1;

    const consumer = this.consumers.shift();
    if (consumer) {
      consumer.resolve(item);
      return;
    }

    this.items.push(item);
  }

  async dequeue(): Promise<T | undefined> {
    if (this.items.length > 0) {
      return this.items.shift();
    }

    if (this.closed) {
      return undefined;
    }

    return await new Promise<T | undefined>((resolve) => {
      this.consumers.push({ resolve });
    });
  }

  markHandoff(): void {
    if (this.unfinished > 0) {
      this.unfinished -= 1;
    }

    if (this.unfinished === 0) {
      while (this.drainWaiters.length > 0) {
        this.drainWaiters.shift()?.();
      }
    }
  }

  async waitForDrain(): Promise<void> {
    if (this.unfinished === 0) {
      return;
    }

    await new Promise<void>((resolve) => {
      this.drainWaiters.push(resolve);
    });
  }

  close(markUnfinishedAsDone: boolean): void {
    this.closed = true;

    if (markUnfinishedAsDone) {
      this.unfinished = 0;
      while (this.drainWaiters.length > 0) {
        this.drainWaiters.shift()?.();
      }
    }

    while (this.consumers.length > 0) {
      this.consumers.shift()?.resolve(undefined);
    }
  }

  get size(): number {
    return this.items.length;
  }
}
