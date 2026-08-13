interface WaitingGet<T> {
  resolve: (value: T | undefined) => void;
}

interface WaitingPut<T> {
  value: T;
  resolve: () => void;
}

export class AsyncReceiveQueue<T> {
  private readonly items: T[] = [];
  private readonly waitingGets: WaitingGet<T>[] = [];
  private readonly waitingPuts: WaitingPut<T>[] = [];
  private readonly maxSize: number;
  private closed = false;

  constructor(maxSize: number) {
    this.maxSize = maxSize;
  }

  async put(value: T): Promise<void> {
    if (this.closed) {
      return;
    }

    const waiter = this.waitingGets.shift();
    if (waiter) {
      waiter.resolve(value);
      return;
    }

    if (this.maxSize === 0 || this.items.length < this.maxSize) {
      this.items.push(value);
      return;
    }

    await new Promise<void>((resolve) => {
      this.waitingPuts.push({ value, resolve });
    });
  }

  async get(timeoutMs?: number): Promise<T | undefined> {
    const immediate = this.shiftImmediate();
    if (immediate !== undefined) {
      return immediate;
    }

    if (this.closed) {
      return undefined;
    }

    const getPromise = new Promise<T | undefined>((resolve) => {
      this.waitingGets.push({ resolve });
    });

    if (timeoutMs === undefined) {
      return await getPromise;
    }

    return await Promise.race<T | undefined>([
      getPromise,
      new Promise<T | undefined>((resolve) => {
        setTimeout(() => resolve(undefined), timeoutMs);
      })
    ]);
  }

  getNowait(): T | undefined {
    return this.shiftImmediate();
  }

  close(): void {
    this.closed = true;

    while (this.waitingGets.length > 0) {
      this.waitingGets.shift()?.resolve(undefined);
    }

    while (this.waitingPuts.length > 0) {
      this.waitingPuts.shift()?.resolve();
    }
  }

  private shiftImmediate(): T | undefined {
    if (this.items.length === 0) {
      return undefined;
    }

    const item = this.items.shift();
    this.releaseWaitingPutIfSpace();
    return item;
  }

  private releaseWaitingPutIfSpace(): void {
    if (this.maxSize !== 0 && this.items.length >= this.maxSize) {
      return;
    }

    const waitingPut = this.waitingPuts.shift();
    if (!waitingPut) {
      return;
    }

    const waitingGet = this.waitingGets.shift();
    if (waitingGet) {
      waitingGet.resolve(waitingPut.value);
      waitingPut.resolve();
      return;
    }

    this.items.push(waitingPut.value);
    waitingPut.resolve();
  }
}
