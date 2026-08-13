export class GrpcConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrpcConnectionError";
  }
}

export class GrpcTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrpcTimeoutError";
  }
}

export class GrpcResourceExhaustedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrpcResourceExhaustedError";
  }
}

export class GrpcValueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrpcValueError";
  }
}

export class ClientExit extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ClientExit";
  }
}

export class GrpcEmpty extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrpcEmpty";
  }
}
