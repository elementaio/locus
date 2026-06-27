import { EventEmitter } from "node:events";
import net from "node:net";
import tls from "node:tls";
import { RespReader, RespValue } from "./resp";

/** Connection details for a dedicated subscription socket. */
export interface SubscribeOptions {
  host: string;
  port: number;
  password?: string;
  tls?: boolean;
}

/** One keyspace change delivered live. `value` is the new string for writes, null otherwise. */
export interface Change {
  offset: number;
  op: "write" | "del" | "expire" | string;
  key: string;
  value: string | null;
}

/** A geofence membership event. */
export interface GeoEvent {
  key: string;
  value: string | null;
}

/**
 * A live changefeed subscription on its own connection (Locus pushes the
 * snapshot then live frames; a plain Redis driver can't model this custom push
 * command, so we own the socket and parse the frames ourselves).
 *
 * Events:
 *  - `"snapshot"` ({@link GeoEvent})  one per key in the initial atomic snapshot
 *  - `"ready"`    ({ count, offset }) snapshot complete; live changes follow
 *  - `"change"`   ({@link Change})    a live keyspace change
 *  - `"error"`    (Error)             socket error
 *  - `"close"`    ()                  connection closed
 */
export class Changefeed extends EventEmitter {
  protected socket?: net.Socket;
  private reader = new RespReader();
  private closed = false;

  constructor(
    private readonly opts: SubscribeOptions,
    private readonly command: string[],
  ) {
    super();
    this.connect();
  }

  private connect(): void {
    const { host, port } = this.opts;
    const sock = this.opts.tls
      ? tls.connect({ host, port, rejectUnauthorized: false })
      : net.connect({ host, port });
    this.socket = sock;
    const ready = () => this.onConnect();
    sock.once(this.opts.tls ? "secureConnect" : "connect", ready);
    sock.on("data", (d: Buffer) => {
      for (const frame of this.reader.push(d)) this.dispatch(frame);
    });
    sock.on("error", (e: Error) => this.emit("error", e));
    sock.on("close", () => {
      if (!this.closed) this.emit("close");
    });
  }

  private write(args: string[]): void {
    let out = `*${args.length}\r\n`;
    for (const a of args) out += `$${Buffer.byteLength(a)}\r\n${a}\r\n`;
    this.socket?.write(out);
  }

  private onConnect(): void {
    if (this.opts.password) this.write(["AUTH", this.opts.password]);
    this.write(this.command);
  }

  private dispatch(frame: RespValue): void {
    if (!Array.isArray(frame) || typeof frame[0] !== "string") return; // +OK etc.
    switch (frame[0]) {
      case "cdc-snapshot":
        this.emit("snapshot", {
          key: String(frame[1]),
          value: (frame[2] ?? null) as string | null,
        });
        break;
      case "cdc-snapshot-done":
        this.emit("ready", { count: Number(frame[1]), offset: Number(frame[2]) });
        break;
      case "cdc-change":
        this.emit("change", {
          offset: Number(frame[1]),
          op: String(frame[2]),
          key: String(frame[3]),
          value: (frame[4] ?? null) as string | null,
        });
        break;
    }
  }

  /** Close the subscription connection and drop all listeners. Destroys the
   *  socket so it releases the event loop (a graceful end() can hang waiting on
   *  the server). */
  close(): void {
    this.closed = true;
    this.socket?.destroy();
    this.removeAllListeners();
  }
}

/**
 * A live geofence: subscribe to a circular region and get membership
 * transitions. Tracks members so it can distinguish first entry from movement.
 *
 * Events (in addition to the base {@link Changefeed} events):
 *  - `"enter"` ({@link GeoEvent}) a key entered the region
 *  - `"move"`  ({@link GeoEvent}) a key already inside moved
 *  - `"leave"` ({@link GeoEvent}) a key left the region
 */
export class Geofence extends Changefeed {
  private readonly members = new Set<string>();

  constructor(
    opts: SubscribeOptions,
    lon: number,
    lat: number,
    radius: number,
    unit = "km",
  ) {
    super(opts, [
      "CDCSUBSCRIBE",
      "REGION",
      String(lon),
      String(lat),
      String(radius),
      unit,
    ]);
    this.on("snapshot", (s: GeoEvent) => this.members.add(s.key));
    this.on("change", (c: Change) => {
      if (c.op === "del") {
        this.members.delete(c.key);
        this.emit("leave", { key: c.key, value: null });
      } else {
        const inside = this.members.has(c.key);
        this.members.add(c.key);
        this.emit(inside ? "move" : "enter", { key: c.key, value: c.value });
      }
    });
  }
}
