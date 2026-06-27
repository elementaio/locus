// A minimal, incremental RESP reader — just enough to consume the changefeed
// push stream on a dedicated connection. The full request/reply path uses
// ioredis; this exists only because ioredis can't surface Locus's custom push
// frames (CDCSUBSCRIBE), which arrive as arrays of bulk strings out-of-band.
//
// Handles the frame types Locus can send on a subscription: simple string (+),
// error (-), integer (:), bulk string ($), array (*), RESP3 push (>), and null.

export type RespValue = string | number | null | RespValue[];

const CR = 0x0d;

export class RespReader {
  private buf: Buffer = Buffer.alloc(0);

  /** Feed received bytes; return every complete top-level value now available. */
  push(chunk: Buffer): RespValue[] {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    const out: RespValue[] = [];
    for (;;) {
      const r = this.parse(0);
      if (!r) break; // incomplete — wait for more bytes
      out.push(r.value);
      this.buf = this.buf.subarray(r.next);
    }
    return out;
  }

  // Parse one value starting at absolute offset `i`. Returns the value and the
  // offset just past it, or null if the buffer doesn't hold a complete value yet.
  private parse(i: number): { value: RespValue; next: number } | null {
    if (i >= this.buf.length) return null;
    const type = this.buf[i];
    const line = this.readLine(i + 1);
    if (!line) return null;
    const { text, next } = line;
    switch (type) {
      case 0x2b: // '+' simple string
      case 0x2d: // '-' error (surfaced as a string; callers ignore handshake noise)
        return { value: text, next };
      case 0x3a: // ':' integer
        return { value: Number(text), next };
      case 0x24: {
        // '$' bulk string
        const len = Number(text);
        if (len < 0) return { value: null, next };
        const end = next + len;
        if (end + 2 > this.buf.length) return null; // payload + CRLF not all here
        return { value: this.buf.toString('utf8', next, end), next: end + 2 };
      }
      case 0x2a: // '*' array
      case 0x3e: {
        // '>' RESP3 push (same framing as an array)
        const len = Number(text);
        if (len < 0) return { value: null, next };
        const arr: RespValue[] = [];
        let pos = next;
        for (let k = 0; k < len; k++) {
          const el = this.parse(pos);
          if (!el) return null; // a nested element is incomplete
          arr.push(el.value);
          pos = el.next;
        }
        return { value: arr, next: pos };
      }
      case 0x5f: // '_' RESP3 null
        return { value: null, next };
      default:
        // Inline/unknown line — return as text so we never stall the stream.
        return { value: text, next };
    }
  }

  private readLine(i: number): { text: string; next: number } | null {
    const cr = this.buf.indexOf(CR, i);
    if (cr < 0 || cr + 1 >= this.buf.length) return null; // need the '\n' too
    return { text: this.buf.toString('utf8', i, cr), next: cr + 2 };
  }
}
