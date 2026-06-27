import Redis, { RedisOptions } from "ioredis";
import {
  Change,
  Changefeed,
  Geofence,
  SubscribeOptions,
} from "./changefeed";

export type LocusOptions = RedisOptions;

/** A `GEOSEARCH` query. Provide exactly one origin and exactly one shape. */
export interface GeoSearchQuery {
  fromLonLat?: [number, number];
  fromMember?: string;
  byRadius?: [number, GeoUnit];
  byBox?: [number, number, GeoUnit];
  order?: "ASC" | "DESC";
  count?: number;
  withCoord?: boolean;
  withDist?: boolean;
  /** Inline attribute equality filters (AND), e.g. `{ status: "free" }`. */
  where?: Record<string, string>;
}

export type GeoUnit = "m" | "km" | "mi" | "ft";

/** One `GEOSEARCH` hit; `dist`/`coord` are present only if requested. */
export interface GeoHit {
  key: string;
  dist?: number;
  coord?: [number, number];
}

/**
 * A Locus client. Wraps `ioredis` for every request/reply command (standard
 * Redis ops via `.redis`, the differentiator verbs via the typed helpers here)
 * and adds the reactive APIs — {@link changefeed} and {@link geofence} — that a
 * plain Redis driver can't surface.
 */
export class LocusClient {
  /** The underlying ioredis connection — use it for any standard Redis command. */
  readonly redis: Redis;
  private readonly sub: SubscribeOptions;

  constructor(options: LocusOptions = {}) {
    this.redis = new Redis(options);
    this.sub = {
      host: options.host ?? "127.0.0.1",
      port: options.port ?? 6379,
      password: typeof options.password === "string" ? options.password : undefined,
      tls: !!options.tls,
    };
  }

  // ---- geo ----------------------------------------------------------------

  /** Store a geo point, with optional inline attributes for `where` filtering. */
  geoSet(
    key: string,
    lon: number,
    lat: number,
    attrs?: Record<string, string>,
  ): Promise<unknown> {
    const extra = attrs ? Object.entries(attrs).flat() : [];
    return this.redis.call("GEOSET", key, String(lon), String(lat), ...extra);
  }

  /** `[lon, lat]` for a key, or null if absent. */
  async geoPos(key: string): Promise<[number, number] | null> {
    const r = (await this.redis.call("GEOPOS", key)) as string[] | null;
    return r && r.length >= 2 ? [Number(r[0]), Number(r[1])] : null;
  }

  /** Distance between two geo keys in `unit` (default meters). */
  async geoDist(a: string, b: string, unit: GeoUnit = "m"): Promise<number | null> {
    const r = (await this.redis.call("GEODIST", a, b, unit)) as string | null;
    return r == null ? null : Number(r);
  }

  /** Spatial search; returns parsed hits (cluster-aware — the server merges shards). */
  async geoSearch(q: GeoSearchQuery): Promise<GeoHit[]> {
    const args: string[] = [];
    if (q.fromLonLat) args.push("FROMLONLAT", String(q.fromLonLat[0]), String(q.fromLonLat[1]));
    if (q.fromMember) args.push("FROMMEMBER", q.fromMember);
    if (q.byRadius) args.push("BYRADIUS", String(q.byRadius[0]), q.byRadius[1]);
    if (q.byBox) args.push("BYBOX", String(q.byBox[0]), String(q.byBox[1]), q.byBox[2]);
    if (q.order) args.push(q.order);
    if (q.count != null) args.push("COUNT", String(q.count));
    if (q.withCoord) args.push("WITHCOORD");
    if (q.withDist) args.push("WITHDIST");
    if (q.where) for (const [f, v] of Object.entries(q.where)) args.push("WHERE", f, v);

    const raw = (await this.redis.call("GEOSEARCH", ...args)) as unknown[];
    if (!q.withCoord && !q.withDist) {
      return (raw as string[]).map((key) => ({ key }));
    }
    return (raw as unknown[][]).map((row) => {
      const hit: GeoHit = { key: String(row[0]) };
      let i = 1;
      if (q.withDist) hit.dist = Number(row[i++]);
      if (q.withCoord) {
        const c = row[i++] as string[];
        hit.coord = [Number(c[0]), Number(c[1])];
      }
      return hit;
    });
  }

  /** The cell hashtag for a point — name geo keys `{cell}id` for bounded cluster search. */
  async cell(lon: number, lat: number): Promise<string> {
    return String(await this.redis.call("CLUSTER", "CELL", String(lon), String(lat)));
  }

  // ---- sketches -----------------------------------------------------------

  /** Bloom add — resolves 1 if newly added, 0 if probably already present. */
  bfAdd(key: string, item: string): Promise<number> {
    return this.redis.call("BFADD", key, item) as Promise<number>;
  }
  bfExists(key: string, item: string): Promise<number> {
    return this.redis.call("BFEXISTS", key, item) as Promise<number>;
  }
  cmsIncrBy(key: string, item: string, by: number): Promise<unknown> {
    return this.redis.call("CMSINCRBY", key, item, String(by));
  }
  async cmsQuery(key: string, item: string): Promise<number> {
    return Number(await this.redis.call("CMSQUERY", key, item));
  }
  topkAdd(key: string, ...items: string[]): Promise<unknown> {
    return this.redis.call("TOPKADD", key, ...items);
  }
  topkList(key: string): Promise<string[]> {
    return this.redis.call("TOPKLIST", key) as Promise<string[]>;
  }
  tdAdd(key: string, ...values: number[]): Promise<unknown> {
    return this.redis.call("TDADD", key, ...values.map(String));
  }
  async tdQuantile(key: string, ...quantiles: number[]): Promise<number[]> {
    const r = (await this.redis.call(
      "TDQUANTILE",
      key,
      ...quantiles.map(String),
    )) as string[];
    return r.map(Number);
  }

  // ---- conditional writes (CAS) ------------------------------------------

  /** Set `key` to `next` only if it currently equals `expected`. 1 on swap. */
  cas(key: string, expected: string, next: string): Promise<number> {
    return this.redis.call("CAS", key, expected, next) as Promise<number>;
  }
  caDel(key: string, expected: string): Promise<number> {
    return this.redis.call("CADEL", key, expected) as Promise<number>;
  }
  setMax(key: string, value: number): Promise<unknown> {
    return this.redis.call("SETMAX", key, String(value));
  }
  incrCap(key: string, by: number, cap: number): Promise<unknown> {
    return this.redis.call("INCRCAP", key, String(by), String(cap));
  }

  // ---- secondary index ----------------------------------------------------

  idxCreate(name: string, field: string): Promise<unknown> {
    return this.redis.call("IDXCREATE", name, field);
  }
  idxDrop(name: string): Promise<unknown> {
    return this.redis.call("IDXDROP", name);
  }
  idxGet(name: string, value: string): Promise<string[]> {
    return this.redis.call("IDXGET", name, value) as Promise<string[]>;
  }
  idxRange(name: string, min: string, max: string, count?: number): Promise<string[]> {
    const extra = count != null ? ["COUNT", String(count)] : [];
    return this.redis.call("IDXRANGE", name, min, max, ...extra) as Promise<string[]>;
  }

  // ---- changefeed: pull ---------------------------------------------------

  /** Read changes after `offset` (needs `LOCUS_CDC_MAXLEN` retention on the server). */
  async cdcRead(
    offset: number,
    opts: { count?: number; prefix?: string } = {},
  ): Promise<Change[]> {
    const args = [String(offset)];
    if (opts.count != null) args.push("COUNT", String(opts.count));
    if (opts.prefix != null) args.push("PREFIX", opts.prefix);
    const raw = (await this.redis.call("CDCREAD", ...args)) as unknown[][];
    return raw.map((r) => ({
      offset: Number(r[0]),
      op: String(r[1]),
      key: String(r[2]),
      value: (r[3] ?? null) as string | null,
    }));
  }

  // ---- cluster: cross-shard changefeed -----------------------------------

  /** Global, HLC-ordered changefeed merged across all shards. Advance `sinceHlc`. */
  async clusterCdcMerge(
    sinceHlc = 0,
    count?: number,
  ): Promise<Array<{ hlc: number; op: string; key: string; value: string | null }>> {
    const extra = count != null ? ["COUNT", String(count)] : [];
    const raw = (await this.redis.call(
      "CLUSTER",
      "CDCMERGE",
      String(sinceHlc),
      ...extra,
    )) as unknown[][];
    return raw.map((r) => ({
      hlc: Number(r[0]),
      op: String(r[1]),
      key: String(r[2]),
      value: (r[3] ?? null) as string | null,
    }));
  }

  // ---- reactive (the part a plain driver can't do) ------------------------

  /** Live changefeed over a dedicated connection. Pass a key prefix to filter. */
  changefeed(prefix?: string): Changefeed {
    return new Changefeed(this.sub, prefix ? ["CDCSUBSCRIBE", prefix] : ["CDCSUBSCRIBE"]);
  }

  /** Live geofence over a dedicated connection — emits enter/move/leave. */
  geofence(lon: number, lat: number, radius: number, unit: GeoUnit = "km"): Geofence {
    return new Geofence(this.sub, lon, lat, radius, unit);
  }

  /** Close the underlying ioredis connection. */
  quit(): Promise<"OK"> {
    return this.redis.quit();
  }
}
