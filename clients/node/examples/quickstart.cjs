// Quickstart / smoke test. Start Locus, then:  node examples/quickstart.cjs
// (uses the built ./dist — run `npm run build` first). PORT env overrides 6379.
const { LocusClient } = require("..");

async function main() {
  const port = Number(process.env.PORT || 6379);
  const locus = new LocusClient({ host: "127.0.0.1", port });

  // --- differentiator verbs (typed) ---
  await locus.geoSet("driver:7", 13.36, 38.11, { status: "free" });
  const hits = await locus.geoSearch({
    fromLonLat: [13.4, 38.1],
    byRadius: [50, "km"],
    withDist: true,
    where: { status: "free" },
  });
  console.log("geoSearch:", hits);

  console.log("bfAdd new:", await locus.bfAdd("seen", "m1")); // 1
  console.log("bfAdd dup:", await locus.bfAdd("seen", "m1")); // 0

  await locus.idxCreate("by_status", "status");
  await locus.redis.call("HSET", "order:1", "status", "paid");
  console.log("idxGet:", await locus.idxGet("by_status", "paid"));

  // --- reactive changefeed (the part a plain driver can't surface) ---
  const feed = locus.changefeed("user:");
  const change = new Promise((resolve) => feed.on("change", resolve));
  await new Promise((resolve) => feed.on("ready", resolve)); // snapshot complete
  await locus.redis.set("user:42", "alice");
  console.log("changefeed change:", await change);
  feed.close();

  await locus.quit();
  console.log("SMOKE OK");
  process.exit(0);
}

main().catch((e) => {
  console.error("SMOKE FAIL", e);
  process.exit(1);
});
