/**
 * queue-worker — Cloudflare Worker
 *
 * The "processes each lead" stage. Sits between cron-worker (finds leads)
 * and the delivery workers (integration-worker for HubSpot/CMS/VMG,
 * digest-worker for the email digest). Owns exactly one responsibility:
 * per-destination deduplication and routing.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * NO LONGER A QUEUE CONSUMER — called directly via Service Binding
 * ─────────────────────────────────────────────────────────────────────────
 * Originally consumed discovered-leads-queue. Converted to a Service
 * Binding target (cron-worker calls env.QUEUE_WORKER.fetch(...) directly)
 * because Cloudflare Queues costs ~3 operations per message and has a
 * 10k/day budget on the free plan — per-LEAD traffic through a queue here
 * blew well past that. Service Binding calls are billed as ordinary
 * Workers requests instead, not Queues operations. See cron-worker's file
 * header "QUEUES OPERATIONS BUDGET" note for the full numbers.
 *
 * This Worker in turn calls integration-worker and digest-worker the same
 * way — direct Service Binding fetch() calls, not queue messages. The
 * ENTIRE per-lead path (cron-worker → queue-worker → integration-worker /
 * digest-worker) is now queue-free. The only real queue left anywhere in
 * this pipeline is branch-fetch-queue inside cron-worker, which
 * deliberately stays a queue (see its file header for why).
 *
 * TRADE-OFF WORTH KNOWING: Service Binding calls are synchronous, unlike
 * fire-and-forget queue messages. cron-worker now waits for this Worker to
 * finish (which itself waits for integration-worker/digest-worker to
 * finish) before moving to the next lead. Retry is also no longer
 * automatic — a failed call here means cron-worker's lead-level dedup
 * marker (see its file header) is never written, so the same lead gets
 * retried whole on the NEXT dispatch tick (up to 30 min later) rather than
 * a queue's near-immediate automatic retry. Acceptable for this volume;
 * revisit if per-branch invocation duration becomes a real problem.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * CALL CONTRACT — POST /process-lead (from cron-worker)
 * ─────────────────────────────────────────────────────────────────────────
 * Body: { dealerKey, branchCode, intent, lead, approvalChance, destinations }
 * — destinations is the dealer's already-resolved array (shared CMS/VMG
 *   credentials merged in upstream by cron-worker's dispatch stage).
 *
 * For EACH destination in that array, this Worker:
 *   1. Computes the same cacheKey scheme used throughout the pipeline:
 *      branchCode ? `${dealerKey}-${branchCode}-${intent}-${dest.type}-${uniqueId}-${lead.date}`
 *                 : `${dealerKey}-${intent}-${dest.type}-${uniqueId}-${lead.date}`
 *   2. Checks LEADS_SYNC_CACHE[cacheKey] — skips if already "queued" (in
 *      flight) or "1" (done).
 *   3. Calls the right downstream Worker via Service Binding:
 *        dest.type === "email"  → DIGEST_WORKER  POST /accumulate
 *          body: { dealerKey, branchCode, intent, lead, cacheKey }
 *          — no dest needed; digest-worker never used one, even before.
 *        anything else          → INTEGRATION_WORKER  POST /deliver
 *          body: { dealerKey, branchCode, intent, dest, lead, approvalChance, cacheKey }
 *   4. Marks LEADS_SYNC_CACHE[cacheKey] = "queued" (1hr TTL) immediately
 *      before calling — same "in flight" marker purpose as before, just
 *      renamed conceptually now that there's no queue. The delivery
 *      workers themselves flip this to "1" (7-day TTL) once actually
 *      delivered — this Worker never writes the "done" state, only "queued".
 *
 * Returns { routedCount } on success.
 *
 * REQUIRED wrangler.toml:
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CACHE"
 *   [[services]] binding = "INTEGRATION_WORKER" service = "integration-worker"
 *   [[services]] binding = "DIGEST_WORKER" service = "digest-worker"
 */

const QUEUED_MARKER_TTL = 3600; // 1 hour — matches the dedup cache's existing "in flight" TTL scheme.

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/process-lead" && request.method === "POST") {
      try {
        const body = await request.json();
        const routedCount = await processDiscoveredLead(body, env);
        console.log(`✅ [queue-worker] Routed ${routedCount} destination(s) for one lead.`);
        return new Response(JSON.stringify({ routedCount }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        console.error(`❌ [queue-worker] Failed to process lead: ${err.message}`);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response("queue-worker", { status: 200 });
  },
};

// Processes ONE discovered lead: fans it out across its destinations,
// deduplicating and calling the right delivery Worker for each. Returns
// how many destinations were actually newly routed (excludes ones skipped
// as already queued/done).
async function processDiscoveredLead(msg, env) {
  const { dealerKey, branchCode, intent, lead, approvalChance, destinations } = msg;
  const uniqueId = lead.idNumber || lead.mobileNumber || "unknown";

  let routedCount = 0;

  for (const dest of destinations) {
    const cacheKey = branchCode
      ? `${dealerKey}-${branchCode}-${intent}-${dest.type}-${uniqueId}-${lead.date}`
      : `${dealerKey}-${intent}-${dest.type}-${uniqueId}-${lead.date}`;

    const cached = await env.LEADS_SYNC_CACHE.get(cacheKey);
    if (cached) continue; // already "queued" (in flight) or "1" (done) — skip.

    await env.LEADS_SYNC_CACHE.put(cacheKey, "queued", { expirationTtl: QUEUED_MARKER_TTL });

    try {
      if (dest.type === "email") {
        const res = await env.DIGEST_WORKER.fetch("https://internal/accumulate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dealerKey, branchCode, intent, lead, cacheKey }),
        });
        if (!res.ok) throw new Error(`digest-worker responded ${res.status}`);
      } else {
        const res = await env.INTEGRATION_WORKER.fetch("https://internal/deliver", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dealerKey, branchCode, intent, dest, lead, approvalChance, cacheKey }),
        });
        if (!res.ok) throw new Error(`integration-worker responded ${res.status}`);
      }
      routedCount++;
    } catch (err) {
      console.error(`  ❌ [queue-worker] Failed to route [${dest.type}] for ${cacheKey}: ${err.message}`);
      // Deliberately leave the "queued" marker in place rather than
      // deleting it — a delivery Worker failure here is usually transient
      // (network blip, downstream API hiccup). Since this marker has a
      // 1-hour TTL, it'll expire and the NEXT lead-forward retry from
      // cron-worker will pick this destination back up naturally.
    }
  }

  return routedCount;
}
