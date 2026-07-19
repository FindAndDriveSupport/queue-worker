/**
 * queue-worker — Cloudflare Worker
 *
 * The "processes each lead" stage. Sits between cron-worker (finds leads)
 * and the delivery workers (integration-worker for HubSpot/CMS/VMG,
 * digest-worker for the email digest). Owns exactly one responsibility:
 * per-destination deduplication and routing. No external API calls at all
 * — this Worker only ever talks to KV and other queues, so like
 * cron-worker's dispatch stage, it can never run out of subrequest budget
 * regardless of lead volume.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * MESSAGE CONTRACT — discovered-leads-queue (consumed here, produced by cron-worker)
 * ─────────────────────────────────────────────────────────────────────────
 * ONE message per LEAD (not per lead-destination pair) — cron-worker fetches
 * a branch's leads, runs Kredo enrichment once per lead if applicable, and
 * hands off the whole lead with its full resolved destinations array:
 *   { dealerKey, branchCode, intent, lead, approvalChance, destinations }
 * — destinations is the dealer's already-resolved array (shared CMS/VMG
 *   credentials merged in upstream, same as before) — this Worker doesn't
 *   need to know about __shared_credentials__ at all.
 *
 * For EACH destination in that array, this Worker:
 *   1. Computes the same cacheKey scheme used throughout the pipeline:
 *      branchCode ? `${dealerKey}-${branchCode}-${intent}-${dest.type}-${uniqueId}-${lead.date}`
 *                 : `${dealerKey}-${intent}-${dest.type}-${uniqueId}-${lead.date}`
 *   2. Checks LEADS_SYNC_CACHE[cacheKey] — skips if already "queued" (in
 *      flight) or "1" (done). This is the ONE place dedup decisions get
 *      made now — cron-worker doesn't check the cache at all, keeping its
 *      job purely "fetch and hand off."
 *   3. Routes to the right downstream queue:
 *        dest.type === "email"  → DIGEST_QUEUE  (digest-accumulate-queue)
 *          message: { dealerKey, branchCode, intent, lead, cacheKey }
 *          — no dest needed; digest-worker never used one, even before.
 *        anything else          → INTEGRATION_QUEUE (integration-queue)
 *          message: { dealerKey, branchCode, intent, dest, lead, approvalChance, cacheKey }
 *   4. Marks LEADS_SYNC_CACHE[cacheKey] = "queued" (1hr TTL) immediately
 *      after a successful send — prevents cron-worker's next 5-min tick
 *      from causing this same lead-destination pair to be routed twice
 *      while the downstream delivery is still in flight. The delivery
 *      workers themselves flip this to "1" (7-day TTL) once actually
 *      delivered — this Worker never writes the "done" state, only "queued".
 *
 * REQUIRED wrangler.toml:
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CACHE"
 *   [[queues.producers]] binding = "INTEGRATION_QUEUE" queue = "integration-queue"
 *   [[queues.producers]] binding = "DIGEST_QUEUE" queue = "digest-accumulate-queue"
 *   [[queues.consumers]] queue = "discovered-leads-queue"
 *     max_batch_size = 10, max_retries = 3, dead_letter_queue = "discovered-leads-dlq"
 *     (no max_concurrency cap needed — this Worker has no shared mutable
 *     state of its own; LEADS_SYNC_CACHE writes here are all independent
 *     per-cacheKey, not a read-modify-write on one shared key like the
 *     digest bucket is)
 *   [[queues.consumers]] queue = "discovered-leads-dlq"
 *     max_batch_size = 10, max_retries = 3
 */

const QUEUED_MARKER_TTL = 3600; // 1 hour — matches the dedup cache's existing "in flight" TTL scheme.
const MAX_QUEUE_RETRIES = 3;    // keep in sync with discovered-leads-queue's max_retries in wrangler.toml.

export default {
  async fetch(request) {
    return new Response("queue-worker", { status: 200 });
  },

  async queue(batch, env, ctx) {
    if (batch.queue === "discovered-leads-dlq") {
      return handleDeadLetterBatch(batch);
    }
    return handleDiscoveredLeadsBatch(batch, env);
  },
};

async function handleDiscoveredLeadsBatch(batch, env) {
  for (const message of batch.messages) {
    try {
      const routedCount = await processDiscoveredLead(message.body, env);
      console.log(`✅ [queue-worker] Routed ${routedCount} destination(s) for one lead.`);
      message.ack();
    } catch (err) {
      const { dealerKey, branchCode } = message.body;
      const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;
      const isFinalAttempt = message.attempts > MAX_QUEUE_RETRIES;

      if (isFinalAttempt) {
        console.error(`❌ [DEAD LETTER] Lead routing permanently failed for ${label} after ${message.attempts} attempts: ${err.message}`);
      } else {
        console.log(`⚠️  [queue-worker] Attempt ${message.attempts} failed for ${label}, will retry: ${err.message}`);
      }
      message.retry();
    }
  }
}

async function handleDeadLetterBatch(batch) {
  for (const message of batch.messages) {
    const { dealerKey, branchCode, intent, lead } = message.body;
    const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;
    console.error(
      `❌ [DEAD LETTER QUEUE] Lead routing for ${label} landed in DLQ. ` +
      `Lead: ${lead?.firstName} ${lead?.lastName} (${lead?.mobileNumber}), intent: ${intent}. Needs manual review.`
    );
    message.ack();
  }
}

// Processes ONE discovered lead: fans it out across its destinations,
// deduplicating and routing each. Returns how many destinations were
// actually newly routed (excludes ones skipped as already queued/done).
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

    if (dest.type === "email") {
      await env.DIGEST_QUEUE.send({ dealerKey, branchCode, intent, lead, cacheKey });
    } else {
      await env.INTEGRATION_QUEUE.send({ dealerKey, branchCode, intent, dest, lead, approvalChance, cacheKey });
    }

    await env.LEADS_SYNC_CACHE.put(cacheKey, "queued", { expirationTtl: QUEUED_MARKER_TTL });
    routedCount++;
  }

  return routedCount;
}
