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
 * retried whole on the NEXT dispatch tick (up to 5 min later) rather than
 * a queue's near-immediate automatic retry. Acceptable for this volume;
 * revisit if per-branch invocation duration becomes a real problem.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * PER-DESTINATION STATUS REPORTING (added 2026-09-04)
 * ─────────────────────────────────────────────────────────────────────────
 * PRIOR BUG: cron-worker marked a lead "forwarded" (3-day marker, see its
 * file header) as soon as THIS Worker returned any 200 — regardless of
 * whether any individual destination actually succeeded. Since this
 * Worker's own /process-lead handler always returns 200 as long as it
 * doesn't crash outright (per-destination failures are caught and logged,
 * not thrown — see the try/catch below), a lead whose DealerOS delivery
 * failed (e.g. during the multi-day DealerOS 503 outage, 2026-08-22 to
 * 2026-09-04) still got marked "fully forwarded" by cron-worker. Once
 * marked, cron-worker's marker outlives the 2-day Seriti lookback window
 * by design (3-day TTL), so the lead could NEVER be reconsidered or
 * retried — it was silently and permanently lost from DealerOS's
 * perspective, even though queue-worker and integration-worker logs
 * showed nothing crash-level wrong.
 *
 * FIX: on the NORMAL forward path only (not policyOnlyUpdate — see that
 * section below), routedResults now reports a status for EVERY
 * destination in the call, not just ones newly attempted this invocation:
 *   "success"          — delivered successfully THIS call.
 *   "failed"           — attempted THIS call, delivery Worker threw/
 *                         returned non-2xx. Cache key's "queued" marker is
 *                         deliberately left in place (1hr TTL) so it
 *                         naturally expires and the next cron tick retries
 *                         it — same behavior as before, just now visible
 *                         to the caller instead of silently swallowed.
 *   "skipped-done"      — LEADS_SYNC_CACHE already had "1" for this
 *                         destination — a PRIOR call already delivered it
 *                         successfully. Safe to treat as done.
 *   "skipped-inflight"  — LEADS_SYNC_CACHE already had "queued" for this
 *                         destination — another call is (or was, if the
 *                         1hr TTL hasn't expired yet) mid-attempt. NOT
 *                         safe to treat as done — this is exactly the
 *                         ambiguous state that caused the original bug,
 *                         so it's now surfaced explicitly rather than
 *                         silently skipped.
 *
 * cron-worker now only writes its 3-day "fully forwarded" marker when
 * EVERY destination's status is "success" or "skipped-done" — see its own
 * file header for the corresponding fix.
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
 *   2. Checks LEADS_SYNC_CACHE[cacheKey] — records "skipped-done" or
 *      "skipped-inflight" (see above) and moves on without calling
 *      anything, if already "1" or "queued" respectively.
 *   3. Otherwise calls the right downstream Worker via Service Binding:
 *        dest.type === "email"  → DIGEST_WORKER  POST /accumulate
 *          body: { dealerKey, branchCode, intent, lead, cacheKey }
 *          — no dest needed; digest-worker never used one, even before.
 *        anything else          → INTEGRATION_WORKER  POST /deliver
 *          body: { dealerKey, branchCode, intent, dest, lead, approvalChance, cacheKey }
 *          — response body on success: { ok: true, externalLeadId } —
 *            externalLeadId (CRM-assigned contact/lead ID) is captured
 *            and logged here, then included in this Worker's own
 *            response under routedResults. digest-worker never returns
 *            one, so externalLeadId stays null for "email" destinations.
 *   4. Marks LEADS_SYNC_CACHE[cacheKey] = "queued" (1hr TTL) immediately
 *      before calling — same "in flight" marker purpose as before, just
 *      renamed conceptually now that there's no queue. The delivery
 *      workers themselves flip this to "1" (7-day TTL) once actually
 *      delivered — this Worker never writes the "done" state, only "queued".
 *
 * Returns { routedCount, routedResults } on success. routedCount is the
 * number of destinations newly delivered successfully THIS call (status
 * "success" only — unchanged meaning from before). routedResults on the
 * normal forward path is now an array covering EVERY destination passed
 * in, each { type, cacheKey, status, externalLeadId } — see
 * "PER-DESTINATION STATUS REPORTING" above.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * POLICY-ONLY UPDATE — Body: { ..., policyOnlyUpdate: true } (added 2026-08-29)
 * ─────────────────────────────────────────────────────────────────────────
 * Sent by cron-worker once a policy number appears in D1 for a lead that
 * was already fully synced on an earlier tick (see cron-worker's file
 * header "POLICY NUMBER BACKFILL"). This is NOT a normal lead-forward —
 * it does not run the queued/done dedup check above (that check answers
 * "has this lead-destination pair been CREATED yet", which is already
 * true and irrelevant here) and it never writes a "queued" marker.
 *
 * For each destination:
 *   - "email" calls DIGEST_WORKER POST /update-policy-number with
 *     { dealerKey, branchCode, uniqueId, date, policyNumber } — no
 *     externalLeadId exists for the digest, so it amends the lead directly
 *     inside the branch's current accumulation bucket if it's still there
 *     (see digest-worker's file header "POLICY NUMBER BACKFILL"). A miss
 *     (already sent) is a silent no-op, not an error.
 *   - Every other destination requires the ORIGINAL delivery to have fully
 *     completed (LEADS_SYNC_CACHE[cacheKey] === "1") and to have an
 *     externalLeadId on record (LEADS_SYNC_CACHE[`${cacheKey}-external-id`]).
 *     If either is missing, that destination is silently skipped — there's
 *     nothing to attach the policy number to (either the original send
 *     never finished, or that destination never returned an ID at all).
 *   - Otherwise, calls INTEGRATION_WORKER POST /update-policy-number with
 *     { dest, externalLeadId, policyNumber }. See integration-worker's
 *     file header for which destinations actually support this yet
 *     (HubSpot only, as of this change).
 *
 * routedResults entries for this path carry `policyNumberUpdated: true`
 * instead of a freshly-created externalLeadId, to distinguish them from a
 * normal create in any logging/monitoring built on this response shape.
 * NOTE: the policyOnlyUpdate path is a distinct, best-effort, one-shot
 * update — it deliberately does NOT get the "status" field treatment
 * added above, since cron-worker's own policyFwdKey marker (separate from
 * forwardKey) already governs its own retry logic independently. See
 * cron-worker's file header "POLICY NUMBER BACKFILL".
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
        const { routedCount, routedResults } = await processDiscoveredLead(body, env);
        console.log(`✅ [queue-worker] Routed ${routedCount} destination(s) for one lead.`);
        return new Response(JSON.stringify({ routedCount, routedResults }), {
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
// deduplicating and calling the right delivery Worker for each. On the
// normal forward path, routedResults now covers EVERY destination with a
// status (see file header "PER-DESTINATION STATUS REPORTING") so the
// caller (cron-worker) can tell a truly-fully-delivered lead apart from
// one that merely didn't crash.
//
// When msg.policyOnlyUpdate is true, this instead fans a policy number
// out to whichever destinations already have a completed delivery on
// record — see file header "POLICY-ONLY UPDATE".
async function processDiscoveredLead(msg, env) {
  const { dealerKey, branchCode, intent, lead, approvalChance, destinations, policyOnlyUpdate } = msg;
  const uniqueId = lead.idNumber || lead.mobileNumber || "unknown";

  let routedCount = 0;
  const routedResults = [];

  for (const dest of destinations) {
    const cacheKey = branchCode
      ? `${dealerKey}-${branchCode}-${intent}-${dest.type}-${uniqueId}-${lead.date}`
      : `${dealerKey}-${intent}-${dest.type}-${uniqueId}-${lead.date}`;

    if (policyOnlyUpdate) {
      const done = await env.LEADS_SYNC_CACHE.get(cacheKey);
      if (done !== "1") continue; // original delivery/accumulation never completed — nothing to update.

      if (dest.type === "email") {
        // digest-worker has no externalLeadId to look up — it amends the
        // lead directly inside the branch's current accumulation bucket,
        // if it's still sitting there (see digest-worker's file header
        // "POLICY NUMBER BACKFILL" for why a miss here is a silent no-op:
        // the digest may have already been sent).
        try {
          const res = await env.DIGEST_WORKER.fetch("https://internal/update-policy-number", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ dealerKey, branchCode, uniqueId, date: lead.date, policyNumber: lead.policyNumber }),
          });
          if (!res.ok) throw new Error(`digest-worker responded ${res.status}`);

          const data = await res.json().catch(() => ({}));
          if (data.updated) {
            routedCount++;
            routedResults.push({ type: dest.type, cacheKey, policyNumberUpdated: true });
            console.log(`  ✅ [queue-worker] [email] policy number added to digest bucket (${cacheKey})`);
          } else {
            console.log(`  ⏭️  [queue-worker] [email] policy number not added — ${data.reason || "lead no longer in bucket"} (${cacheKey})`);
          }
        } catch (err) {
          console.error(`  ❌ [queue-worker] Failed to push policy number [email] for ${cacheKey}: ${err.message}`);
        }
        continue;
      }

      const externalLeadId = await env.LEADS_SYNC_CACHE.get(`${cacheKey}-external-id`);
      if (!externalLeadId) continue; // that destination never returned an ID to update against.

      try {
        const res = await env.INTEGRATION_WORKER.fetch("https://internal/update-policy-number", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dest, externalLeadId, policyNumber: lead.policyNumber }),
        });
        if (!res.ok) throw new Error(`integration-worker responded ${res.status}`);

        routedCount++;
        routedResults.push({ type: dest.type, cacheKey, externalLeadId, policyNumberUpdated: true });
        console.log(`  ✅ [queue-worker] [${dest.type}] policy number attached to ${externalLeadId} (${cacheKey})`);
      } catch (err) {
        console.error(`  ❌ [queue-worker] Failed to push policy number [${dest.type}] for ${cacheKey}: ${err.message}`);
        // No marker to roll back here — cron-worker's own POLICY_FWD_MARKER_TTL
        // marker stays unset on failure, so it retries on the next tick.
      }
      continue;
    }

    // ── Normal forward path — see file header "PER-DESTINATION STATUS
    // REPORTING". Every destination gets a routedResults entry now, not
    // just newly-attempted ones, so cron-worker can tell "fully delivered"
    // from "queue-worker merely didn't crash".
    const cached = await env.LEADS_SYNC_CACHE.get(cacheKey);
    if (cached === "1") {
      routedResults.push({ type: dest.type, cacheKey, status: "skipped-done", externalLeadId: null });
      continue;
    }
    if (cached === "queued") {
      routedResults.push({ type: dest.type, cacheKey, status: "skipped-inflight", externalLeadId: null });
      continue;
    }

    await env.LEADS_SYNC_CACHE.put(cacheKey, "queued", { expirationTtl: QUEUED_MARKER_TTL });

    try {
      let externalLeadId = null;

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

        const data = await res.json().catch(() => ({}));
        externalLeadId = data?.externalLeadId ?? null;
      }

      routedCount++;
      routedResults.push({ type: dest.type, cacheKey, status: "success", externalLeadId });

      if (externalLeadId) {
        console.log(`  ✅ [queue-worker] [${dest.type}] external lead ID: ${externalLeadId} (${cacheKey})`);
      }
    } catch (err) {
      console.error(`  ❌ [queue-worker] Failed to route [${dest.type}] for ${cacheKey}: ${err.message}`);
      routedResults.push({ type: dest.type, cacheKey, status: "failed", externalLeadId: null, error: err.message });
      // Deliberately leave the "queued" marker in place rather than
      // deleting it — a delivery Worker failure here is usually transient
      // (network blip, downstream API hiccup). Since this marker has a
      // 1-hour TTL, it'll expire on its own; cron-worker no longer needs
      // to wait for that expiry to know a retry is needed, though — its
      // own forward marker now simply won't be set this tick (see its
      // file header), so it retries the whole lead on the very next tick
      // regardless of this TTL.
    }
  }

  return { routedCount, routedResults };
}
