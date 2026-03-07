import fs from "node:fs";
import path from "node:path";

const MARKER = "__PLUTONIA_REVIEW_NO_REPLY_GUARD_V6__";
const LEGACY_MARKERS = [
  "__PLUTONIA_REVIEW_NO_REPLY_GUARD_V5__",
  "__PLUTONIA_REVIEW_NO_REPLY_GUARD_V4__",
  "__PLUTONIA_REVIEW_NO_REPLY_GUARD_V3__",
  "__PLUTONIA_REVIEW_NO_REPLY_GUARD_V2__",
  "__PLUTONIA_REVIEW_NO_REPLY_GUARD__",
];
const roots = [
  "/usr/local/lib/node_modules/openclaw/dist",
  "/usr/local/lib/node_modules/openclaw/dist/plugin-sdk",
];

const old1 =
  "\tconst replyQuoteText = ctxPayload.ReplyToIsQuote && ctxPayload.ReplyToBody ? ctxPayload.ReplyToBody.trim() || void 0 : void 0;\n\tconst deliveryState = createLaneDeliveryStateTracker();";
const new1 =
  "\tconst replyQuoteText = ctxPayload.ReplyToIsQuote && ctxPayload.ReplyToBody ? ctxPayload.ReplyToBody.trim() || void 0 : void 0;\n\tconst reviewDecisionIntent = resolveTelegramReviewDecisionIntent({\n\t\tctxPayload,\n\t\tmessageText: msg?.text ?? msg?.caption\n\t});\n\tconst reviewDecisionKind = resolveTelegramReviewDecisionKind({\n\t\tctxPayload,\n\t\tmessageText: msg?.text ?? msg?.caption\n\t});\n\tlet resolvedReviewDecisionIntent = reviewDecisionIntent;\n\tlet resolvedReviewDecisionKind = reviewDecisionKind;\n\tlet reviewDecisionHandledByHardGuard = false;\n\tlet reviewDecisionCommandSucceeded = false;\n\tconst deliveryState = createLaneDeliveryStateTracker();";

const old6 =
  "\t\t\t\tdeliver: async (payload, info) => {\n\t\t\t\t\tconst previewButtons = (payload.channelData?.telegram)?.buttons;";
const new6 =
  "\t\t\t\tdeliver: async (payload, info) => {\n\t\t\t\t\tif (reviewDecisionIntent) return;\n\t\t\t\t\tconst previewButtons = (payload.channelData?.telegram)?.buttons;";

const old7 = "\t\t({queuedFinal} = await dispatchReplyWithBufferedBlockDispatcher({";
const new7 = "\t\tif (!reviewDecisionIntent) ({queuedFinal} = await dispatchReplyWithBufferedBlockDispatcher({";

const old8 = "\t\t}));\n\t} finally {";
const new8 = "\t\t}));\n\t\telse reviewDecisionHandledByHardGuard = true;\n\t} finally {";

const old4 =
  "\tlet sentFallback = false;\n\tconst deliverySummary = deliveryState.snapshot();\n\tif (!deliverySummary.delivered && (deliverySummary.skippedNonSilent > 0 || deliverySummary.failedNonSilent > 0)) sentFallback = (await deliverReplies({\n\t\treplies: [{ text: EMPTY_RESPONSE_FALLBACK$1 }],\n\t\t...deliveryBaseOptions\n\t})).delivered;";
const new4 =
  "\tif (resolvedReviewDecisionIntent && reviewDecisionHandledByHardGuard) {\n\t\tconst fallbackResult = await runTelegramReviewDecisionFallback({\n\t\t\tintent: resolvedReviewDecisionIntent,\n\t\t\tkind: resolvedReviewDecisionKind,\n\t\t\truntime\n\t\t});\n\t\tif (fallbackResult.ok) {\n\t\t\treviewDecisionCommandSucceeded = true;\n\t\t\tqueuedFinal = true;\n\t\t} else runtime.error?.(danger(`telegram review hard-guard fallback failed (${resolvedReviewDecisionIntent}, kind=${resolvedReviewDecisionKind}): ${fallbackResult.error}`));\n\t}\n\tlet sentFallback = false;\n\tconst deliverySummary = deliveryState.snapshot();\n\tif (!resolvedReviewDecisionIntent) resolvedReviewDecisionIntent = resolveTelegramReviewDecisionIntent({\n\t\tctxPayload,\n\t\tmessageText: msg?.text ?? msg?.caption\n\t});\n\tif (!resolvedReviewDecisionKind) resolvedReviewDecisionKind = resolveTelegramReviewDecisionKind({\n\t\tctxPayload,\n\t\tmessageText: msg?.text ?? msg?.caption\n\t});\n\tif (!queuedFinal && !deliverySummary.delivered && resolvedReviewDecisionIntent && !reviewDecisionCommandSucceeded) {\n\t\tconst safetyResult = await runTelegramReviewDecisionFallback({\n\t\t\tintent: resolvedReviewDecisionIntent,\n\t\t\tkind: resolvedReviewDecisionKind,\n\t\t\truntime\n\t\t});\n\t\tif (safetyResult.ok) {\n\t\t\treviewDecisionCommandSucceeded = true;\n\t\t\tqueuedFinal = true;\n\t\t} else runtime.error?.(danger(`telegram review hard-guard safety fallback failed (${resolvedReviewDecisionIntent}, kind=${resolvedReviewDecisionKind}): ${safetyResult.error}`));\n\t}\n\tconst hardGuardFailedNeedsUserNotice = Boolean(resolvedReviewDecisionIntent && !reviewDecisionCommandSucceeded);\n\tif (!deliverySummary.delivered && (deliverySummary.skippedNonSilent > 0 || deliverySummary.failedNonSilent > 0 || hardGuardFailedNeedsUserNotice)) sentFallback = (await deliverReplies({\n\t\treplies: [{ text: hardGuardFailedNeedsUserNotice ? \"Could not process decision automatically. Please resend approved/rejected.\" : EMPTY_RESPONSE_FALLBACK$1 }],\n\t\t...deliveryBaseOptions\n\t})).delivered;";

const old5 = "//#endregion\n//#region src/telegram/bot-message.ts";
const insert5 = `const REVIEW_DECISION_APPROVE_STRONG_TOKENS = new Set([
\t"approve",
\t"approved",
\t"accept",
\t"accepted",
\t"aprovar",
\t"aprovado",
\t"aceitar",
\t"aceite"
]);
const REVIEW_DECISION_APPROVE_WEAK_TOKENS = new Set([
\t"yes",
\t"sim",
\t"ok"
]);
const REVIEW_DECISION_REJECT_STRONG_TOKENS = new Set([
\t"reject",
\t"rejected",
\t"decline",
\t"declined",
\t"rejeitar",
\t"rejeitado",
\t"recusar",
\t"recusado"
]);
const REVIEW_DECISION_REJECT_WEAK_TOKENS = new Set([
\t"no",
\t"nao"
]);
function normalizeReviewDecisionToken(raw) {
\tif (typeof raw !== "string") return "";
\treturn raw.normalize("NFD").replace(/[\\u0300-\\u036f]/g, "").toLowerCase().replace(/[\`"'“”‘’]/g, " ").replace(/[^\\p{L}\\p{N}\\s]/gu, " ").replace(/\\s+/g, " ").trim();
}
function detectTelegramReviewDecisionIntent(rawText) {
\tif (typeof rawText !== "string") return null;
\tconst normalized = normalizeReviewDecisionToken(rawText);
\tif (!normalized) return null;
\tconst tokens = normalized.split(" ").filter(Boolean);
\tif (tokens.length === 0) return null;
\tlet lastStrongIntent = null;
\tlet lastWeakIntent = null;
\tlet lastWeakIndex = -1;
\tfor (let idx = 0; idx < tokens.length; idx += 1) {
\t\tconst token = tokens[idx];
\t\tif (REVIEW_DECISION_APPROVE_STRONG_TOKENS.has(token)) {
\t\t\tlastStrongIntent = "approve";
\t\t\tcontinue;
\t\t}
\t\tif (REVIEW_DECISION_REJECT_STRONG_TOKENS.has(token)) {
\t\t\tlastStrongIntent = "reject";
\t\t\tcontinue;
\t\t}
\t\tif (REVIEW_DECISION_APPROVE_WEAK_TOKENS.has(token)) {
\t\t\tlastWeakIntent = "approve";
\t\t\tlastWeakIndex = idx;
\t\t\tcontinue;
\t\t}
\t\tif (REVIEW_DECISION_REJECT_WEAK_TOKENS.has(token)) {
\t\t\tlastWeakIntent = "reject";
\t\t\tlastWeakIndex = idx;
\t\t}
\t}
\tif (lastStrongIntent) return lastStrongIntent;
\tif (lastWeakIntent && (tokens.length <= 4 || lastWeakIndex >= tokens.length - 2)) return lastWeakIntent;
\treturn null;
}
function resolveTelegramReviewDecisionIntent(params) {
\tconst hint = normalizeReviewDecisionToken(typeof params?.ctxPayload?.ReviewDecisionIntentHint === "string" ? params.ctxPayload.ReviewDecisionIntentHint : "");
\tif (hint === "approve" || hint === "reject") return hint;
\tconst candidates = [
\t\tparams?.messageText,
\t\tparams?.ctxPayload?.Body,
\t\tparams?.ctxPayload?.RawBody,
\t\tparams?.ctxPayload?.CommandBody,
\t\tparams?.ctxPayload?.BodyForCommands,
\t\tparams?.ctxPayload?.BodyStripped,
\t\tparams?.ctxPayload?.Transcript,
\t\tparams?.ctxPayload?.ReplyToBody,
\t\tparams?.ctxPayload?.BodyForAgent
\t];
\tfor (const candidate of candidates) {
\t\tconst intent = detectTelegramReviewDecisionIntent(candidate);
\t\tif (intent) return intent;
\t}
\tconst packed = normalizeReviewDecisionToken(candidates.map((value) => typeof value === "string" ? value : "").filter(Boolean).join(" "));
\tif (packed) {
\t\tif (/\\b(reject|rejected|decline|declined|rejeitar|rejeitado|recusar|recusado)\\b/.test(packed)) return "reject";
\t\tif (/\\b(approve|approved|accept|accepted|aprovar|aprovado|aceitar|aceite)\\b/.test(packed)) return "approve";
\t}
\treturn null;
}
function resolveTelegramReviewDecisionKind(params) {
\tconst hint = normalizeReviewDecisionToken(typeof params?.ctxPayload?.ReviewDecisionKindHint === "string" ? params.ctxPayload.ReviewDecisionKindHint : "");
\tif (hint === "article" || hint === "content") return hint;
\tconst candidates = [
\t\tparams?.messageText,
\t\tparams?.ctxPayload?.Body,
\t\tparams?.ctxPayload?.RawBody,
\t\tparams?.ctxPayload?.CommandBody,
\t\tparams?.ctxPayload?.BodyForCommands,
\t\tparams?.ctxPayload?.BodyStripped,
\t\tparams?.ctxPayload?.Transcript,
\t\tparams?.ctxPayload?.ReplyToBody,
\t\tparams?.ctxPayload?.BodyForAgent
\t].map((value) => normalizeReviewDecisionToken(typeof value === "string" ? value : "")).filter(Boolean);
\tfor (const candidate of candidates) {
\t\tif (candidate.includes("editorial review required") || candidate.includes("articles pending content approval") || candidate.includes("private preview")) return "content";
\t\tif (candidate.includes("review required newsletter")) return "article";
\t}
\treturn "article";
}
function parseJsonObjectFromText(rawText) {
\tif (typeof rawText !== "string") return null;
\tconst value = rawText.trim();
\tif (!value) return null;
\ttry {
\t\tconst parsed = JSON.parse(value);
\t\tif (parsed && typeof parsed === "object") return parsed;
\t} catch {}
\tconst start = value.indexOf("{");
\tconst end = value.lastIndexOf("}");
\tif (start < 0 || end <= start) return null;
\ttry {
\t\tconst parsed = JSON.parse(value.slice(start, end + 1));
\t\tif (parsed && typeof parsed === "object") return parsed;
\t} catch {}
\treturn null;
}
function isTelegramReviewDecisionToolSuccess(rawText) {
\tconst parsed = parseJsonObjectFromText(rawText);
\tif (parsed && typeof parsed === "object") {
\t\tconst status = normalizeReviewDecisionToken(String(parsed.status ?? ""));
\t\tif (status === "no pending context") return true;
\t\tconst decision = normalizeReviewDecisionToken(String(parsed.decision ?? ""));
\t\tif (decision === "approved" || decision === "rejected") return true;
\t\tconst httpStatus = typeof parsed.http_status === "number" ? parsed.http_status : null;
\t\tconst appliedVia = String(parsed.applied_via ?? "");
\t\tif (httpStatus != null && httpStatus >= 200 && httpStatus < 300 && appliedVia.includes("/api/review/article-decision")) return true;
\t\tconst result = parsed.result;
\t\tif (result && typeof result === "object") {
\t\t\tconst innerStatus = normalizeReviewDecisionToken(String(result.status ?? ""));
\t\t\tif (innerStatus === "no pending context") return true;
\t\t\tconst innerDecision = normalizeReviewDecisionToken(String(result.decision ?? ""));
\t\t\tif (innerDecision === "approved" || innerDecision === "rejected") return true;
\t\t}
\t}
\tconst text = typeof rawText === "string" ? rawText : "";
\tif (/"applied_via"\\s*:\\s*"\\/api\\/review\\/article-decision"/i.test(text) && /"http_status"\\s*:\\s*2\\d\\d/i.test(text)) return true;
\treturn false;
}
async function runTelegramReviewDecisionFallback(params) {
\tconst kind = params.kind === "content" ? "content" : "article";
\tconst command = [
\t\t"docker",
\t\t"exec",
\t\t"ainews-gmail-monitor",
\t\t"python",
\t\t"/opt/ai-news-scripts/sign_review_command.py",
\t\t"--kind",
\t\tkind,
\t\t"--decision",
\t\tparams.intent
\t];
\tlet lastError = "unknown_error";
\tfor (let attempt = 1; attempt <= 2; attempt += 1) {
\t\ttry {
\t\t\tconst result = await runCommandWithTimeout(command, {
\t\t\t\ttimeoutMs: 45e3,
\t\t\t\tnoOutputTimeoutMs: 4e4
\t\t\t});
\t\t\tconst stdout = typeof result.stdout === "string" ? result.stdout.trim() : "";
\t\t\tconst stderr = typeof result.stderr === "string" ? result.stderr.trim() : "";
\t\t\tconst ok = result.code === 0 && isTelegramReviewDecisionToolSuccess(stdout);
\t\t\tif (ok) {
\t\t\t\tparams.runtime.log?.(\`telegram review hard-guard fallback applied (\${params.intent}, kind=\${kind}) attempt=\${attempt}\`);
\t\t\t\treturn { ok: true };
\t\t\t}
\t\t\tconst combined = [stdout, stderr].filter(Boolean).join(" | ").slice(0, 700);
\t\t\tlastError = \`command failed (attempt=\${attempt} exit=\${String(result.code ?? "unknown")}\${combined ? \` output=\${combined}\` : ""})\`;
\t\t} catch (err) {
\t\t\tlastError = \`command exception (attempt=\${attempt} err=\${String(err)})\`;
\t\t}
\t\tif (attempt < 2) await new Promise((resolve) => setTimeout(resolve, 700));
\t}
\treturn {
\t\tok: false,
\t\terror: lastError
\t};
}

// __PLUTONIA_REVIEW_NO_REPLY_GUARD_V6__

//#endregion
//#region src/telegram/bot-message.ts`;

function patchText(source) {
  if (source.includes(MARKER)) return { changed: false, reason: "already" };
  if (LEGACY_MARKERS.some((marker) => source.includes(marker))) return { changed: false, reason: "legacy-needs-rebuild" };
  if (!source.includes("const replyQuoteText = ctxPayload.ReplyToIsQuote") || !source.includes("createTelegramMessageProcessor")) {
    return { changed: false, reason: "not-target" };
  }
  const anchors = [old1, old6, old7, old8, old4, old5];
  if (!anchors.every((anchor) => source.includes(anchor))) {
    return { changed: false, reason: "anchor-miss" };
  }
  let next = source;
  next = next.replace(old1, new1);
  next = next.replace(old6, new6);
  next = next.replace(old7, new7);
  next = next.replace(old8, new8);
  next = next.replace(old4, new4);
  next = next.replace(old5, insert5);
  return { changed: true, text: next };
}

let patched = 0;
let already = 0;
let legacy = 0;
let scanned = 0;
for (const root of roots) {
  if (!fs.existsSync(root)) continue;
  for (const file of fs.readdirSync(root)) {
    if (!/^reply-.*\.js$/.test(file)) continue;
    const full = path.join(root, file);
    scanned += 1;
    const original = fs.readFileSync(full, "utf8");
    const result = patchText(original);
    if (result.reason === "already") already += 1;
    if (result.reason === "legacy-needs-rebuild") legacy += 1;
    if (result.changed) {
      fs.writeFileSync(full, result.text, "utf8");
      patched += 1;
      console.log(`patched ${full}`);
    }
  }
}

if (patched === 0 && already === 0 && legacy === 0) {
  throw new Error(`no reply bundle patched or pre-patched (scanned=${scanned})`);
}
if (legacy > 0) {
  throw new Error(`legacy hard-guard marker detected in ${legacy} files; clean rebuild required`);
}
console.log(`patched_files=${patched} already_patched_files=${already} legacy_patched_files=${legacy}`);
