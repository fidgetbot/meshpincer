import {
  buildJsonChannelConfigSchema,
  createChatChannelPlugin,
  createChannelPluginBase,
  type OpenClawConfig,
  type PluginRuntime,
} from "openclaw/plugin-sdk/core";
import { createChannelMessageAdapterFromOutbound } from "openclaw/plugin-sdk/channel-outbound";
import { resolveAgentRoute } from "openclaw/plugin-sdk/routing";
import { defaultSocketPath, type JsonValue, MeshPincerClient } from "./client.js";

const publicKeyPattern = /^[0-9a-f]{64}$/i;
const defaultAccountId = "default";
const defaultConsumerId = "native-channel";
const defaultPollIntervalMs = 1_000;
export const meshCoreHardTextBytes = 160;
export const meshCoreAgentTargetBytes = 75;
export const meshCoreOverlongReplyNotice = "Answer too long; narrow the question.";
export const meshCoreEmptyReplyNotice = "No answer generated; try again.";

export const meshCoreAgentSystemPrompt = [
  "You are replying over MeshCore LoRa radio with scarce shared airtime.",
  "Output only the user-visible RF reply.",
  "MeshCore is a low-bandwidth LoRa surface.",
  `Use the fewest words that fully answer and aim for at most ${meshCoreAgentTargetBytes} UTF-8 bytes; ${meshCoreHardTextBytes} bytes is a hard protocol ceiling, not a target.`,
  "Send one plain-text sentence with no Markdown, preamble, restatement, or sign-off.",
  "If the user writes 'Reply: X' or requests exact text, send only X.",
  "Never send delivery, retry, missing-ACK, or automatic-resend commentary over RF; delivery uncertainty is recorded locally.",
].join(" ");

export const meshCoreAgentGuidance = meshCoreAgentSystemPrompt;

let pluginRuntime: PluginRuntime | undefined;

export function setMeshCorePluginRuntime(runtime: PluginRuntime): void {
  pluginRuntime = runtime;
}

export function meshCorePromptPolicy(context: {
  channel?: string;
  messageProvider?: string;
}): { appendSystemContext: string; toolsAllow: [] } | undefined {
  if (context.channel !== "meshcore" && context.messageProvider !== "meshcore") {
    return undefined;
  }
  return {
    appendSystemContext: meshCoreAgentSystemPrompt,
    toolsAllow: [],
  };
}

const channelConfigJsonSchema = {
  type: "object",
  additionalProperties: false,
  properties: {
    enabled: { type: "boolean", default: false },
    socketPath: {
      type: "string",
      description: "Path to the meshpincerd Unix-domain socket.",
    },
    consumerId: {
      type: "string",
      pattern: "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",
      default: defaultConsumerId,
    },
    startAtLatest: {
      type: "boolean",
      default: true,
      description: "Baseline a new channel consumer at the current event head.",
    },
    pollIntervalMs: {
      type: "integer",
      minimum: 250,
      maximum: 60_000,
      default: defaultPollIntervalMs,
    },
    allowDirectFrom: {
      type: "array",
      items: { type: "string", pattern: "^[0-9A-Fa-f]{64}$" },
      default: [],
      description: "MeshCore public keys allowed to start native OpenClaw DM turns.",
    },
    allowChannelIndices: {
      type: "array",
      items: { type: "integer", minimum: 1, maximum: 255 },
      default: [],
      description:
        "Private MeshCore channel slots allowed to start native group turns. Public (slot 0) is never accepted.",
    },
    channelSenderLabel: {
      type: "string",
      minLength: 1,
      maxLength: 32,
      description: "Optional sender label for native private-channel replies.",
    },
  },
} as const;

type MeshCoreChannelConfig = {
  enabled?: boolean;
  socketPath?: string;
  consumerId?: string;
  startAtLatest?: boolean;
  pollIntervalMs?: number;
  allowDirectFrom?: string[];
  allowChannelIndices?: number[];
  channelSenderLabel?: string;
};

export type ResolvedMeshCoreAccount = {
  accountId: string;
  enabled: boolean;
  configured: boolean;
  socketPath: string;
  consumerId: string;
  startAtLatest: boolean;
  pollIntervalMs: number;
  allowDirectFrom: string[];
  allowChannelIndices: number[];
  channelSenderLabel?: string;
};

export type MeshCoreEvent = {
  id: number;
  kind: string;
  payload: Record<string, JsonValue>;
  recorded_at?: string;
};

type CursorRecord = { consumer_id: string; event_id: number };
type MeshCoreChannelRuntime = PluginRuntime["channel"];
type ServiceStatus = {
  last_event_id?: number;
  radio?: { public_key?: string | null; node_name?: string | null };
};

type MeshCoreEventPumpOptions = {
  client: Pick<MeshPincerClient, "get" | "put">;
  consumerId: string;
  startAtLatest: boolean;
  pollIntervalMs: number;
  processEvent: (event: MeshCoreEvent) => Promise<void>;
  onError?: (error: unknown) => void;
};

function objectValue(value: JsonValue | undefined): Record<string, JsonValue> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value
    : undefined;
}

function numberValue(value: JsonValue | undefined): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function stringValue(value: JsonValue | undefined): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function parseCursor(value: JsonValue): CursorRecord {
  const object = objectValue(value);
  const consumerId = stringValue(object?.consumer_id);
  const eventId = numberValue(object?.event_id);
  if (!consumerId || eventId === undefined) {
    throw new Error("MeshPincer returned an invalid consumer cursor");
  }
  return { consumer_id: consumerId, event_id: eventId };
}

function parseStatus(value: JsonValue): ServiceStatus {
  const object = objectValue(value);
  if (!object) throw new Error("MeshPincer returned an invalid status payload");
  const radio = objectValue(object.radio);
  return {
    last_event_id: numberValue(object.last_event_id),
    radio: radio
      ? {
          public_key: stringValue(radio.public_key) ?? null,
          node_name: stringValue(radio.node_name) ?? null,
        }
      : undefined,
  };
}

function parseEvents(value: JsonValue): MeshCoreEvent[] {
  if (!Array.isArray(value)) throw new Error("MeshPincer returned an invalid event batch");
  return value.map((entry) => {
    const object = objectValue(entry);
    const id = numberValue(object?.id);
    const kind = stringValue(object?.kind);
    const payload = objectValue(object?.payload);
    if (id === undefined || !kind || !payload) {
      throw new Error("MeshPincer returned an invalid event record");
    }
    return {
      id,
      kind,
      payload,
      recorded_at: stringValue(object?.recorded_at),
    };
  });
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

export class MeshCoreEventPump {
  private initialized = false;

  constructor(private readonly options: MeshCoreEventPumpOptions) {}

  async initialize(): Promise<number> {
    const encoded = encodeURIComponent(this.options.consumerId);
    let cursor = parseCursor(
      await this.options.client.get(`/v1/consumers/${encoded}/cursor`),
    );
    if (cursor.event_id === 0 && this.options.startAtLatest) {
      const status = parseStatus(await this.options.client.get("/v1/status"));
      const head = status.last_event_id ?? 0;
      cursor = parseCursor(
        await this.options.client.put(`/v1/consumers/${encoded}/cursor`, {
          event_id: head,
        }),
      );
    }
    this.initialized = true;
    return cursor.event_id;
  }

  async pumpOnce(): Promise<number> {
    if (!this.initialized) await this.initialize();
    const encoded = encodeURIComponent(this.options.consumerId);
    const events = parseEvents(
      await this.options.client.get(`/v1/consumers/${encoded}/events?limit=100`),
    );
    for (const event of events) {
      await this.options.processEvent(event);
      await this.options.client.put(`/v1/consumers/${encoded}/cursor`, {
        event_id: event.id,
      });
    }
    return events.length;
  }

  async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        const count = await this.pumpOnce();
        if (count === 100) continue;
      } catch (error) {
        this.options.onError?.(error);
      }
      await delay(this.options.pollIntervalMs, signal);
    }
  }
}

function rawChannelConfig(cfg: OpenClawConfig): MeshCoreChannelConfig {
  const channels = cfg.channels as Record<string, unknown> | undefined;
  const value = channels?.meshcore;
  return value && typeof value === "object" ? (value as MeshCoreChannelConfig) : {};
}

export function resolveMeshCoreAccount(
  cfg: OpenClawConfig,
  accountId?: string | null,
): ResolvedMeshCoreAccount {
  const config = rawChannelConfig(cfg);
  const allowDirectFrom = (config.allowDirectFrom ?? [])
    .map((entry) => entry.toLowerCase())
    .filter((entry) => publicKeyPattern.test(entry));
  const allowChannelIndices = [
    ...new Set(
      (config.allowChannelIndices ?? []).filter(
        (entry) => Number.isInteger(entry) && entry > 0 && entry <= 255,
      ),
    ),
  ];
  return {
    accountId: accountId ?? defaultAccountId,
    enabled: config.enabled === true,
    configured: config.enabled === true,
    socketPath: config.socketPath ?? defaultSocketPath,
    consumerId: config.consumerId ?? defaultConsumerId,
    startAtLatest: config.startAtLatest !== false,
    pollIntervalMs: config.pollIntervalMs ?? defaultPollIntervalMs,
    allowDirectFrom,
    allowChannelIndices,
    channelSenderLabel: config.channelSenderLabel?.trim() || undefined,
  };
}

export function normalizeMeshCoreTarget(target: string): string | undefined {
  const trimmed = target.trim();
  const withoutPrefix = trimmed.toLowerCase().startsWith("meshcore:")
    ? trimmed.slice("meshcore:".length)
    : trimmed;
  if (publicKeyPattern.test(withoutPrefix)) return withoutPrefix.toLowerCase();
  const channel = /^channel:([1-9][0-9]{0,2})$/i.exec(withoutPrefix);
  if (!channel) return undefined;
  const index = Number(channel[1]);
  return index <= 255 ? `channel:${index}` : undefined;
}

function channelIndexFromTarget(target: string): number | undefined {
  const normalized = normalizeMeshCoreTarget(target);
  if (!normalized?.startsWith("channel:")) return undefined;
  return Number(normalized.slice("channel:".length));
}

function replyText(payload: unknown): string {
  if (!payload || typeof payload !== "object" || !("text" in payload)) return "";
  return typeof payload.text === "string" ? payload.text : "";
}

function normalizedRadioText(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

function utf8Bytes(text: string): number {
  return new TextEncoder().encode(text).length;
}

export function singleRadioReply(text: string): string {
  const normalized = normalizedRadioText(text);
  return utf8Bytes(normalized) <= meshCoreHardTextBytes
    ? normalized
    : meshCoreOverlongReplyNotice;
}

function isRestartRecoveryNotice(text: string): boolean {
  return (
    /couldn['’]t confirm whether my previous reply reached this chat/i.test(text) &&
    /won['’]t resend it automatically/i.test(text)
  );
}

export function nativeRadioReply(text: string): string | undefined {
  const normalized = normalizedRadioText(text);
  if (!normalized) return undefined;

  // OpenClaw can mirror a pending delivery-recovery notice after a Gateway
  // restart. That notice is local transport state, not a reply for scarce RF
  // airtime. Treat it as successfully suppressed so it cannot consume the
  // direct-message cooldown or block the actual agent reply that follows.
  if (isRestartRecoveryNotice(normalized)) return undefined;

  if (utf8Bytes(normalized) <= meshCoreAgentTargetBytes) return normalized;
  return undefined;
}

export function exactReplyRequest(text: string): string | undefined {
  const match = /^\s*reply(?:\s+exactly)?\s*:\s*(.*?)\s*$/is.exec(text);
  if (!match) return undefined;
  const exact = normalizedRadioText(match[1] ?? "");
  return exact || undefined;
}

export type RadioReplyRepair = (draft: string, budgetBytes: number) => Promise<string>;

function runtimeRadioReplyRepair(agentId: string): RadioReplyRepair {
  return async (draft: string, budgetBytes: number): Promise<string> => {
    if (!pluginRuntime) return "";
    const result = await pluginRuntime.subagent.complete({
      agentId,
      message: [
        `Rewrite the draft below to at most ${budgetBytes} UTF-8 bytes.`,
        "Preserve the answer's meaning and essential facts.",
        "Return only one plain-text sentence: no Markdown, preamble, explanation, sign-off, delivery status, or byte count.",
        "Draft:",
        draft,
      ].join("\n"),
      extraSystemPrompt: [
        "This is a fresh, tool-free MeshCore reply compression pass.",
        `Your entire output must be at most ${budgetBytes} UTF-8 bytes.`,
        "Output only the rewritten radio reply.",
      ].join(" "),
      // Compression is optional, never a prerequisite for replying. Keep the
      // isolated provider call brief; callers retain a deterministic fallback.
      timeoutMs: 5_000,
    });
    return result.text;
  };
}

export async function constrainedRadioReply(
  text: string,
  options: {
    budgetBytes?: number;
    hardLimitBytes?: number;
    repair?: RadioReplyRepair;
  } = {},
): Promise<string | undefined> {
  const budgetBytes = options.budgetBytes ?? meshCoreAgentTargetBytes;
  const hardLimitBytes = options.hardLimitBytes ?? meshCoreHardTextBytes;
  const normalized = normalizedRadioText(text);
  if (!normalized) return meshCoreEmptyReplyNotice;
  if (isRestartRecoveryNotice(normalized)) return undefined;

  const originalBytes = utf8Bytes(normalized);
  if (originalBytes <= budgetBytes) return normalized;

  const repair = options.repair;
  if (repair) {
    try {
      const repaired = normalizedRadioText(await repair(normalized, budgetBytes));
      const repairedBytes = utf8Bytes(repaired);
      if (repaired && repairedBytes <= budgetBytes) return repaired;
      if (originalBytes > hardLimitBytes && repaired && repairedBytes <= hardLimitBytes) {
        return repaired;
      }
    } catch {
      // Compression is an airtime optimization. Provider failure must not
      // turn a legal reply into silence.
    }
  }

  if (originalBytes <= hardLimitBytes) return normalized;
  return meshCoreOverlongReplyNotice;
}

function escapeRegex(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function privateChannelInvocation(text: string, nodeName: string): string | undefined {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized || !nodeName.trim()) return undefined;
  const escapedName = escapeRegex(nodeName.trim());
  const mention = new RegExp(
    `(?:^|\\s)@(?:${escapedName}(?=$|[\\s,:])|\\[${escapedName}\\](?=$|[\\s,:]))`,
    "i",
  );
  const match = mention.exec(normalized);
  if (!match) return undefined;
  const body = normalized.slice(match.index + match[0].length).replace(/^[\s,:-]+/, "").trim();
  return body || undefined;
}

export async function privateChannelRadioReply(
  label: string,
  text: string,
  repair?: RadioReplyRepair,
): Promise<string | undefined> {
  const prefix = `${label.trim()}: `;
  const bodyBudget = meshCoreAgentTargetBytes - utf8Bytes(prefix);
  const bodyHardLimit = meshCoreHardTextBytes - utf8Bytes(prefix);
  if (bodyHardLimit <= 0) return meshCoreOverlongReplyNotice;
  const response = await constrainedRadioReply(text, {
    budgetBytes: Math.max(0, bodyBudget),
    hardLimitBytes: bodyHardLimit,
    repair,
  });
  if (!response) return undefined;
  const combined = `${prefix}${response}`;
  if (utf8Bytes(combined) <= meshCoreHardTextBytes) return combined;
  const fallback = `${prefix}${meshCoreOverlongReplyNotice}`;
  return utf8Bytes(fallback) <= meshCoreHardTextBytes
    ? fallback
    : meshCoreOverlongReplyNotice;
}

export function acceptsDirectDeliveryState(
  state: string | undefined,
  requireAcknowledgement: boolean,
): boolean {
  return state === "acknowledged" || (!requireAcknowledgement && state === "timed_out");
}

async function sendDirect(
  client: MeshPincerClient,
  publicKey: string,
  text: string,
  requireAcknowledgement = true,
): Promise<{ messageId: string; deliveryState: string }> {
  const response = objectValue(
    await client.post("/v1/messages/direct", {
      public_key: publicKey,
      channel_index: null,
      text: singleRadioReply(text),
    }),
  );
  const messageId = numberValue(response?.message_id);
  const state = stringValue(response?.delivery_state);
  const accepted = acceptsDirectDeliveryState(state, requireAcknowledgement);
  if (messageId === undefined || state === undefined || !accepted) {
    throw new Error(`MeshCore direct delivery was not acknowledged (${state ?? "unknown"})`);
  }
  return { messageId: String(messageId), deliveryState: state };
}

async function sendChannel(
  client: MeshPincerClient,
  channelIndex: number,
  text: string,
): Promise<{ messageId: string; deliveryState: string }> {
  const response = objectValue(
    await client.post("/v1/messages/channel", {
      public_key: null,
      channel_index: channelIndex,
      text: singleRadioReply(text),
    }),
  );
  const messageId = numberValue(response?.message_id);
  const state = stringValue(response?.delivery_state);
  if (messageId === undefined || state !== "transmitted") {
    throw new Error(`MeshCore channel delivery was not transmitted (${state ?? "unknown"})`);
  }
  return { messageId: String(messageId), deliveryState: state };
}

const outbound = {
  deliveryMode: "gateway" as const,
  textChunkLimit: 160,
  resolveTarget: ({ to }: { to?: string | null }) => {
    const normalized = normalizeMeshCoreTarget(to ?? "");
    return normalized
      ? { ok: true as const, to: normalized }
      : {
          ok: false as const,
          error: new Error(
            "MeshCore target must be a 64-character public key or private channel:<index>",
          ),
        };
  },
  sendText: async ({
    to,
    text,
    cfg,
    accountId,
  }: {
    to: string;
    text: string;
    cfg: OpenClawConfig;
    accountId?: string | null;
  }) => {
    const account = resolveMeshCoreAccount(cfg, accountId);
    const target = normalizeMeshCoreTarget(to);
    if (!target) {
      throw new Error(
        "MeshCore target must be a 64-character public key or private channel:<index>",
      );
    }
    const channelIndex = channelIndexFromTarget(target);
    if (channelIndex !== undefined) {
      if (!account.allowChannelIndices.includes(channelIndex)) {
        throw new Error("MeshCore private channel is not allowlisted");
      }
      const client = new MeshPincerClient(account.socketPath);
      const status = parseStatus(await client.get("/v1/status"));
      const label = account.channelSenderLabel ?? status.radio?.node_name ?? "MeshCore agent";
      const reply = await privateChannelRadioReply(label, text);
      if (!reply) return { messageId: "suppressed-local-recovery" };
      const result = await sendChannel(
        client,
        channelIndex,
        reply,
      );
      return { messageId: result.messageId };
    }
    const reply = await constrainedRadioReply(text);
    if (!reply) return { messageId: "suppressed-local-recovery" };
    const result = await sendDirect(
      new MeshPincerClient(account.socketPath),
      target,
      reply,
    );
    return { messageId: result.messageId };
  },
};

const messageAdapter = createChannelMessageAdapterFromOutbound({
  id: "meshcore",
  outbound,
  capabilities: { text: true },
  receive: {
    defaultAckPolicy: "after_agent_dispatch",
    supportedAckPolicies: ["after_agent_dispatch"],
  },
});

const channelBase = createChannelPluginBase<ResolvedMeshCoreAccount>({
      id: "meshcore",
      meta: {
        label: "MeshCore",
        selectionLabel: "MeshCore",
        detailLabel: "MeshCore via MeshPincer",
        docsLabel: "MeshPincer",
        docsPath: "/channels/meshcore",
        blurb: "Direct and allowlisted private-group OpenClaw conversations over MeshCore radio.",
      },
      capabilities: {
        chatTypes: ["direct", "group"],
        media: false,
        reactions: false,
        threads: false,
        nativeCommands: false,
        blockStreaming: true,
      },
      reload: { configPrefixes: ["channels.meshcore"] },
      configSchema: buildJsonChannelConfigSchema(channelConfigJsonSchema),
      config: {
        listAccountIds: () => [defaultAccountId],
        defaultAccountId: () => defaultAccountId,
        resolveAccount: resolveMeshCoreAccount,
        inspectAccount(cfg, accountId) {
          const account = resolveMeshCoreAccount(cfg, accountId);
          return {
            enabled: account.enabled,
            configured: account.configured,
          };
        },
        isEnabled: (account) => account.enabled,
        isConfigured: (account) => account.configured,
        describeAccount: (account) => ({
          accountId: account.accountId,
          enabled: account.enabled,
          configured: account.configured,
          extra: { consumerId: account.consumerId },
        }),
      },
      agentPrompt: {
        messageToolHints: () => [meshCoreAgentGuidance],
      },
    });

export const meshcoreChannelPlugin = createChatChannelPlugin<ResolvedMeshCoreAccount>({
  base: {
    ...channelBase,
    config: channelBase.config!,
    messaging: {
      targetPrefixes: ["meshcore"],
      normalizeTarget: normalizeMeshCoreTarget,
      inferTargetChatType: ({ to }: { to?: string | null }) =>
        normalizeMeshCoreTarget(to ?? "")?.startsWith("channel:") ? "group" : "direct",
      targetResolver: {
        looksLikeId: (value) => normalizeMeshCoreTarget(value) !== undefined,
        hint: "<64-character MeshCore public key | channel:index>",
      },
    },
    message: messageAdapter,
    gateway: {
      startAccount: async (ctx) => {
        const runtime = ctx.channelRuntime as MeshCoreChannelRuntime | undefined;
        if (!runtime) throw new Error("MeshCore channel runtime is unavailable");
        const client = new MeshPincerClient(ctx.account.socketPath);
        const allow = new Set(ctx.account.allowDirectFrom);
        const allowedChannels = new Set(ctx.account.allowChannelIndices);
        const status = parseStatus(await client.get("/v1/status"));
        const selfName = status.radio?.node_name ?? "MeshCore agent";
        ctx.setStatus({
          accountId: ctx.account.accountId,
          enabled: true,
          configured: true,
          running: true,
          connected: true,
          lastConnectedAt: Date.now(),
          name: selfName,
        });

        const pump = new MeshCoreEventPump({
          client,
          consumerId: ctx.account.consumerId,
          startAtLatest: ctx.account.startAtLatest,
          pollIntervalMs: ctx.account.pollIntervalMs,
          processEvent: async (event) => {
            if (event.kind !== "message.received") return;
            if (stringValue(event.payload.direction) !== "inbound") return;
            const kind = stringValue(event.payload.kind);
            if (kind === "channel") {
              const channelIndex = numberValue(event.payload.channel_index);
              const rawText = stringValue(event.payload.text);
              if (channelIndex === undefined || channelIndex === 0 || !rawText) return;
              if (!allowedChannels.has(channelIndex)) return;
              const body = privateChannelInvocation(rawText, selfName);
              if (!body) return;

              const label = ctx.account.channelSenderLabel ?? selfName;
              const exact = exactReplyRequest(body);
              if (exact) {
                const response = singleRadioReply(`${label}: ${exact}`);
                await sendChannel(client, channelIndex, response);
                return;
              }

              const target = `meshcore:channel:${channelIndex}`;
              const route = resolveAgentRoute({
                cfg: ctx.cfg,
                channel: "meshcore",
                accountId: ctx.account.accountId,
                peer: { kind: "group", id: `channel-${channelIndex}` },
              });
              const ctxPayload = runtime.inbound.buildContext({
                channel: "meshcore",
                accountId: ctx.account.accountId,
                provider: "meshcore",
                surface: "meshcore",
                messageId: `meshpincer-event-${event.id}`,
                timestamp: event.recorded_at ? Date.parse(event.recorded_at) : undefined,
                from: target,
                sender: {
                  id: `channel-${channelIndex}-participant`,
                  displayLabel: "Unverified private-channel participant",
                },
                conversation: {
                  kind: "group",
                  id: `channel-${channelIndex}`,
                  label: `MeshCore private channel ${channelIndex}`,
                  nativeChannelId: String(channelIndex),
                  routePeer: { kind: "group", id: `channel-${channelIndex}` },
                },
                route: {
                  agentId: route.agentId,
                  accountId: route.accountId,
                  routeSessionKey: route.sessionKey,
                  mainSessionKey: route.mainSessionKey,
                  createIfMissing: true,
                },
                reply: {
                  to: target,
                  originatingTo: target,
                  nativeChannelId: String(channelIndex),
                  replyTarget: target,
                  deliveryTarget: target,
                  sourceReplyDeliveryMode: "channel",
                },
                message: {
                  rawBody: rawText,
                  bodyForAgent: body,
                  commandBody: body,
                  senderLabel: "Unverified private-channel participant",
                },
                access: {
                  commands: { authorized: false },
                  mentions: {
                    canDetectMention: true,
                    wasMentioned: true,
                    hasAnyMention: true,
                    explicitlyMentionedBot: true,
                    requireMention: true,
                    effectiveWasMentioned: true,
                  },
                },
                channelIngress: "unsupported",
              });
              await runtime.inbound.dispatch({
                cfg: ctx.cfg,
                channel: "meshcore",
                accountId: ctx.account.accountId,
                route: {
                  agentId: route.agentId,
                  dmScope: route.dmScope,
                  sessionKey: route.sessionKey,
                },
                ctxPayload,
                toolsAllow: [],
                record: {
                  createIfMissing: true,
                  onRecordError: (error: unknown) =>
                    ctx.log?.error?.(`meshcore group record failed: ${String(error)}`),
                },
                delivery: {
                  deliver: async (payload: unknown) => {
                    const response = await privateChannelRadioReply(
                      label,
                      replyText(payload),
                      runtimeRadioReplyRepair(route.agentId),
                    );
                    if (!response) {
                      ctx.log?.warn?.(
                        `meshcore kept local-only group transport output for event ${event.id}`,
                      );
                      return { visibleReplySent: false };
                    }
                    const result = await sendChannel(client, channelIndex, response);
                    return {
                      messageIds: [result.messageId],
                      visibleReplySent: true,
                      content: response,
                    };
                  },
                  onError: (error: unknown) =>
                    ctx.log?.error?.(`meshcore group dispatch failed: ${String(error)}`),
                },
              });
              return;
            }
            if (kind !== "direct") return;
            const peerKey = stringValue(event.payload.peer_key)?.toLowerCase();
            const text = stringValue(event.payload.text);
            if (!peerKey || !publicKeyPattern.test(peerKey) || !text) return;
            if (!allow.has(peerKey)) {
              ctx.log?.warn?.(`meshcore ignored direct event ${event.id}: sender not allowed`);
              return;
            }
            const peerPrefix = peerKey.slice(0, 12);
            const exact = exactReplyRequest(text);
            if (exact) {
              const response = singleRadioReply(exact);
              const result = await sendDirect(client, peerKey, response, false);
              if (result.deliveryState === "timed_out") {
                ctx.log?.warn?.(
                  `meshcore exact reply ${result.messageId} transmitted without ACK; not resending`,
                );
              }
              return;
            }

            const target = `meshcore:${peerKey}`;
            const route = resolveAgentRoute({
              cfg: ctx.cfg,
              channel: "meshcore",
              accountId: ctx.account.accountId,
              peer: { kind: "direct", id: peerKey },
            });
            const ctxPayload = runtime.inbound.buildContext({
              channel: "meshcore",
              accountId: ctx.account.accountId,
              provider: "meshcore",
              surface: "meshcore",
              messageId: `meshpincer-event-${event.id}`,
              timestamp: event.recorded_at ? Date.parse(event.recorded_at) : undefined,
              from: target,
              sender: { id: peerKey, displayLabel: `MeshCore ${peerPrefix}` },
              conversation: {
                kind: "direct",
                id: peerKey,
                label: `MeshCore ${peerPrefix}`,
                routePeer: { kind: "direct", id: peerKey },
              },
              route: {
                agentId: route.agentId,
                accountId: route.accountId,
                routeSessionKey: route.sessionKey,
                mainSessionKey: route.mainSessionKey,
                createIfMissing: true,
              },
              reply: {
                to: target,
                originatingTo: target,
                replyTarget: target,
                deliveryTarget: target,
                sourceReplyDeliveryMode: "direct",
              },
              message: {
                rawBody: text,
                bodyForAgent: text,
                commandBody: text,
                senderLabel: `MeshCore ${peerPrefix}`,
              },
              access: { commands: { authorized: false } },
              channelIngress: "unsupported",
            });
            await runtime.inbound.dispatch({
              cfg: ctx.cfg,
              channel: "meshcore",
              accountId: ctx.account.accountId,
              route: {
                agentId: route.agentId,
                dmScope: route.dmScope,
                sessionKey: route.sessionKey,
              },
              ctxPayload,
              toolsAllow: [],
              record: {
                createIfMissing: true,
                onRecordError: (error: unknown) =>
                  ctx.log?.error?.(`meshcore inbound record failed: ${String(error)}`),
              },
              delivery: {
                deliver: async (payload: unknown) => {
                  const response = await constrainedRadioReply(replyText(payload), {
                    repair: runtimeRadioReplyRepair(route.agentId),
                  });
                  if (!response) {
                    ctx.log?.warn?.(
                      `meshcore kept local-only direct transport output for event ${event.id}`,
                    );
                    return { visibleReplySent: false };
                  }
                  const result = await sendDirect(client, peerKey, response, false);
                  if (result.deliveryState === "timed_out") {
                    ctx.log?.warn?.(
                      `meshcore reply ${result.messageId} transmitted without ACK; not resending`,
                    );
                  }
                  return {
                    messageIds: [result.messageId],
                    visibleReplySent: true,
                    content: response,
                  };
                },
                onError: (error: unknown) =>
                  ctx.log?.error?.(`meshcore inbound dispatch failed: ${String(error)}`),
              },
            });
          },
          onError: (error) => {
            ctx.log?.error?.(`meshcore channel pump failed: ${String(error)}`);
            ctx.setStatus({
              ...ctx.getStatus(),
              running: true,
              connected: false,
              lastError: String(error),
            });
          },
        });
        await pump.run(ctx.abortSignal);
      },
    },
  },
  security: {
    dm: {
      channelKey: "meshcore",
      resolvePolicy: () => "allowlist",
      resolveAllowFrom: (account) => account.allowDirectFrom,
      defaultPolicy: "allowlist",
      normalizeEntry: (entry) => normalizeMeshCoreTarget(entry) ?? entry,
    },
  },
  threading: { topLevelReplyToMode: "none" },
  outbound: {
    base: outbound,
    attachedResults: {
      channel: "meshcore",
      sendText: outbound.sendText,
    },
  },
});
