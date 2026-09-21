import {
  buildJsonChannelConfigSchema,
  createChatChannelPlugin,
  createChannelPluginBase,
  type OpenClawConfig,
} from "openclaw/plugin-sdk/core";
import { dispatchInboundDirectDm } from "openclaw/plugin-sdk/channel-inbound";
import { createChannelMessageAdapterFromOutbound } from "openclaw/plugin-sdk/channel-outbound";
import { defaultSocketPath, type JsonValue, MeshPincerClient } from "./client.js";

const publicKeyPattern = /^[0-9a-f]{64}$/i;
const defaultAccountId = "default";
const defaultConsumerId = "native-channel";
const defaultPollIntervalMs = 1_000;
export const meshCoreHardTextBytes = 160;
export const meshCoreAgentTargetBytes = 75;

export const meshCoreAgentGuidance = [
  "MeshCore is a low-bandwidth LoRa surface.",
  `Use the fewest words that fully answer and aim for at most ${meshCoreAgentTargetBytes} UTF-8 bytes; ${meshCoreHardTextBytes} bytes is a hard protocol ceiling, not a target.`,
  "Send one plain-text sentence with no Markdown, preamble, restatement, or sign-off.",
  "If the user requests exact text, send only that text.",
  "Never send delivery, retry, missing-ACK, or automatic-resend commentary over RF; delivery uncertainty is recorded locally.",
].join(" ");

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
  },
} as const;

type MeshCoreChannelConfig = {
  enabled?: boolean;
  socketPath?: string;
  consumerId?: string;
  startAtLatest?: boolean;
  pollIntervalMs?: number;
  allowDirectFrom?: string[];
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
};

export type MeshCoreEvent = {
  id: number;
  kind: string;
  payload: Record<string, JsonValue>;
  recorded_at?: string;
};

type CursorRecord = { consumer_id: string; event_id: number };
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
  return {
    accountId: accountId ?? defaultAccountId,
    enabled: config.enabled === true,
    configured: config.enabled === true,
    socketPath: config.socketPath ?? defaultSocketPath,
    consumerId: config.consumerId ?? defaultConsumerId,
    startAtLatest: config.startAtLatest !== false,
    pollIntervalMs: config.pollIntervalMs ?? defaultPollIntervalMs,
    allowDirectFrom,
  };
}

export function normalizeMeshCoreTarget(target: string): string | undefined {
  const trimmed = target.trim();
  const withoutPrefix = trimmed.toLowerCase().startsWith("meshcore:")
    ? trimmed.slice("meshcore:".length)
    : trimmed;
  return publicKeyPattern.test(withoutPrefix) ? withoutPrefix.toLowerCase() : undefined;
}

function replyText(payload: unknown): string {
  if (!payload || typeof payload !== "object" || !("text" in payload)) return "";
  return typeof payload.text === "string" ? payload.text : "";
}

export function singleRadioReply(text: string): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  const encoder = new TextEncoder();
  if (encoder.encode(normalized).length <= meshCoreHardTextBytes) return normalized;

  const ellipsis = "…";
  const budget = meshCoreHardTextBytes - encoder.encode(ellipsis).length;
  let used = 0;
  let truncated = "";
  for (const codepoint of normalized) {
    const bytes = encoder.encode(codepoint).length;
    if (used + bytes > budget) break;
    truncated += codepoint;
    used += bytes;
  }
  return `${truncated}${ellipsis}`;
}

const nativeReplyTooLong = "Reply too long. Ask again briefly.";

export function nativeRadioReply(text: string): string | undefined {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized) return undefined;

  // OpenClaw can mirror a pending delivery-recovery notice after a Gateway
  // restart. That notice is local transport state, not a reply for scarce RF
  // airtime. Treat it as successfully suppressed so it cannot consume the
  // direct-message cooldown or block the actual agent reply that follows.
  const isRestartRecovery =
    /couldn['’]t confirm whether my previous reply reached this chat/i.test(normalized) &&
    /won['’]t resend it automatically/i.test(normalized);
  if (isRestartRecovery) return undefined;

  const encoder = new TextEncoder();
  if (encoder.encode(normalized).length <= meshCoreAgentTargetBytes) return normalized;
  return nativeReplyTooLong;
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

const outbound = {
  deliveryMode: "gateway" as const,
  textChunkLimit: 160,
  resolveTarget: ({ to }: { to?: string | null }) => {
    const normalized = normalizeMeshCoreTarget(to ?? "");
    return normalized
      ? { ok: true as const, to: normalized }
      : {
          ok: false as const,
          error: new Error("MeshCore target must be a 64-character public key"),
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
    if (!target) throw new Error("MeshCore target must be a 64-character public key");
    const reply = nativeRadioReply(text);
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
        blurb: "Direct OpenClaw conversations over MeshCore radio.",
      },
      capabilities: {
        chatTypes: ["direct"],
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
      inferTargetChatType: () => "direct",
      targetResolver: {
        looksLikeId: (value) => normalizeMeshCoreTarget(value) !== undefined,
        hint: "<64-character MeshCore public key>",
      },
    },
    message: messageAdapter,
    gateway: {
      startAccount: async (ctx) => {
        const client = new MeshPincerClient(ctx.account.socketPath);
        const allow = new Set(ctx.account.allowDirectFrom);
        const status = parseStatus(await client.get("/v1/status"));
        const selfKey = status.radio?.public_key ?? "unknown";
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
            if (stringValue(event.payload.kind) !== "direct") return;
            const peerKey = stringValue(event.payload.peer_key)?.toLowerCase();
            const text = stringValue(event.payload.text);
            if (!peerKey || !publicKeyPattern.test(peerKey) || !text) return;
            if (!allow.has(peerKey)) {
              ctx.log?.warn?.(`meshcore ignored direct event ${event.id}: sender not allowed`);
              return;
            }
            const peerPrefix = peerKey.slice(0, 12);
            await dispatchInboundDirectDm({
              cfg: ctx.cfg,
              channel: "meshcore",
              channelLabel: "MeshCore",
              accountId: ctx.account.accountId,
              peer: { kind: "direct", id: peerKey },
              senderId: peerKey,
              senderAddress: `meshcore:${peerKey}`,
              recipientAddress: `meshcore:${selfKey}`,
              conversationLabel: `MeshCore ${peerPrefix}`,
              rawBody: text,
              bodyForAgent: text,
              messageId: `meshpincer-event-${event.id}`,
              timestamp: event.recorded_at ? Date.parse(event.recorded_at) : undefined,
              commandAuthorized: false,
              inboundAccessAuthorized: true,
              channelIngress: "unsupported",
              channelRuntime: ctx.channelRuntime as
                | { inbound?: { buildContext?: unknown } }
                | undefined,
              deliver: async (payload) => {
                const response = nativeRadioReply(replyText(payload));
                if (response) {
                  const result = await sendDirect(client, peerKey, response, false);
                  if (result.deliveryState === "timed_out") {
                    ctx.log?.warn?.(
                      `meshcore reply ${result.messageId} transmitted without ACK; not resending`,
                    );
                  }
                }
              },
              onRecordError: (error) =>
                ctx.log?.error?.(`meshcore inbound record failed: ${String(error)}`),
              onDispatchError: (error) =>
                ctx.log?.error?.(`meshcore inbound dispatch failed: ${String(error)}`),
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
