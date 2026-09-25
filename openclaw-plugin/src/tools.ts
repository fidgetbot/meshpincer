import { Type } from "typebox";
import { defineToolPlugin } from "openclaw/plugin-sdk/tool-plugin";
import {
  defaultSocketPath,
  type JsonValue,
  MeshPincerClient,
} from "./client.js";

export const pluginConfigSchema = Type.Object({
  socketPath: Type.Optional(
    Type.String({ description: "Path to the meshpincerd Unix-domain socket." }),
  ),
  consumerId: Type.Optional(
    Type.String({
      description: "Durable event-cursor identity used by the operator inbox.",
      pattern: "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",
    }),
  ),
});

type MessageQuery = {
  mode: "unread" | "history";
  afterId: number;
  limit: number;
  acknowledgeThroughEventId?: number;
};

export async function queryMessages(
  client: MeshPincerClient,
  consumerId: string,
  query: MessageQuery,
): Promise<JsonValue> {
  if (query.mode === "history") {
    const parameters = new URLSearchParams({
      after_id: String(query.afterId),
      limit: String(query.limit),
    });
    return {
      mode: "history",
      messages: await client.get(`/v1/messages?${parameters}`),
    };
  }

  const encodedConsumerId = encodeURIComponent(consumerId);
  if (query.acknowledgeThroughEventId !== undefined) {
    await client.put(`/v1/consumers/${encodedConsumerId}/cursor`, {
      event_id: query.acknowledgeThroughEventId,
    });
  }
  const parameters = new URLSearchParams({ limit: String(query.limit) });
  const [cursor, events] = await Promise.all([
    client.get(`/v1/consumers/${encodedConsumerId}/cursor`),
    client.get(`/v1/consumers/${encodedConsumerId}/events?${parameters}`),
  ]);
  return {
    mode: "unread",
    consumer_id: consumerId,
    cursor,
    events,
    acknowledgement_required: true,
  };
}

export const toolEntry = defineToolPlugin({
  id: "meshpincer",
  name: "MeshPincer",
  description: "Operate a MeshCore companion radio through MeshPincer.",
  configSchema: pluginConfigSchema,
  tools: (tool) => [
    tool({
      name: "meshcore_status",
      description: "Get the MeshPincer service and connected MeshCore radio status.",
      parameters: Type.Object({}),
      execute: async (_params, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return { response: await client.get("/v1/status") };
      },
    }),
    tool({
      name: "meshcore_messages",
      description:
        "Read the durable MeshCore inbox or message history. Unread mode never advances its cursor automatically; after processing a result, acknowledge through its last event ID on the next call.",
      parameters: Type.Object({
        mode: Type.Optional(
          Type.Union([Type.Literal("unread"), Type.Literal("history")]),
        ),
        afterId: Type.Optional(Type.Integer({ minimum: 0 })),
        limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })),
        acknowledgeThroughEventId: Type.Optional(Type.Integer({ minimum: 0 })),
      }),
      execute: async (
        {
          mode = "unread",
          afterId = 0,
          limit = 100,
          acknowledgeThroughEventId,
        },
        config,
      ) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await queryMessages(client, config.consumerId ?? "operator-tools", {
            mode,
            afterId,
            limit,
            acknowledgeThroughEventId,
          }),
        };
      },
    }),
    tool({
      name: "meshcore_contacts",
      description: "List contacts known to the connected MeshCore companion radio.",
      parameters: Type.Object({}),
      execute: async (_params, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return { response: await client.get("/v1/contacts") };
      },
    }),
    tool({
      name: "meshcore_channels",
      description: "List configured MeshCore channels or all device channel slots.",
      parameters: Type.Object({
        includeEmpty: Type.Optional(Type.Boolean()),
      }),
      execute: async ({ includeEmpty = false }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await client.get(`/v1/channels?include_empty=${String(includeEmpty)}`),
        };
      },
    }),
    tool({
      name: "meshcore_send",
      description: "Send a direct or channel message over MeshCore.",
      parameters: Type.Object({
        kind: Type.Union([Type.Literal("direct"), Type.Literal("channel")]),
        text: Type.String({ minLength: 1, maxLength: 160 }),
        publicKey: Type.Optional(Type.String()),
        channelIndex: Type.Optional(Type.Integer({ minimum: 0 })),
      }),
      optional: true,
      execute: async ({ kind, text, publicKey, channelIndex }, config) => {
        if (new TextEncoder().encode(text).length > 160) {
          throw new Error("MeshCore text must be at most 160 UTF-8 bytes");
        }
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        const path = kind === "direct" ? "/v1/messages/direct" : "/v1/messages/channel";
        return {
          response: await client.post(path, {
            text,
            public_key: publicKey ?? null,
            channel_index: channelIndex ?? null,
          }),
        };
      },
    }),
    tool({
      name: "meshcore_repeater_status",
      description:
        "Send one rate-limited status request to a known MeshCore repeater.",
      parameters: Type.Object({
        publicKey: Type.String({ pattern: "^[0-9A-Fa-f]{64}$" }),
        transport: Type.Optional(
          Type.Union([Type.Literal("binary"), Type.Literal("legacy")]),
        ),
      }),
      execute: async ({ publicKey, transport }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        const query = transport ? `?transport=${encodeURIComponent(transport)}` : "";
        return {
          response: await client.get(
            `/v1/repeaters/${encodeURIComponent(publicKey)}/status${query}`,
          ),
        };
      },
    }),
    tool({
      name: "meshcore_repeater_acl",
      description:
        "Read one rate-limited ACL snapshot from a known MeshCore repeater. Requires repeater admin permission and returns only public-key prefixes and permission bytes.",
      parameters: Type.Object({
        publicKey: Type.String({ pattern: "^[0-9A-Fa-f]{64}$" }),
      }),
      execute: async ({ publicKey }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await client.get(
            `/v1/repeaters/${encodeURIComponent(publicKey)}/acl`,
          ),
        };
      },
    }),
    tool({
      name: "meshcore_repeater_config_get",
      description:
        "Read one typed repeater setting with a bounded, rate-limited remote request.",
      parameters: Type.Object({
        publicKey: Type.String({ pattern: "^[0-9A-Fa-f]{64}$" }),
        setting: Type.Literal("local_advert_interval_minutes"),
      }),
      execute: async ({ publicKey, setting }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await client.get(
            `/v1/repeaters/${encodeURIComponent(publicKey)}/config/${encodeURIComponent(setting)}`,
          ),
        };
      },
    }),
    tool({
      name: "meshcore_repeater_acl_set",
      description:
        "Set one companion ACL permission on a known repeater and verify it by binary ACL read-back. Refuses to mutate this radio's own admin entry.",
      parameters: Type.Object({
        publicKey: Type.String({ pattern: "^[0-9A-Fa-f]{64}$" }),
        companionPublicKey: Type.String({ pattern: "^[0-9A-Fa-f]{64}$" }),
        permissions: Type.Integer({ minimum: 0, maximum: 3 }),
      }),
      optional: true,
      execute: async ({ publicKey, companionPublicKey, permissions }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await client.patch(
            `/v1/repeaters/${encodeURIComponent(publicKey)}/acl`,
            { companion_public_key: companionPublicKey, permissions },
          ),
        };
      },
    }),
    tool({
      name: "meshcore_repeater_configure",
      description:
        "Set a typed, reversible repeater setting with mandatory pre-read and post-change read-back verification.",
      parameters: Type.Object({
        publicKey: Type.String({ pattern: "^[0-9A-Fa-f]{64}$" }),
        setting: Type.Literal("local_advert_interval_minutes"),
        value: Type.Integer({ minimum: 0, maximum: 240 }),
      }),
      optional: true,
      execute: async ({ publicKey, setting, value }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await client.patch(
            `/v1/repeaters/${encodeURIComponent(publicKey)}/config`,
            { setting, value },
          ),
        };
      },
    }),
  ],
});
