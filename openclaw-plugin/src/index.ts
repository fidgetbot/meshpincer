import { Type } from "typebox";
import { defineToolPlugin } from "openclaw/plugin-sdk/tool-plugin";
import { defaultSocketPath, MeshPincerClient } from "./client.js";

const configSchema = Type.Object({
  socketPath: Type.Optional(
    Type.String({ description: "Path to the meshpincerd Unix-domain socket." }),
  ),
});

const scalar = Type.Union([Type.String(), Type.Number(), Type.Boolean()]);

export default defineToolPlugin({
  id: "meshpincer",
  name: "MeshPincer",
  description: "Operate a MeshCore companion radio through MeshPincer.",
  configSchema,
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
      description: "List MeshCore messages recorded after an optional message ID.",
      parameters: Type.Object({
        afterId: Type.Optional(Type.Integer({ minimum: 0 })),
        limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })),
      }),
      execute: async ({ afterId = 0, limit = 100 }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        const query = new URLSearchParams({
          after_id: String(afterId),
          limit: String(limit),
        });
        return { response: await client.get(`/v1/messages?${query}`) };
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
        text: Type.String({ minLength: 1, maxLength: 1024 }),
        publicKey: Type.Optional(Type.String()),
        channelIndex: Type.Optional(Type.Integer({ minimum: 0 })),
      }),
      optional: true,
      execute: async ({ kind, text, publicKey, channelIndex }, config) => {
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
      description: "Request status from a known MeshCore repeater.",
      parameters: Type.Object({ publicKey: Type.String({ minLength: 2 }) }),
      execute: async ({ publicKey }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await client.get(
            `/v1/repeaters/${encodeURIComponent(publicKey)}/status`,
          ),
        };
      },
    }),
    tool({
      name: "meshcore_repeater_configure",
      description: "Apply validated settings to a known MeshCore repeater.",
      parameters: Type.Object({
        publicKey: Type.String({ minLength: 2 }),
        settings: Type.Record(Type.String(), scalar),
      }),
      optional: true,
      execute: async ({ publicKey, settings }, config) => {
        const client = new MeshPincerClient(config.socketPath ?? defaultSocketPath);
        return {
          response: await client.patch(
            `/v1/repeaters/${encodeURIComponent(publicKey)}/config`,
            { settings },
          ),
        };
      },
    }),
  ],
});
