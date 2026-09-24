import { describe, expect, it } from "vitest";
import entry from "./index.js";
import { getToolPluginMetadata } from "openclaw/plugin-sdk/tool-plugin";
import { toolEntry } from "./tools.js";

describe("meshpincer", () => {
  it("declares tool metadata", () => {
    expect(getToolPluginMetadata(toolEntry)?.tools.map((tool) => tool.name)).toEqual([
      "meshcore_status",
      "meshcore_messages",
      "meshcore_contacts",
      "meshcore_channels",
      "meshcore_send",
      "meshcore_repeater_status",
      "meshcore_repeater_acl",
      "meshcore_repeater_configure",
    ]);
  });

  it("registers the native MeshCore channel", () => {
    const channel = entry.channelPlugin as {
      id: string;
      capabilities: { chatTypes: string[] };
    };
    expect(channel.id).toBe("meshcore");
    expect(channel.capabilities.chatTypes).toEqual(["direct", "group"]);
  });
});
