import { describe, expect, it } from "vitest";
import entry from "./index.js";
import { getToolPluginMetadata } from "openclaw/plugin-sdk/tool-plugin";

describe("meshpincer", () => {
  it("declares tool metadata", () => {
    expect(getToolPluginMetadata(entry)?.tools.map((tool) => tool.name)).toEqual([
      "meshcore_status",
      "meshcore_messages",
      "meshcore_send",
      "meshcore_repeater_status",
      "meshcore_repeater_configure",
    ]);
  });
});
