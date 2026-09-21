import { defineChannelPluginEntry } from "openclaw/plugin-sdk/channel-core";
import {
  getToolPluginMetadata,
  toolPluginMetadataSymbol,
} from "openclaw/plugin-sdk/tool-plugin";
import { meshcoreChannelPlugin } from "./channel.js";
import { toolEntry } from "./tools.js";

export { queryMessages } from "./tools.js";

const entry: ReturnType<typeof defineChannelPluginEntry> = defineChannelPluginEntry({
  id: "meshpincer",
  name: "MeshPincer",
  description: "MeshCore operator tools and native messaging for OpenClaw.",
  plugin: meshcoreChannelPlugin,
  registerFull(api) {
    toolEntry.register(api);
  },
});

Object.defineProperty(entry, toolPluginMetadataSymbol, {
  value: getToolPluginMetadata(toolEntry),
  enumerable: false,
});

export default entry;
