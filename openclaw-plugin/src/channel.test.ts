import { describe, expect, it, vi } from "vitest";
import {
  acceptsDirectDeliveryState,
  constrainedRadioReply,
  exactReplyRequest,
  meshCoreAgentGuidance,
  meshCoreAgentSystemPrompt,
  meshCoreAgentTargetBytes,
  meshCoreHardTextBytes,
  MeshCoreEventPump,
  meshCorePromptPolicy,
  nativeRadioReply,
  normalizeMeshCoreTarget,
  privateChannelInvocation,
  privateChannelRadioReply,
  singleRadioReply,
  type MeshCoreEvent,
} from "./channel.js";
import type { JsonValue } from "./client.js";

class FakeClient {
  readonly puts: Array<{ path: string; body: JsonValue }> = [];
  constructor(
    private readonly cursor: number,
    private readonly head: number,
    private readonly events: MeshCoreEvent[],
  ) {}

  async get(path: string): Promise<JsonValue> {
    if (path === "/v1/status") return { last_event_id: this.head };
    if (path.endsWith("/cursor")) {
      return { consumer_id: "native-channel", event_id: this.cursor };
    }
    if (path.includes("/events")) return this.events as unknown as JsonValue;
    throw new Error(`unexpected GET ${path}`);
  }

  async put(path: string, body: JsonValue): Promise<JsonValue> {
    this.puts.push({ path, body });
    const object = body as { event_id: number };
    return { consumer_id: "native-channel", event_id: object.event_id };
  }
}

describe("MeshCore native channel", () => {
  it("uses a short agent target below the firmware byte ceiling", () => {
    expect(meshCoreAgentTargetBytes).toBe(75);
    expect(meshCoreHardTextBytes).toBe(160);
    expect(meshCoreAgentGuidance).toContain("fewest words");
    expect(meshCoreAgentGuidance).toContain("75 UTF-8 bytes");
    expect(meshCoreAgentGuidance).toContain("automatic-resend");
    expect(meshCoreAgentSystemPrompt).toContain("Output only the user-visible RF reply");
  });

  it("injects system guidance and an empty tool surface only for MeshCore turns", () => {
    expect(meshCorePromptPolicy({ channel: "meshcore" })).toEqual({
      appendSystemContext: meshCoreAgentSystemPrompt,
      toolsAllow: [],
    });
    expect(meshCorePromptPolicy({ messageProvider: "meshcore" })).toEqual({
      appendSystemContext: meshCoreAgentSystemPrompt,
      toolsAllow: [],
    });
    expect(meshCorePromptPolicy({ channel: "telegram" })).toBeUndefined();
  });

  it("enforces the hard limit in UTF-8 bytes without splitting code points", () => {
    const reply = singleRadioReply("🙂".repeat(60));
    expect(new TextEncoder().encode(reply).length).toBeLessThanOrEqual(160);
    expect(reply.endsWith("…")).toBe(true);
    expect(reply).not.toContain("�");
  });

  it("enforces the 75-byte native reply budget in code", () => {
    expect(nativeRadioReply("MeshPincer OK")).toBe("MeshPincer OK");
    expect(nativeRadioReply("x".repeat(76))).toBeUndefined();
    expect(nativeRadioReply("🙂".repeat(19))).toBeUndefined();
  });

  it("runs one local compression repair and validates its UTF-8 bytes", async () => {
    const repair = vi.fn(async () => "Short answer.");
    await expect(
      constrainedRadioReply("x".repeat(76), { repair }),
    ).resolves.toBe("Short answer.");
    expect(repair).toHaveBeenCalledOnce();
    expect(repair).toHaveBeenCalledWith("x".repeat(76), meshCoreAgentTargetBytes);

    await expect(
      constrainedRadioReply("x".repeat(76), {
        repair: async () => "🙂".repeat(19),
      }),
    ).resolves.toBeUndefined();
  });

  it("bypasses generation for exact-reply requests", () => {
    expect(exactReplyRequest("Reply exactly: MeshPincer OK")).toBe("MeshPincer OK");
    expect(exactReplyRequest(" reply exactly:   OK  ")).toBe("OK");
    expect(exactReplyRequest("reply ok")).toBeUndefined();
  });

  it("suppresses OpenClaw restart-recovery commentary on RF", () => {
    expect(
      nativeRadioReply(
        "I couldn’t confirm whether my previous reply reached this chat, so I won’t resend it automatically. Please ask for any missing remainder.",
      ),
    ).toBeUndefined();
  });

  it("does not repair a suppressed restart-recovery notice", async () => {
    const repair = vi.fn(async () => "Recovered");
    await expect(
      constrainedRadioReply(
        "I couldn’t confirm whether my previous reply reached this chat, so I won’t resend it automatically.",
        { repair },
      ),
    ).resolves.toBeUndefined();
    expect(repair).not.toHaveBeenCalled();
  });

  it("keeps uncertain native replies local instead of triggering RF recovery text", () => {
    expect(acceptsDirectDeliveryState("timed_out", false)).toBe(true);
    expect(acceptsDirectDeliveryState("timed_out", true)).toBe(false);
    expect(acceptsDirectDeliveryState("failed", false)).toBe(false);
  });

  it("normalizes direct-message targets by public key", () => {
    const key = "AB".repeat(32);
    expect(normalizeMeshCoreTarget(key)).toBe(key.toLowerCase());
    expect(normalizeMeshCoreTarget(`meshcore:${key}`)).toBe(key.toLowerCase());
    expect(normalizeMeshCoreTarget("channel:0")).toBeUndefined();
    expect(normalizeMeshCoreTarget("channel:3")).toBe("channel:3");
    expect(normalizeMeshCoreTarget("meshcore:channel:3")).toBe("channel:3");
  });

  it("requires an explicit runtime-name mention for private-channel turns", () => {
    expect(privateChannelInvocation("NVM-P: hello", "Fidget")).toBeUndefined();
    expect(privateChannelInvocation("NVM-P: @Fidget status?", "Fidget")).toBe(
      "status?",
    );
    expect(
      privateChannelInvocation("NVM-P: @[Fidget] status?", "Fidget"),
    ).toBe("status?");
    expect(privateChannelInvocation("@runtime-node: ping", "runtime-node")).toBe(
      "ping",
    );
  });

  it("counts the channel sender label inside the 75-byte RF budget", async () => {
    await expect(privateChannelRadioReply("Fidget", "Private OK")).resolves.toBe(
      "Fidget: Private OK",
    );
    const oversized = await privateChannelRadioReply(
      "Fidget",
      "x".repeat(75),
      async () => "Brief.",
    );
    expect(oversized).toBe("Fidget: Brief.");
    expect(new TextEncoder().encode(oversized ?? "").length).toBeLessThanOrEqual(
      meshCoreAgentTargetBytes,
    );
  });

  it("suppresses a private-channel reply when its single repair still exceeds budget", async () => {
    await expect(
      privateChannelRadioReply("Fidget", "x".repeat(75), async () => "y".repeat(75)),
    ).resolves.toBeUndefined();
  });

  it("baselines a new consumer at the current event head", async () => {
    const client = new FakeClient(0, 257, []);
    const processEvent = vi.fn();
    const pump = new MeshCoreEventPump({
      client,
      consumerId: "native-channel",
      startAtLatest: true,
      pollIntervalMs: 1_000,
      processEvent,
    });

    expect(await pump.initialize()).toBe(257);
    expect(client.puts).toEqual([
      {
        path: "/v1/consumers/native-channel/cursor",
        body: { event_id: 257 },
      },
    ]);
    expect(processEvent).not.toHaveBeenCalled();
  });

  it("advances only after an event is processed successfully", async () => {
    const event: MeshCoreEvent = {
      id: 258,
      kind: "message.received",
      payload: { direction: "inbound", kind: "direct", text: "hello" },
    };
    const client = new FakeClient(257, 258, [event]);
    const processEvent = vi.fn(async () => undefined);
    const pump = new MeshCoreEventPump({
      client,
      consumerId: "native-channel",
      startAtLatest: false,
      pollIntervalMs: 1_000,
      processEvent,
    });

    expect(await pump.pumpOnce()).toBe(1);
    expect(processEvent).toHaveBeenCalledWith(event);
    expect(client.puts.at(-1)).toEqual({
      path: "/v1/consumers/native-channel/cursor",
      body: { event_id: 258 },
    });
  });

  it("leaves the cursor unchanged when dispatch fails", async () => {
    const event: MeshCoreEvent = {
      id: 258,
      kind: "message.received",
      payload: { direction: "inbound", kind: "direct", text: "hello" },
    };
    const client = new FakeClient(257, 258, [event]);
    const pump = new MeshCoreEventPump({
      client,
      consumerId: "native-channel",
      startAtLatest: false,
      pollIntervalMs: 1_000,
      processEvent: async () => {
        throw new Error("dispatch failed");
      },
    });

    await expect(pump.pumpOnce()).rejects.toThrow("dispatch failed");
    expect(client.puts).toEqual([]);
  });
});
