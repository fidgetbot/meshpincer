import { createServer, type Server } from "node:http";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshPincerClient } from "./client.js";
import { queryMessages } from "./index.js";

let server: Server | undefined;

afterEach(async () => {
  if (server) {
    await new Promise<void>((resolve, reject) =>
      server?.close((error) => (error ? reject(error) : resolve())),
    );
    server = undefined;
  }
});

describe("MeshPincerClient", () => {
  it("reads replay events and advances a consumer cursor over a Unix socket", async () => {
    const directory = await mkdtemp(join(tmpdir(), "meshpincer-client-"));
    const socketPath = join(directory, "daemon.sock");
    server = createServer((request, response) => {
      if (request.method === "GET") {
        response.writeHead(200, { "content-type": "application/json" });
        response.end('[{"id":7,"kind":"message.received"}]');
        return;
      }
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            consumer_id: "native-channel",
            event_id: JSON.parse(Buffer.concat(chunks).toString("utf8")).event_id,
          }),
        );
      });
    });
    await new Promise<void>((resolve, reject) => {
      server?.once("error", reject);
      server?.listen(socketPath, resolve);
    });

    const client = new MeshPincerClient(socketPath);
    expect(await client.get("/v1/consumers/native-channel/events")).toEqual([
      { id: 7, kind: "message.received" },
    ]);
    expect(
      await client.put("/v1/consumers/native-channel/cursor", { event_id: 7 }),
    ).toEqual({ consumer_id: "native-channel", event_id: 7 });
  });

  it("reads an operator inbox without advancing its cursor implicitly", async () => {
    const directory = await mkdtemp(join(tmpdir(), "meshpincer-inbox-"));
    const socketPath = join(directory, "daemon.sock");
    const requests: Array<{ method: string; url: string; body: string }> = [];
    server = createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf8");
        requests.push({ method: request.method ?? "", url: request.url ?? "", body });
        response.writeHead(200, { "content-type": "application/json" });
        if (request.url?.endsWith("/cursor") && request.method === "GET") {
          response.end('{"consumer_id":"operator-tools","event_id":4}');
        } else if (request.url?.includes("/events")) {
          response.end('[{"id":5,"kind":"message.received"}]');
        } else {
          response.end('{"consumer_id":"operator-tools","event_id":4}');
        }
      });
    });
    await new Promise<void>((resolve, reject) => {
      server?.once("error", reject);
      server?.listen(socketPath, resolve);
    });

    const result = await queryMessages(
      new MeshPincerClient(socketPath),
      "operator-tools",
      { mode: "unread", afterId: 0, limit: 20 },
    );

    expect(result).toEqual({
      mode: "unread",
      consumer_id: "operator-tools",
      cursor: { consumer_id: "operator-tools", event_id: 4 },
      events: [{ id: 5, kind: "message.received" }],
      acknowledgement_required: true,
    });
    expect(requests.every((request) => request.method === "GET")).toBe(true);
  });

  it("acknowledges a prior inbox batch before returning the next batch", async () => {
    const directory = await mkdtemp(join(tmpdir(), "meshpincer-inbox-ack-"));
    const socketPath = join(directory, "daemon.sock");
    const requests: Array<{ method: string; url: string; body: string }> = [];
    server = createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf8");
        requests.push({ method: request.method ?? "", url: request.url ?? "", body });
        response.writeHead(200, { "content-type": "application/json" });
        if (request.url?.includes("/events")) {
          response.end("[]");
        } else {
          response.end('{"consumer_id":"operator-tools","event_id":5}');
        }
      });
    });
    await new Promise<void>((resolve, reject) => {
      server?.once("error", reject);
      server?.listen(socketPath, resolve);
    });

    await queryMessages(new MeshPincerClient(socketPath), "operator-tools", {
      mode: "unread",
      afterId: 0,
      limit: 20,
      acknowledgeThroughEventId: 5,
    });

    expect(requests[0]).toEqual({
      method: "PUT",
      url: "/v1/consumers/operator-tools/cursor",
      body: '{"event_id":5}',
    });
  });
});
