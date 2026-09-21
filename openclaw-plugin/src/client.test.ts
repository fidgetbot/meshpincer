import { createServer, type Server } from "node:http";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshPincerClient } from "./client.js";

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
});
