import { homedir } from "node:os";
import { join } from "node:path";
import { request } from "node:http";

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export const defaultSocketPath = join(
  homedir(),
  ".openclaw",
  "state",
  "meshpincer",
  "meshpincer.sock",
);

export class MeshPincerClient {
  constructor(private readonly socketPath: string = defaultSocketPath) {}

  get(path: string): Promise<JsonValue> {
    return this.requestJson("GET", path);
  }

  post(path: string, body: JsonValue): Promise<JsonValue> {
    return this.requestJson("POST", path, body);
  }

  patch(path: string, body: JsonValue): Promise<JsonValue> {
    return this.requestJson("PATCH", path, body);
  }

  put(path: string, body: JsonValue): Promise<JsonValue> {
    return this.requestJson("PUT", path, body);
  }

  private requestJson(method: string, path: string, body?: JsonValue): Promise<JsonValue> {
    const payload = body === undefined ? undefined : JSON.stringify(body);

    return new Promise((resolve, reject) => {
      const req = request(
        {
          socketPath: this.socketPath,
          path,
          method,
          headers: payload
            ? {
                "content-type": "application/json",
                "content-length": Buffer.byteLength(payload),
              }
            : undefined,
        },
        (response) => {
          const chunks: Buffer[] = [];
          response.on("data", (chunk: Buffer) => chunks.push(chunk));
          response.on("end", () => {
            const text = Buffer.concat(chunks).toString("utf8");
            let parsed: JsonValue = null;
            if (text) {
              try {
                parsed = JSON.parse(text) as JsonValue;
              } catch {
                reject(new Error(`MeshPincer returned invalid JSON: ${text}`));
                return;
              }
            }

            const statusCode = response.statusCode ?? 500;
            if (statusCode < 200 || statusCode >= 300) {
              reject(new Error(`MeshPincer request failed (${statusCode}): ${text}`));
              return;
            }
            resolve(parsed);
          });
        },
      );

      req.on("error", (error) => {
        reject(
          new Error(
            `Cannot reach MeshPincer at ${this.socketPath}: ${error.message}`,
            { cause: error },
          ),
        );
      });
      if (payload) req.write(payload);
      req.end();
    });
  }
}
