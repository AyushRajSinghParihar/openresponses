import { afterEach, describe, expect, test } from "bun:test";
import { responseFixture } from "./compliance-test-fixtures";
import {
  runAllTests,
  testTemplates,
  type TestConfig,
  type TestResult,
  type TestTemplate,
} from "./compliance-tests";

const servers: ReturnType<typeof Bun.serve>[] = [];
afterEach(() => {
  for (const server of servers.splice(0)) server.stop(true);
});

function config(server: ReturnType<typeof Bun.serve>): TestConfig {
  servers.push(server);
  return {
    baseUrl: server.url.toString().replace(/\/$/, ""),
    apiKey: "test-key",
    authHeaderName: "Authorization",
    useBearerPrefix: true,
    model: "test-model",
    runtime: "server",
    timeoutMs: 100,
  };
}

const httpTemplate: TestTemplate = {
  id: "http",
  name: "HTTP request",
  description: "Fixture HTTP request",
  getRequest: () => ({ model: "test-model", input: "Hello" }),
};

function template(id: string) {
  return testTemplates.find((value) => value.id === id)!;
}

function terminalEvent() {
  return JSON.stringify({
    type: "response.completed",
    sequence_number: 0,
    response: responseFixture,
  });
}

describe("HTTP request deadlines", () => {
  test("a stalled response header fails without blocking another test or terminal progress", async () => {
    let requests = 0;
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      fetch(request) {
        requests++;
        return new URL(request.url).pathname === "/stall"
          ? new Promise<Response>(() => {})
          : Response.json(responseFixture);
      },
    });
    const progress: TestResult[] = [];
    const results = await runAllTests(
      config(server),
      (result) => progress.push(result),
      [{ ...httpTemplate, id: "stalled", endpoint: "/stall" }, httpTemplate],
    );
    expect(requests).toBe(2);
    expect(results.map((result) => result.status)).toEqual([
      "failed",
      "passed",
    ]);
    expect(results[0].errors?.length).toBeGreaterThan(0);
    expect(progress.filter((result) => result.status !== "running")).toEqual([
      results[1],
      results[0],
    ]);
  }, 2000);

  test.each(["application/json", "text/plain"])(
    "a stalled %s response body is aborted",
    async (contentType) => {
      let cancel!: () => void;
      const cancelled = new Promise<void>((resolve) => {
        cancel = resolve;
      });
      const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        idleTimeout: 0,
        fetch() {
          return new Response(
            new ReadableStream({
              start(controller) {
                controller.enqueue(new TextEncoder().encode("{"));
              },
              cancel,
            }),
            { headers: { "Content-Type": contentType } },
          );
        },
      });
      const [result] = await runAllTests(config(server), () => {}, [
        httpTemplate,
      ]);
      expect(result.status).toBe("failed");
      expect(result.errors?.length).toBeGreaterThan(0);
      await cancelled;
    },
    2000,
  );

  test("a terminal SSE response without DONE cannot hang or pass", async () => {
    let cancel!: () => void;
    const cancelled = new Promise<void>((resolve) => {
      cancel = resolve;
    });
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      fetch() {
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode(
                  `event: response.completed\ndata: ${terminalEvent()}\n\n`,
                ),
              );
            },
            cancel,
          }),
          { headers: { "Content-Type": "text/event-stream" } },
        );
      },
    });
    const [result] = await runAllTests(config(server), () => {}, [
      template("streaming-response"),
    ]);
    expect(result.status).toBe("failed");
    expect(result.errors?.length).toBeGreaterThan(0);
    await cancelled;
  }, 2000);

  test("the WebSocket compaction preflight also bounds body consumption", async () => {
    let path: string | undefined;
    let cancel!: () => void;
    const cancelled = new Promise<void>((resolve) => {
      cancel = resolve;
    });
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      fetch(request) {
        path = new URL(request.url).pathname;
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode('{"output":['));
            },
            cancel,
          }),
          { headers: { "Content-Type": "application/json" } },
        );
      },
    });
    const [result] = await runAllTests(config(server), () => {}, [
      template("websocket-compact-new-chain"),
    ]);
    expect(path).toBe("/responses/compact");
    expect(result.status).toBe("failed");
    expect(result.errors?.length).toBeGreaterThan(0);
    await cancelled;
  }, 2000);
});

describe("WebSocket request deadlines and cleanup", () => {
  test("a stalled handshake fails", async () => {
    let requests = 0;
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      fetch() {
        requests++;
        return new Promise<Response>(() => {});
      },
    });
    const [result] = await runAllTests(config(server), () => {}, [
      template("websocket-response"),
    ]);
    expect(requests).toBe(1);
    expect(result.status).toBe("failed");
    expect(result.errors?.length).toBeGreaterThan(0);
  }, 2000);

  test.each(["stall", "disconnect", "complete"] as const)(
    "handles %s during the second turn and closes the socket",
    async (behavior) => {
      let messages = 0;
      let close!: () => void;
      const closed = new Promise<void>((resolve) => {
        close = resolve;
      });
      const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        idleTimeout: 0,
        fetch(request, server) {
          if (server.upgrade(request)) return;
          return new Response(null, { status: 400 });
        },
        websocket: {
          message(ws) {
            messages++;
            if (messages === 1 || behavior === "complete")
              ws.send(terminalEvent());
            else if (behavior === "disconnect") ws.close();
          },
          close,
        },
      });
      const [result] = await runAllTests(config(server), () => {}, [
        template("websocket-sequential-responses"),
      ]);
      expect(messages).toBe(2);
      expect(result.status).toBe(behavior === "complete" ? "passed" : "failed");
      if (behavior !== "complete")
        expect(result.errors?.length).toBeGreaterThan(0);
      await closed;
    },
    2000,
  );
});

test("template setup and custom runner failures each report a terminal result", async () => {
  const failures: TestTemplate[] = [
    {
      ...httpTemplate,
      id: "setup",
      getRequest() {
        throw new Error("setup failed");
      },
    },
    {
      ...httpTemplate,
      id: "run",
      async run() {
        throw new Error("runner failed");
      },
    },
    {
      ...httpTemplate,
      id: "support",
      unsupportedReason() {
        throw new Error("support check failed");
      },
    },
  ];
  const progress: TestResult[] = [];
  const results = await runAllTests(
    {
      baseUrl: "http://127.0.0.1:1",
      apiKey: "",
      authHeaderName: "Authorization",
      useBearerPrefix: true,
      model: "test-model",
    },
    (result) => progress.push(result),
    failures,
  );
  expect(results.map((result) => result.status)).toEqual([
    "failed",
    "failed",
    "failed",
  ]);
  for (const result of results) {
    expect(result.errors?.length).toBeGreaterThan(0);
    expect(
      progress.filter(
        (entry) => entry.id === result.id && entry.status === "failed",
      ),
    ).toEqual([result]);
  }
});
