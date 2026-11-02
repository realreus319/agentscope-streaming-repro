package repro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Deterministic stub OpenAI-compatible SSE server.
 *
 * <p>It answers every {@code POST /v1/chat/completions} with a fixed tool-call chain driven by a
 * {@code state} field carried in the request body:</p>
 *
 * <ul>
 *   <li>state 0..4: emit a {@code get_weather} tool call, then state+1</li>
 *   <li>state 5..8: emit a {@code get_time} tool call, then state+1</li>
 *   <li>state 9: emit a final text answer (no tool calls)</li>
 * </ul>
 *
 * <p>Token emission is controlled by {@code tokensPerCall} / {@code tokenIntervalMs} /
 * {@code delivery}: {@code metronome} flushes one token per interval, {@code batch100} flushes
 * 100 tokens per interval. The client is expected to echo the next state back; if no state is
 * present, state 0 is assumed.</p>
 */
public class StubLlmServer {

    private final HttpServer server;
    private final ObjectMapper mapper = new ObjectMapper();
    private final int tokensPerCall;
    private final int tokenIntervalMs;
    private final String delivery;

    public StubLlmServer(int port, int tokensPerCall, int tokenIntervalMs, String delivery) throws IOException {
        this.tokensPerCall = tokensPerCall;
        this.tokenIntervalMs = tokenIntervalMs;
        this.delivery = delivery;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/v1/chat/completions", this::handle);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public void stop() {
        server.stop(0);
    }

    private void handle(HttpExchange ex) throws IOException {
        try {
            byte[] body = ex.getRequestBody().readAllBytes();
            JsonNode req = mapper.readTree(body);
            int state = 0;
            // The driver encodes the next state in a marker message; fall back to scanning
            // tool messages for the last tool name if the marker is absent.
            if (req.has("state")) {
                state = req.get("state").asInt();
            } else if (req.has("messages")) {
                for (JsonNode m : req.get("messages")) {
                    if (m.has("role") && "tool".equals(m.get("role").asText()) && m.has("name")) {
                        String n = m.get("name").asText();
                        if ("get_weather".equals(n)) {
                            state = Math.max(state, 1);
                        }
                    }
                }
                // Count tool messages to approximate state when marker is missing.
                int toolMsgs = 0;
                for (JsonNode m : req.get("messages")) {
                    if (m.has("role") && "tool".equals(m.get("role").asText())) {
                        toolMsgs++;
                    }
                }
                state = Math.max(state, toolMsgs);
            }
            boolean stream = req.has("stream") && req.get("stream").asBoolean();
            if (!stream) {
                sendNonStreaming(ex, state);
                return;
            }
            ex.getResponseHeaders().set("Content-Type", "text/event-stream");
            ex.getResponseHeaders().set("Cache-Control", "no-cache");
            ex.getResponseHeaders().set("Connection", "keep-alive");
            ex.sendResponseHeaders(200, 0);
            try (OutputStream out = ex.getResponseBody()) {
                if (state < 9) {
                    String tool = state < 5 ? "get_weather" : "get_time";
                    String args = state < 5
                            ? "{\"city\": \"Paris\"}"
                            : "{\"timezone\": \"Europe/Paris\"}";
                    String id = "chatcmpl-stub-" + state;
                    // First chunk: role + tool-call header.
                    ObjectNode chunk0 = mapper.createObjectNode();
                    chunk0.put("id", id);
                    chunk0.put("object", "chat.completion.chunk");
                    chunk0.put("created", System.currentTimeMillis() / 1000);
                    chunk0.put("model", "stub");
                    ArrayNode choices0 = chunk0.putArray("choices");
                    ObjectNode c0 = choices0.addObject();
                    c0.put("index", 0);
                    ObjectNode delta0 = c0.putObject("delta");
                    delta0.put("role", "assistant");
                    delta0.put("content", (String) null);
                    ArrayNode tc = delta0.putArray("tool_calls");
                    ObjectNode t = tc.addObject();
                    t.put("index", 0);
                    t.put("id", "call_" + state);
                    t.put("type", "function");
                    ObjectNode fn = t.putObject("function");
                    fn.put("name", tool);
                    fn.put("arguments", "");
                    c0.put("finish_reason", (String) null);
                    writeSse(out, chunk0.toString());
                    // Argument tokens, emitted at the configured rate.
                    emitTokens(out, id, args, tool);
                    // Final chunk with finish_reason=tool_calls.
                    ObjectNode fin = mapper.createObjectNode();
                    fin.put("id", id);
                    fin.put("object", "chat.completion.chunk");
                    fin.put("created", System.currentTimeMillis() / 1000);
                    fin.put("model", "stub");
                    ArrayNode choicesF = fin.putArray("choices");
                    ObjectNode cf = choicesF.addObject();
                    cf.put("index", 0);
                    cf.putObject("delta");
                    cf.put("finish_reason", "tool_calls");
                    writeSse(out, fin.toString());
                } else {
                    String id = "chatcmpl-stub-final";
                    String answer = "Done. Weather and time retrieved.";
                    emitTokens(out, id, answer, null);
                    ObjectNode fin = mapper.createObjectNode();
                    fin.put("id", id);
                    fin.put("object", "chat.completion.chunk");
                    fin.put("created", System.currentTimeMillis() / 1000);
                    fin.put("model", "stub");
                    ArrayNode choicesF = fin.putArray("choices");
                    ObjectNode cf = choicesF.addObject();
                    cf.put("index", 0);
                    cf.putObject("delta");
                    cf.put("finish_reason", "stop");
                    writeSse(out, fin.toString());
                }
                out.write("[DONE]\n\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                ex.sendResponseHeaders(500, -1);
            } catch (Exception ignored) {
            }
        } finally {
            ex.close();
        }
    }

    private void emitTokens(OutputStream out, String id, String text, String tool) throws Exception {
        // Pad / truncate the payload to exactly tokensPerCall "tokens" (whitespace-separated
        // words). For tool arguments the JSON is kept intact and padded with spaces so the
        // client still parses it; for the final answer plain words are used.
        String payload = text;
        if (tool != null) {
            // Keep the JSON arguments, but count tokens as words; pad with spaces to reach
            // the token budget without breaking JSON parsing (trailing spaces are ignored).
            int words = text.trim().isEmpty() ? 0 : text.trim().split("\\s+").length;
            int pad = Math.max(0, tokensPerCall - words);
            payload = text + " ".repeat(pad);
        } else {
            String[] words = new String[tokensPerCall];
            java.util.Arrays.fill(words, "tok");
            payload = String.join(" ", words);
        }
        if ("batch100".equals(delivery)) {
            int batch = 100;
            for (int i = 0; i < payload.length(); i += batch) {
                String part = payload.substring(i, Math.min(payload.length(), i + batch));
                writeContentChunk(out, id, part);
                if (i + batch < payload.length()) {
                    Thread.sleep(tokenIntervalMs);
                }
            }
        } else {
            // metronome: one token (word + space) per interval; for tool JSON this splits
            // the argument string across chunks, which the client reassembles.
            String[] toks = payload.split(" ", -1);
            for (int i = 0; i < toks.length; i++) {
                String part = toks[i] + (i + 1 < toks.length ? " " : "");
                if (part.isEmpty()) {
                    continue;
                }
                writeContentChunk(out, id, part);
                if (i + 1 < toks.length) {
                    Thread.sleep(tokenIntervalMs);
                }
            }
        }
    }

    private void writeContentChunk(OutputStream out, String id, String content) throws IOException {
        ObjectNode chunk = mapper.createObjectNode();
        chunk.put("id", id);
        chunk.put("object", "chat.completion.chunk");
        chunk.put("created", System.currentTimeMillis() / 1000);
        chunk.put("model", "stub");
        ArrayNode choices = chunk.putArray("choices");
        ObjectNode c = choices.addObject();
        c.put("index", 0);
        ObjectNode delta = c.putObject("delta");
        delta.put("role", (String) null);
        delta.put("content", content);
        c.put("finish_reason", (String) null);
        writeSse(out, chunk.toString());
    }

    private void writeSse(OutputStream out, String json) throws IOException {
        out.write(("data: " + json + "\n\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private void sendNonStreaming(HttpExchange ex, int state) throws IOException {
        ObjectNode resp = mapper.createObjectNode();
        resp.put("id", "chatcmpl-stub-" + state);
        resp.put("object", "chat.completion");
        resp.put("created", System.currentTimeMillis() / 1000);
        resp.put("model", "stub");
        ArrayNode choices = resp.putArray("choices");
        ObjectNode c = choices.addObject();
        c.put("index", 0);
        ObjectNode msg = c.putObject("message");
        msg.put("role", "assistant");
        if (state < 9) {
            String tool = state < 5 ? "get_weather" : "get_time";
            String args = state < 5 ? "{\"city\": \"Paris\"}" : "{\"timezone\": \"Europe/Paris\"}";
            msg.put("content", (String) null);
            ArrayNode tc = msg.putArray("tool_calls");
            ObjectNode t = tc.addObject();
            t.put("id", "call_" + state);
            t.put("type", "function");
            ObjectNode fn = t.putObject("function");
            fn.put("name", tool);
            fn.put("arguments", args);
            c.put("finish_reason", "tool_calls");
        } else {
            msg.put("content", "Done. Weather and time retrieved.");
            c.put("finish_reason", "stop");
        }
        ObjectNode usage = resp.putObject("usage");
        usage.put("prompt_tokens", 10);
        usage.put("completion_tokens", tokensPerCall);
        usage.put("total_tokens", 10 + tokensPerCall);
        byte[] bytes = mapper.writeValueAsBytes(resp);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }
}
