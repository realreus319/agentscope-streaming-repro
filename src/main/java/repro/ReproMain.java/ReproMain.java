package repro;

import io.agentscope.core.agent.ReActAgent;
import io.agentscope.core.message.TextMessage;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatModel;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.OpenAIModel;
import io.agentscope.core.model.OpenAIModelConfig;
import io.agentscope.core.stream.AgentStream;
import io.agentscope.core.stream.StreamEvent;
import io.agentscope.core.stream.StreamEventType;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.core.tool.ToolkitConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Driver: run N concurrent {@link ReActAgent} chains against the stub server through
 * {@code streamEvents} and report latency / effective tokens/sec.
 *
 * <p>Each chain is expected to make exactly 10 model calls and 9 tool executions. The state
 * machine is driven by counting tool executions in the stream: after each tool-result event the
 * driver restarts the agent with the next state encoded in a marker message so the stub can
 * advance. In practice the stub also counts tool messages, so the marker is belt-and-braces.</p>
 *
 * <p>Usage: {@code ReproMain [--chains N] [--tokens N] [--interval MS] [--delivery metronome|batch100] [--port P]}</p>
 */
public class ReproMain {

    public static void main(String[] args) throws Exception {
        int chains = 5;
        int tokens = 300;
        int interval = 20;
        String delivery = "metronome";
        int port = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--chains" -> chains = Integer.parseInt(args[++i]);
                case "--tokens" -> tokens = Integer.parseInt(args[++i]);
                case "--interval" -> interval = Integer.parseInt(args[++i]);
                case "--delivery" -> delivery = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                default -> System.err.println("unknown arg: " + args[i]);
            }
        }
        StubLlmServer stub = new StubLlmServer(port, tokens, interval, delivery);
        String baseUrl = "http://127.0.0.1:" + stub.port() + "/v1";
        System.out.println("repro: baseUrl=" + baseUrl
                + " cores=" + Runtime.getRuntime().availableProcessors()
                + " chains=" + chains
                + " delivery=" + delivery
                + " tokensPerCall=" + tokens
                + " tokenIntervalMs=" + interval
                + " expectedPerChain: modelCalls=10 toolExecutions=9");
        try {
            run(baseUrl, chains, tokens);
        } finally {
            stub.stop();
        }
    }

    private static void run(String baseUrl, int chains, int tokensPerCall) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(chains, 512));
        CountDownLatch latch = new CountDownLatch(chains);
        List<Long> durationsMs = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger toolInvocations = new AtomicInteger();
        ConcurrentHashMap<Integer, Integer> modelCallsPerChain = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Integer> toolExecPerChain = new ConcurrentHashMap<>();
        long wallStart = System.nanoTime();
        for (int c = 0; c < chains; c++) {
            int chainId = c;
            pool.submit(() -> {
                long start = System.nanoTime();
                try {
                    runChain(baseUrl, chainId, modelCallsPerChain, toolExecPerChain, toolInvocations);
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("chain " + chainId + " failed: " + e);
                    e.printStackTrace(System.err);
                } finally {
                    durationsMs.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                    latch.countDown();
                }
            });
        }
        latch.await();
        long wallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart);
        pool.shutdown();
        List<Long> sorted = new ArrayList<>(durationsMs);
        sorted.sort(Long::compareTo);
        double mean = sorted.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = sorted.get((int) (sorted.size() * 0.50));
        long p95 = sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * 0.95)));
        long min = sorted.get(0);
        long max = sorted.get(sorted.size() - 1);
        double effTokPerSec = (10.0 * tokensPerCall) / (mean / 1000.0);
        int expectedTools = chains * 9;
        boolean countOk = modelCallsPerChain.values().stream().allMatch(v -> v == 10)
                && toolExecPerChain.values().stream().allMatch(v -> v == 9)
                && modelCallsPerChain.size() == chains
                && toolExecPerChain.size() == chains;
        System.out.println("=== repro results ===");
        System.out.printf("chains=%d completed=%d errors=%d wall=%.2fs%n", chains, chains - errors.get(), errors.get(), wallMs / 1000.0);
        System.out.printf("chain seconds: mean=%.2f p50=%.2f p95=%.2f min=%.2f max=%.2f%n",
                mean / 1000.0, p50 / 1000.0, p95 / 1000.0, min / 1000.0, max / 1000.0);
        double nominal = 1000.0 / 20.0; // placeholder, replaced below when interval known
        System.out.printf("effective tok/s per chain: %.2f (nominal generation rate %s)%n", effTokPerSec, "see args");
        System.out.printf("tool invocations (global counter): %d (expected %d)%n", toolInvocations.get(), expectedTools);
        System.out.println("count assertion: " + (countOk ? "PASS" : "FAIL")
                + " (every chain must show exactly 10 model calls / 9 tool executions)");
        if (!countOk) {
            System.out.println("modelCallsPerChain sample: " + modelCallsPerChain);
            System.out.println("toolExecPerChain sample: " + toolExecPerChain);
        }
        // Keep the nominal-rate line simple: recompute from the stub schedule is not
        // possible here, so the log header already records tokens/interval.
        @SuppressWarnings("unused")
        double unusedNominal = nominal;
    }

    private static void runChain(String baseUrl, int chainId,
                                 ConcurrentHashMap<Integer, Integer> modelCallsPerChain,
                                 ConcurrentHashMap<Integer, Integer> toolExecPerChain,
                                 AtomicInteger toolInvocations) throws Exception {
        ChatModel model = new OpenAIModel(
                OpenAIModelConfig.builder()
                        .modelName("stub")
                        .apiKey("stub")
                        .baseUrl(baseUrl)
                        .stream(true)
                        .build());
        Toolkit toolkit = new Toolkit(ToolkitConfig.builder().build());
        toolkit.registerTool(new ReproTools());
        ReActAgent agent = new ReActAgent(
                io.agentscope.core.agent.AgentConfig.builder()
                        .name("repro-agent-" + chainId)
                        .sysPrompt("You are a helpful assistant. Use tools when needed.")
                        .model(model)
                        .toolkit(toolkit)
                        .build());
        int modelCalls = 0;
        int toolExecutions = 0;
        int state = 0;
        // Loop over agent turns: each streamEvents run ends when the agent either calls a
        // tool (we execute it via the toolkit through the next run) or finishes. The stub
        // advances state by counting tool messages, so we simply re-run the agent with the
        // conversation history until it produces a final answer.
        List<Object> history = new ArrayList<>();
        history.add(new TextMessage("user", "What is the weather in Paris and what time is it there?"));
        for (int turn = 0; turn < 20; turn++) {
            AgentStream stream = agent.streamEvents(history);
            boolean sawToolCall = false;
            String toolName = null;
            String toolArgs = null;
            String toolCallId = null;
            try {
                while (true) {
                    StreamEvent ev = stream.next();
                    if (ev == null) {
                        break;
                    }
                    if (ev.type() == StreamEventType.MODEL_RESPONSE) {
                        // Model chunks arrive here; count a model call on the first chunk
                        // of each turn is approximated by counting the turn itself below.
                    } else if (ev.type() == StreamEventType.TOOL_CALL) {
                        sawToolCall = true;
                        if (ev.data() instanceof ToolUseBlock b) {
                            toolName = b.name();
                            toolArgs = b.input() == null ? "" : b.input().toString();
                            toolCallId = b.id();
                        } else if (ev.data() instanceof Map<?, ?> m) {
                            toolName = String.valueOf(m.getOrDefault("name", ""));
                            toolArgs = String.valueOf(m.getOrDefault("arguments", ""));
                            toolCallId = String.valueOf(m.getOrDefault("id", "call"));
                        }
                    } else if (ev.type() == StreamEventType.TOOL_RESULT) {
                        toolExecutions++;
                        toolInvocations.incrementAndGet();
                    } else if (ev.type() == StreamEventType.AGENT_END) {
                        break;
                    }
                }
            } catch (Exception e) {
                // streamEvents implementations differ in terminal signalling; treat
                // exhaustion as end-of-turn and continue.
                if (!(e instanceof java.util.NoSuchElementException)) {
                    throw e;
                }
            }
            modelCalls++;
            if (!sawToolCall) {
                break;
            }
            // Execute the tool locally and append the result so the next turn advances.
            Object result = executeTool(toolName, toolArgs);
            history.add(new TextMessage("tool", toolCallId + ":" + toolName + ":" + result));
            state++;
            // Marker message carrying the next state for the stub (ignored by the agent
            // prompt in practice; the stub reads it from the request body if present).
            history.add(new TextMessage("user", "[state=" + state + "] continue"));
        }
        modelCallsPerChain.put(chainId, modelCalls);
        toolExecPerChain.put(chainId, toolExecutions);
        if (modelCalls != 10 || toolExecutions != 9) {
            System.err.println("chain " + chainId + " unexpected counts: modelCalls=" + modelCalls
                    + " toolExecutions=" + toolExecutions);
        }
    }

    private static Object executeTool(String name, String args) {
        ReproTools tools = new ReproTools();
        try {
            if ("get_weather".equals(name)) {
                return tools.getWeather("Paris");
            } else if ("get_time".equals(name)) {
                return tools.getTime("Europe/Paris");
            }
        } catch (Exception e) {
            return "error: " + e.getMessage();
        }
        return "unknown tool: " + name;
    }

    // Unused helper kept to document the expected final-answer shape.
    @SuppressWarnings("unused")
    private static ChatResponse dummy(ChatModel m) {
        return null;
    }
}
