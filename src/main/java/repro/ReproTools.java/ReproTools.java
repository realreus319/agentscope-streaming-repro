package repro;

import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;

/**
 * Tools used by the repro chains.
 *
 * <p>Both tools are intentionally trivial and deterministic so that any latency effect
 * comes from the agent / streaming pipeline, not from tool execution time.</p>
 */
public class ReproTools {

    /**
     * Get the weather for a city.
     *
     * @param city the city name
     * @return a short weather description
     */
    @Tool(name = "get_weather", description = "Get the weather for a city")
    public String getWeather(@ToolParam(name = "city", description = "City name") String city) {
        return "Sunny, 20C in " + city;
    }

    /**
     * Get the current time in a timezone.
     *
     * @param timezone the timezone name
     * @return a short time description
     */
    @Tool(name = "get_time", description = "Get the current time in a timezone")
    public String getTime(@ToolParam(name = "timezone", description = "Timezone name") String timezone) {
        return "12:00 in " + timezone;
    }
}
