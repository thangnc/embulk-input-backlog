package org.embulk.input.backlog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.backlog.client.BacklogClient;
import org.embulk.input.backlog.helpers.BacklogHelper;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.guess.SchemaGuess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author thangnc
 */
public class BacklogInputPlugin
        implements InputPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(BacklogInputPlugin.class);
    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder()
                                                                                       .addDefaultModules()
                                                                                       .build();
    private static final int GUESS_RECORDS_COUNT = 10;
    private static final int PREVIEW_RECORDS_COUNT = 10;

    public interface PluginTask
            extends Task {

        @Config("auth_method")
        @ConfigDefault("api_key")
        AuthMethod getAuthMethod();

        @Config("api_key")
        @ConfigDefault("null")
        String getApiKey();

        @Config("access_token")
        @ConfigDefault("null")
        Optional<String> getAccessToken();

        @Config("uri")
        @ConfigDefault("null")
        String getUri();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("120000")
        int getMaximumRetryIntervalMillis();

        @Config("timeout_millis")
        @ConfigDefault("300000")
        int getTimeoutMillis();

        @Config("retry_limit")
        @ConfigDefault("5")
        int getRetryLimit();

        @Config("dynamic_schema")
        @ConfigDefault("false")
        boolean getDynamicSchema();

        @Config("columns")
        SchemaConfig getColumns();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  InputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             InputPlugin.Control control) {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
                          Schema schema, int taskIndex,
                          PageOutput output) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        BacklogClient backlogClient = new BacklogClient();
        try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
            if (isPreview()) {
                final List<Issue> issues = backlogClient.searchIssues(task, 0, PREVIEW_RECORDS_COUNT);
                issues.forEach(issue -> BacklogHelper.addRecord(issue, schema, task, pageBuilder));
            } else {
                throw new UnsupportedOperationException("BacklogInputPlugin.run method is not implemented yet");
            }

            pageBuilder.finish();
        }
        return CONFIG_MAPPER_FACTORY.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config) {
        config.set("columns", new ObjectMapper().createArrayNode());
        PluginTask task = config.loadConfig(PluginTask.class);

//        JiraUtil.validateTaskConfig(task);
        BacklogClient backlogClient = getBacklogClient();

        return CONFIG_MAPPER_FACTORY.newConfigDiff().set("columns", getGuessColumns(backlogClient, task));
    }

    public PageBuilder getPageBuilder(final Schema schema, final PageOutput output) {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    private BacklogClient getBacklogClient() {
        return new BacklogClient();
    }

    private boolean isPreview() {
        return Exec.isPreview();
    }

    private SortedSet<String> getUniqueAttributes(final List<Issue> issues) {
        final SortedSet<String> uniqueAttributes = new TreeSet<>();

        for (final Issue issue : issues) {
            for (final Map.Entry<String, JsonElement> entry : issue.getFlatten().entrySet()) {
                uniqueAttributes.add(entry.getKey());
            }
        }

        return uniqueAttributes;
    }

    private List<ConfigDiff> getGuessColumns(final BacklogClient backlogClient, final PluginTask task) {
        final List<Issue> issues = backlogClient.searchIssues(task, 0, GUESS_RECORDS_COUNT);

        if (issues.isEmpty()) {
            throw new ConfigException("Could not guess schema due to empty data set");
        }

        return SchemaGuess.of(CONFIG_MAPPER_FACTORY)
                          .fromLinkedHashMapRecords(createGuessSample(issues, getUniqueAttributes(issues)));
    }

    private List<LinkedHashMap<String, Object>> createGuessSample(final List<Issue> issues, final Set<String> uniqueAttributes) {
        final List<LinkedHashMap<String, Object>> samples = new ArrayList<>();

        for (final Issue issue : issues) {
            final JsonObject flatten = issue.getFlatten();
            final JsonObject unified = new JsonObject();

            for (final String key : uniqueAttributes) {
                JsonElement value = flatten.get(key);
                if (value == null) {
                    value = JsonNull.INSTANCE;
                }
                unified.add(key, value);
            }

            samples.add(BacklogHelper.toLinkedHashMap(unified));
        }

        return samples;
    }
}
