package org.embulk.input.backlog.helpers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.embulk.config.ConfigSource;
import org.embulk.input.backlog.BacklogInputPlugin.PluginTask;
import org.embulk.input.backlog.Issue;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class BacklogHelper {

    public static final String DEFAULT_TIMESTAMP_PATTERN = "%Y-%m-%dT%H:%M:%S.%L%z";

    private BacklogHelper() {
    }

    /*
     * For getting the timestamp value of the node
     * Sometime if the parser could not parse the value then return null
     * */
    private static Timestamp getTimestampValue(final PluginTask task, final Column column, final String value) {
        final List<ColumnConfig> columnConfigs = task.getColumns().getColumns();
        String pattern = DEFAULT_TIMESTAMP_PATTERN;
        for (final ColumnConfig columnConfig : columnConfigs) {
            final ConfigSource columnConfigSource = columnConfig.getConfigSource();
            if (columnConfig.getName().equals(column.getName())
                    && columnConfigSource != null
                    && columnConfigSource.has("format")) {
                pattern = columnConfigSource.get(String.class, "format");
                break;
            }
        }
        final TimestampParser parser = TimestampParser.of(pattern, "UTC");
        Timestamp result = null;
        try {
            result = parser.parse(value);
        } catch (final Exception e) {
        }
        return result;
    }

    /*
     * For getting the Long value of the node
     * Sometime if error occurs (i.e a JSON value but user modified it as long) then return null
     * */
    private static Long getLongValue(final JsonElement value) {
        Long result = null;
        try {
            result = value.getAsLong();
        } catch (final Exception e) {
        }
        return result;
    }

    /*
     * For getting the Double value of the node
     * Sometime if error occurs (i.e a JSON value but user modified it as double) then return null
     * */
    private static Double getDoubleValue(final JsonElement value) {
        Double result = null;
        try {
            result = value.getAsDouble();
        } catch (final Exception e) {
        }
        return result;
    }

    /*
     * For getting the Boolean value of the node
     * Sometime if error occurs (i.e a JSON value but user modified it as boolean) then return null
     * */
    private static Boolean getBooleanValue(final JsonElement value) {
        Boolean result = null;
        try {
            result = value.getAsBoolean();
        } catch (final Exception e) {
        }
        return result;
    }

    public static void addRecord(final Issue issue, final Schema schema, final PluginTask task, final PageBuilder pageBuilder) {
        schema.visitColumns(new ColumnVisitor() {
            @Override
            public void jsonColumn(final Column column) {
                final JsonElement data = issue.getValue(column.getName());

                if (data.isJsonNull() || data.isJsonPrimitive()) {
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setString(column, data.getAsString());
                }
            }

            @Override
            public void stringColumn(final Column column) {
                final JsonElement data = issue.getValue(column.getName());

                if (data.isJsonNull()) {
                    pageBuilder.setNull(column);
                } else if (data.isJsonPrimitive()) {
                    pageBuilder.setString(column, data.getAsString());
                } else if (data.isJsonArray()) {
                    pageBuilder.setString(column, StreamSupport.stream(data.getAsJsonArray().spliterator(), false)
                                                               .map(obj -> {
                                                                   if (obj.isJsonPrimitive()) {
                                                                       return obj.getAsString();
                                                                   } else {
                                                                       return obj.toString();
                                                                   }
                                                               })
                                                               .collect(Collectors.joining(",")));
                } else {
                    pageBuilder.setString(column, data.toString());
                }
            }

            @Override
            public void timestampColumn(final Column column) {
                final JsonElement data = issue.getValue(column.getName());

                if (data.isJsonNull() || data.isJsonObject() || data.isJsonArray()) {
                    pageBuilder.setNull(column);
                } else {
                    final Timestamp value = getTimestampValue(task, column, data.getAsString());
                    if (value == null) {
                        pageBuilder.setNull(column);
                    } else {
                        pageBuilder.setTimestamp(column, value);
                    }
                }
            }

            @Override
            public void booleanColumn(final Column column) {
                final Boolean value = getBooleanValue(issue.getValue(column.getName()));

                if (value == null) {
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setBoolean(column, value);
                }
            }

            @Override
            public void longColumn(final Column column) {
                final Long value = getLongValue(issue.getValue(column.getName()));

                if (value == null) {
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setLong(column, value);
                }
            }

            @Override
            public void doubleColumn(final Column column) {
                final Double value = getDoubleValue(issue.getValue(column.getName()));

                if (value == null) {
                    pageBuilder.setNull(column);
                } else {
                    pageBuilder.setDouble(column, value);
                }
            }
        });

        pageBuilder.addRecord();
    }

    public static LinkedHashMap<String, Object> toLinkedHashMap(final JsonObject flt) {
        final LinkedHashMap<String, Object> result = new LinkedHashMap<>();

        for (final String key : flt.keySet()) {
            final JsonElement elem = flt.get(key);
            if (elem.isJsonPrimitive()) {
                result.put(key, flt.get(key).getAsString());
            } else {
                result.put(key, elem);
            }
        }
        return result;
    }
}
