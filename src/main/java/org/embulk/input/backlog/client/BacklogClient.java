package org.embulk.input.backlog.client;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.embulk.config.ConfigException;
import org.embulk.input.backlog.BacklogInputPlugin.PluginTask;
import org.embulk.input.backlog.Issue;
import org.embulk.input.backlog.exception.BacklogException;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author thangnc
 */
public class BacklogClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(BacklogClient.class);
    private static final int HTTP_TIMEOUT = 300 * 1000;

    public BacklogClient() {
    }

    public CloseableHttpClient createHttpClient() {
        return HttpClientBuilder.create()
                                .setDefaultRequestConfig(RequestConfig.custom()
                                                                      .setConnectTimeout(HTTP_TIMEOUT)
                                                                      .setConnectionRequestTimeout(HTTP_TIMEOUT)
                                                                      .setSocketTimeout(HTTP_TIMEOUT)
                                                                      .setCookieSpec(CookieSpecs.STANDARD)
                                                                      .build())
                                .build();
    }

    public List<Issue> searchIssues(final PluginTask task, final int startAt, final int maxResults) {
        final String response = searchBacklogIssues(task, startAt, maxResults);
        final JsonArray result = new JsonParser().parse(response).getAsJsonArray();

        return StreamSupport.stream(result.spliterator(), false)
                            .map(jsonElement -> {
                                final JsonObject json = jsonElement.getAsJsonObject();
                                final Set<Map.Entry<String, JsonElement>> entries = json.entrySet();
                                json.remove("sharedFiles");
                                json.remove("attachments");
                                json.remove("stars");
                                json.remove("customFields");

                                // Merged all properties in fields to the object
                                for (final Map.Entry<String, JsonElement> entry : entries) {
                                    json.add(entry.getKey(), entry.getValue());
                                }

                                return new Issue(json);
                            })
                            .collect(Collectors.toList());
    }

    private String performRequest(final PluginTask task, final int startAt, final int maxResults)
            throws URISyntaxException, BacklogException {

        try (CloseableHttpClient client = createHttpClient()) {
            HttpRequestBase request;
            request = createGetRequest(task, task.getUri() + "/api/v2/issues?projectId[]=45687&offset=0&count=20");

            try (CloseableHttpResponse response = client.execute(request)) {
                final int statusCode = response.getStatusLine().getStatusCode();

                if (statusCode != HttpStatus.SC_OK) {
                    throw new BacklogException(statusCode, extractErrorMessages(EntityUtils.toString(response.getEntity())));
                }

                return EntityUtils.toString(response.getEntity());
            }
        } catch (final IOException e) {
            throw new BacklogException(-1, e.getMessage());
        }
    }

    private HttpRequestBase createGetRequest(final PluginTask task, final String url)
            throws URISyntaxException {

        final HttpGet request;

        switch (task.getAuthMethod()) {
            case API_KEY:
            case OAUTH:
            default:
                URI newUrl = new URIBuilder(url).addParameter("apiKey", task.getApiKey()).build();
                request = new HttpGet(newUrl.toString());
                break;
        }

        return request;
    }

    private String extractErrorMessages(final String errorResponse) {
        final List<String> messages = new ArrayList<>();

        try {
            final JsonObject errorObject = new JsonParser().parse(errorResponse).getAsJsonObject();
            for (final JsonElement element : errorObject.get("errors").getAsJsonArray()) {
                messages.add(element.getAsString());
            }
        } catch (final Exception e) {
            messages.add(errorResponse);
        }

        return String.join(" , ", messages);
    }

    private String searchBacklogIssues(final PluginTask task, final int startAt, final int maxResults) {
        try {
            return RetryExecutor.builder()
                                .withRetryLimit(task.getRetryLimit())
                                .withInitialRetryWaitMillis(task.getInitialRetryIntervalMillis())
                                .withMaxRetryWaitMillis(task.getMaximumRetryIntervalMillis())
                                .build()
                                .runInterruptible(new Retryable<String>() {
                                    @Override
                                    public String call()
                                            throws Exception {
                                        return performRequest(task, startAt, maxResults);
                                    }

                                    @Override
                                    public boolean isRetryableException(final Exception exception) {
                                        if (exception instanceof BacklogException) {
                                            final int statusCode = ((BacklogException) exception).getStatusCode();
                                            return statusCode / 100 != 4 || statusCode == HttpStatus.SC_UNAUTHORIZED || statusCode == 429;
                                        }
                                        return false;
                                    }

                                    @Override
                                    public void onRetry(final Exception exception, final int retryCount, final int retryLimit, final int retryWait) {

                                        if (exception instanceof BacklogException) {
                                            final String message = String
                                                    .format("Retrying %d/%d after %d seconds. HTTP status code: %s",
                                                            retryCount, retryLimit,
                                                            retryWait / 1000,
                                                            ((BacklogException) exception).getStatusCode());
                                            LOGGER.warn(message);
                                        } else {
                                            final String message = String
                                                    .format("Retrying %d/%d after %d seconds. Message: %s",
                                                            retryCount, retryLimit,
                                                            retryWait / 1000,
                                                            exception.getMessage());
                                            LOGGER.warn(message, exception);
                                        }
                                    }

                                    @Override
                                    public void onGiveup(final Exception firstException, final Exception lastException) {
                                        LOGGER.warn("Retry Limit Exceeded");
                                    }
                                });
        } catch (RetryGiveupException | InterruptedException e) {
            if (e instanceof RetryGiveupException && e.getCause() != null && e.getCause() instanceof BacklogException) {
                throw new ConfigException(e.getCause().getMessage());
            }
            throw new ConfigException(e);
        }
    }
}
