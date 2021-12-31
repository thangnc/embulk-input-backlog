package org.embulk.input.backlog;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

/**
 * @author thangnc
 */
public enum AuthMethod {

    OAUTH("oauth2"),
    API_KEY("api_key");

    private String type;

    AuthMethod(final String type) {
        this.type = type;
    }

    /**
     * Gets type.
     *
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * Find by type auth method.
     *
     * @param type the type
     * @return the auth method
     */
    @JsonCreator
    public static AuthMethod findByType(final String type) {
        for (AuthMethod method : values()) {
            if (method.getType().equals(type.toLowerCase())) {
                return method;
            }
        }

        throw new ConfigException(String.format("Unknown auth_method '%s'. Supported targets are [api_key, oauth2]",
                                                type));
    }
}
