package org.uniprot.store.indexer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

/**
 * Solr properties bean that will be injected with values from application.properties.
 *
 * <p>//TODO: REUSE COMMON SOLR CONFIG..... (DUPLICATED CODE FOR PoC ONLY)
 *
 * @author lgonzales
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "spring.data.solr")
class RepositoryConfigProperties {
    private static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 1000 * 20;
    private static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 1000 * 60 * 60;

    private String zkHost;

    private String httphost;

    private String username;

    private String password;

    private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_MILLIS;

    private int socketTimeout = DEFAULT_SOCKET_TIMEOUT_MILLIS;
}
