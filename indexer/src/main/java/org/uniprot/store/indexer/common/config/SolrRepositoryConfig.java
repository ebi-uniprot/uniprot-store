package org.uniprot.store.indexer.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/**
 * @author lgonzales
 *     <p>
 */
@Configuration
@Profile("online")
@Import(RepositoryConfigProperties.class)
public class SolrRepositoryConfig {
    @Bean
    public UniProtSolrOperations solrOperations(RepositoryConfigProperties configProperties) {
        return new UniProtSolrOperations(configProperties);
    }
}
