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
    @Bean(destroyMethod = "cleanUp")
    public UniProtSolrClient uniProtSolrClient(RepositoryConfigProperties configProperties) {
        return new UniProtSolrClient(configProperties);
    }
}
