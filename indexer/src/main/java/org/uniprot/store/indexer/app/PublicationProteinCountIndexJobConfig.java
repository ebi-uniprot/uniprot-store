package org.uniprot.store.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author sahmad
 * @created 23/12/2020
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "publications-count")
@ComponentScan(basePackages = {"org.uniprot.store.indexer.publication.count"})
@Configuration
public class PublicationProteinCountIndexJobConfig {
}
