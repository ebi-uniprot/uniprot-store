package org.uniprot.store.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author sahmad
 * @created 06/07/2021
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "help-pages")
@ComponentScan(basePackages = {"org.uniprot.store.indexer.help"})
@Configuration
public class HelpIndexingJobConfig {
}
