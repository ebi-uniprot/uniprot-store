package org.uniprot.store.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author sahmad
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "unirule")
@ComponentScan(basePackages = {"org.uniprot.store.indexer.unirule"})
@Configuration
public class UniRuleIndexingJobConfig {}
