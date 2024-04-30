package org.uniprot.store.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "keyword")
@ComponentScan(basePackages = {"org.uniprot.store.indexer.keyword"})
@Configuration
public class KeywordIndexingJobConfig {}
