package org.uniprot.store.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author gqi
 *     <p>Created 22 Oct 2019
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "uniref")
@ComponentScan(basePackages = {"org.uniprot.store.indexer.uniref"})
@Configuration
public class UniRefIndexingJobConfig {}
