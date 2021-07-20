package org.uniprot.store.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 * @since 19/07/2021
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "arba")
@ComponentScan(basePackages = {"org.uniprot.store.indexer.arba"})
@Configuration
public class ArbaIndexingJobConfig {}
