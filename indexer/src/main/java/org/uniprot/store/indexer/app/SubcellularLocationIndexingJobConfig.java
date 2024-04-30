package org.uniprot.store.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "subcellularlocation")
@ComponentScan(basePackages = {"org.uniprot.store.indexer.subcell"})
@Configuration
public class SubcellularLocationIndexingJobConfig {}
