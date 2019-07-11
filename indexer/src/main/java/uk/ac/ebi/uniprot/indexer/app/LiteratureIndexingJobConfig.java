package uk.ac.ebi.uniprot.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "literature")
@ComponentScan(basePackages = {"uk.ac.ebi.uniprot.indexer.literature"})
@Configuration
public class LiteratureIndexingJobConfig {
}
