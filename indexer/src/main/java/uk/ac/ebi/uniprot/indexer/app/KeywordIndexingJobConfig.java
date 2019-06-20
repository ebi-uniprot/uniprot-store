package uk.ac.ebi.uniprot.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "keyword")
@ComponentScan(basePackages = {"uk.ac.ebi.uniprot.indexer.keyword"})
@Configuration
public class KeywordIndexingJobConfig {
}
