package uk.ac.ebi.uniprot.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Created 28/05/19
 *
 * @author Edd
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "taxonomy")
@ComponentScan(basePackages = {"uk.ac.ebi.uniprot.indexer.taxonomy"})
@Configuration
public class TaxonomyIndexingJobConfig {
}