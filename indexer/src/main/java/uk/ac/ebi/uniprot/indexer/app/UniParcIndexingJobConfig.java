package uk.ac.ebi.uniprot.indexer.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @author jluo
 * @date: 19 Jun 2019
 *
*/
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "uniparc")
@ComponentScan(basePackages = {"uk.ac.ebi.uniprot.indexer.uniparc"})
@Configuration
public class UniParcIndexingJobConfig {

}

