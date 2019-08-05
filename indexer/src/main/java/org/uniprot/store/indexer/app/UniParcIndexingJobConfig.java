package org.uniprot.store.indexer.app;

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
@ComponentScan(basePackages = {"org.uniprot.store.indexer.uniparc"})
@Configuration
public class UniParcIndexingJobConfig {

}

