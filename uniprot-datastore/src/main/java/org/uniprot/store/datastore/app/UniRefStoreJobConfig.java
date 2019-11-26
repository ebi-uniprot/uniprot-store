package org.uniprot.store.datastore.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author jluo
 * @date: 16 Aug 2019
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "uniref")
@ComponentScan(basePackages = {"org.uniprot.store.datastore.uniref"})
@Configuration
public class UniRefStoreJobConfig {}
