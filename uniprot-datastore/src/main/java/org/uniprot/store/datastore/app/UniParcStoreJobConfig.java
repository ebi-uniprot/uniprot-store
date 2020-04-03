package org.uniprot.store.datastore.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "uniparc")
@ComponentScan(basePackages = {"org.uniprot.store.datastore.uniparc"})
@Configuration
public class UniParcStoreJobConfig {}
