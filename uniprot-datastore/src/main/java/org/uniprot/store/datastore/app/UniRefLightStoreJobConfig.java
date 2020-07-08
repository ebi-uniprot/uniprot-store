package org.uniprot.store.datastore.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 * @since 08/07/2020
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "uniref-light")
@ComponentScan(basePackages = {"org.uniprot.store.datastore.light.uniref"})
@Configuration
public class UniRefLightStoreJobConfig {
}
