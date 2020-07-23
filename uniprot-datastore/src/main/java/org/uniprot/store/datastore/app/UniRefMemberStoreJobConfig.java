package org.uniprot.store.datastore.app;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@ConditionalOnProperty(prefix = "uniprot.job", name = "name", havingValue = "uniref-member")
@ComponentScan(basePackages = {"org.uniprot.store.datastore.member.uniref"})
@Configuration
public class UniRefMemberStoreJobConfig {}
