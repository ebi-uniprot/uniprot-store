package org.uniprot.store.datastore.member.uniref.config;

import static java.util.Collections.singletonList;

import java.time.temporal.ChronoUnit;

import net.jodah.failsafe.RetryPolicy;

import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.datastore.utils.Constants;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Configuration
@EnableConfigurationProperties({UniRefMemberStoreProperties.class})
public class UniRefMemberConfig {
    private final UniRefMemberStoreProperties unirefMemberStoreProperties;

    @Autowired
    public UniRefMemberConfig(UniRefMemberStoreProperties unirefMemberStoreProperties) {
        this.unirefMemberStoreProperties = unirefMemberStoreProperties;
    }

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(unirefMemberStoreProperties.getWriteRetryLimit())
                .withBackoff(
                        unirefMemberStoreProperties.getWriteRetryBackOffFromMillis(),
                        unirefMemberStoreProperties.getWriteRetryBackOffToMillis(),
                        ChronoUnit.MILLIS);
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener =
                new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(
                new String[] {
                    Constants.STORE_FAILED_ENTRIES_COUNT_KEY,
                    Constants.STORE_WRITTEN_ENTRIES_COUNT_KEY
                });
        return executionContextPromotionListener;
    }
}
