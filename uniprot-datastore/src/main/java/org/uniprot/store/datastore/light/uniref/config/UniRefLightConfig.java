package org.uniprot.store.datastore.light.uniref.config;

import static java.util.Collections.singletonList;

import java.time.temporal.ChronoUnit;

import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.datastore.utils.Constants;

import net.jodah.failsafe.RetryPolicy;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
@Configuration
@EnableConfigurationProperties({UniRefLightStoreProperties.class})
public class UniRefLightConfig {
    private final UniRefLightStoreProperties unirefLightStoreProperties;

    @Autowired
    public UniRefLightConfig(UniRefLightStoreProperties unirefLightStoreProperties) {
        this.unirefLightStoreProperties = unirefLightStoreProperties;
    }

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(unirefLightStoreProperties.getWriteRetryLimit())
                .withBackoff(
                        unirefLightStoreProperties.getWriteRetryBackOffFromMillis(),
                        unirefLightStoreProperties.getWriteRetryBackOffToMillis(),
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
