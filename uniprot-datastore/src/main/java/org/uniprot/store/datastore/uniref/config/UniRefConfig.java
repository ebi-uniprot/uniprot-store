package org.uniprot.store.datastore.uniref.config;

import net.jodah.failsafe.RetryPolicy;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.datastore.utils.Constants;

import java.time.temporal.ChronoUnit;

import static java.util.Collections.singletonList;

/**
 * @author jluo
 * @date: 20 Aug 2019
 */
@Configuration
@EnableConfigurationProperties({UniRefStoreProperties.class})
public class UniRefConfig {
    private final UniRefStoreProperties unirefStoreProperties;

    @Autowired
    public UniRefConfig(UniRefStoreProperties unirefStoreProperties) {
        this.unirefStoreProperties = unirefStoreProperties;
    }

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(unirefStoreProperties.getWriteRetryLimit())
                .withBackoff(
                        unirefStoreProperties.getWriteRetryBackOffFromMillis(),
                        unirefStoreProperties.getWriteRetryBackOffToMillis(),
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
