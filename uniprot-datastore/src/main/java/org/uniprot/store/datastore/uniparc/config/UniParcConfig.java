package org.uniprot.store.datastore.uniparc.config;

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
 * @author lgonzales
 * @since 2020-03-03
 */
@Configuration
@EnableConfigurationProperties({UniParcStoreProperties.class})
public class UniParcConfig {
    private final UniParcStoreProperties uniParcStoreProperties;

    @Autowired
    public UniParcConfig(UniParcStoreProperties uniParcStoreProperties) {
        this.uniParcStoreProperties = uniParcStoreProperties;
    }

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(uniParcStoreProperties.getWriteRetryLimit())
                .withBackoff(
                        uniParcStoreProperties.getWriteRetryBackOffFromMillis(),
                        uniParcStoreProperties.getWriteRetryBackOffToMillis(),
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
