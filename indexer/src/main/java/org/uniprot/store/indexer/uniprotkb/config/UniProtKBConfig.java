package org.uniprot.store.indexer.uniprotkb.config;

import static java.util.Collections.singletonList;

import java.time.temporal.ChronoUnit;

import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;

import net.jodah.failsafe.RetryPolicy;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({ListenerConfig.class})
@EnableConfigurationProperties({UniProtKBIndexingProperties.class})
public class UniProtKBConfig {
    private UniProtKBIndexingProperties uniProtKBIndexingProperties =
            new UniProtKBIndexingProperties();

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(uniProtKBIndexingProperties.getWriteRetryLimit())
                .withBackoff(
                        uniProtKBIndexingProperties.getWriteRetryBackOffFromMillis(),
                        uniProtKBIndexingProperties.getWriteRetryBackOffToMillis(),
                        ChronoUnit.MILLIS);
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener =
                new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(
                new String[] {
                    CommonConstants.FAILED_ENTRIES_COUNT_KEY,
                    CommonConstants.WRITTEN_ENTRIES_COUNT_KEY,
                    Constants.SUGGESTIONS_MAP
                });
        return executionContextPromotionListener;
    }
}
