package uk.ac.ebi.uniprot.datastore.uniprotkb.config;

import net.jodah.failsafe.RetryPolicy;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

import java.time.temporal.ChronoUnit;

import static java.util.Collections.singletonList;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({ListenerConfig.class})
@EnableConfigurationProperties({UniProtKBDataStoreProperties.class})
public class UniProtKBConfig {
    private UniProtKBDataStoreProperties uniProtKBDataStoreProperties = new UniProtKBDataStoreProperties();

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(uniProtKBDataStoreProperties.getWriteRetryLimit())
                .withBackoff(uniProtKBDataStoreProperties.getWriteRetryBackOffFromMillis(),
                             uniProtKBDataStoreProperties.getWriteRetryBackOffToMillis(),
                             ChronoUnit.MILLIS);
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(new String[]{Constants.INDEX_FAILED_ENTRIES_COUNT_KEY,
                                                               Constants.INDEX_WRITTEN_ENTRIES_COUNT_KEY,
                                                               Constants.SUGGESTIONS_MAP});
        return executionContextPromotionListener;
    }
}
