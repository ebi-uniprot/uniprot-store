package uk.ac.ebi.uniprot.datastore.uniprotkb.config;

import net.jodah.failsafe.RetryPolicy;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.uniprot.datastore.listener.ListenerConfig;
import uk.ac.ebi.uniprot.datastore.utils.Constants;

import java.time.temporal.ChronoUnit;

import static java.util.Collections.singletonList;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({ListenerConfig.class})
@EnableConfigurationProperties({UniProtKBStoreProperties.class})
public class UniProtKBConfig {
    private UniProtKBStoreProperties uniProtKBStoreProperties = new UniProtKBStoreProperties();

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(uniProtKBStoreProperties.getWriteRetryLimit())
                .withBackoff(uniProtKBStoreProperties.getWriteRetryBackOffFromMillis(),
                             uniProtKBStoreProperties.getWriteRetryBackOffToMillis(),
                             ChronoUnit.MILLIS);
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(new String[]{Constants.DATASTORE_FAILED_ENTRIES_COUNT_KEY,
                                                               Constants.DATASTORE_WRITTEN_ENTRIES_COUNT_KEY});
        return executionContextPromotionListener;
    }
}
