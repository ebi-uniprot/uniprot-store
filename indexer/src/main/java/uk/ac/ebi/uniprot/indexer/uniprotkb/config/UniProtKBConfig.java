package uk.ac.ebi.uniprot.indexer.uniprotkb.config;

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
@EnableConfigurationProperties({UniProtKBIndexingProperties.class})
public class UniProtKBConfig {
    private UniProtKBIndexingProperties uniProtKBIndexingProperties = new UniProtKBIndexingProperties();

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(uniProtKBIndexingProperties.getWriteRetryLimit())
                .withBackoff(uniProtKBIndexingProperties.getWriteRetryBackOffFromMillis(),
                             uniProtKBIndexingProperties.getWriteRetryBackOffToMillis(),
                             ChronoUnit.MILLIS);
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(new String[]{Constants.INDEX_FAILED_ENTRIES_COUNT_KEY,
                                                               Constants.INDEX_WRITTEN_ENTRIES_COUNT_KEY,
                                                               Constants.SUGGESTIONS_SET});
        return executionContextPromotionListener;
    }
}
