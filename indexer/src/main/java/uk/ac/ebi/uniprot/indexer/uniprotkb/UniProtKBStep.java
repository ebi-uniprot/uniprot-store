package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.util.HashMap;
import java.util.Map;

import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({UniProtKBConfig.class})
public class UniProtKBStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtKBIndexingProperties uniProtKBIndexingProperties;

    @Autowired
    public UniProtKBStep(StepBuilderFactory stepBuilderFactory,
                         UniProtKBIndexingProperties indexingProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniProtKBIndexingProperties = indexingProperties;
    }

    private RetryPolicy retryPolicy() {
        Map<Class<? extends Throwable>, Boolean> throwables = new HashMap<>();
        throwables.put(HttpSolrClient.RemoteSolrException.class, true);
        return new SimpleRetryPolicy(1, throwables);
    }

    @Bean
    public Step uniProtKBIndexingMainFFStep(StepExecutionListener stepListener,
                                            ItemReader<ConvertibleEntry> entryItemReader,
                                            ItemProcessor<ConvertibleEntry, ConvertibleEntry> uniProtDocumentItemProcessor,
                                            ItemWriter<ConvertibleEntry> uniProtDocumentItemWriter) {
        return this.stepBuilderFactory.get(UNIPROTKB_INDEX_STEP)
                .<ConvertibleEntry, ConvertibleEntry>chunk(uniProtKBIndexingProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(uniProtDocumentItemProcessor)
                .writer(uniProtDocumentItemWriter)
//                .faultTolerant()
//                .skipLimit(uniProtKBIndexingProperties.getSkipLimit())
//                .skip(HttpSolrClient.RemoteSolrException.class)
//                .skip(UncategorizedSolrException.class)
//                .skip(SolrServerException.class)
//                .retry(HttpSolrClient.RemoteSolrException.class)
//                .retry(UncategorizedSolrException.class)
//                .retry(SolrServerException.class)
//                .retryLimit(uniProtKBIndexingProperties.getRetryLimit())
//                .retryPolicy(retryPolicy())
//                .skipPolicy((throwable, i) -> true)
                .listener(stepListener)
//                .listener(convertibleEntryChunkListener)
                .build();
    }
}