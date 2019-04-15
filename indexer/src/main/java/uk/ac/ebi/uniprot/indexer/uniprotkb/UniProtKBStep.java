package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.solr.UncategorizedSolrException;

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

    @Bean
    public Step uniProtKBIndexingMainFFStep(StepExecutionListener stepListener,
                                            ConvertibleEntryChunkListener convertibleEntryChunkListener,
                                            ItemReader<ConvertibleEntry> entryItemReader,
                                            ItemProcessor<ConvertibleEntry, ConvertibleEntry> uniProtDocumentItemProcessor,
                                            ItemWriter<ConvertibleEntry> uniProtDocumentItemWriter,
                                            ExecutionContextPromotionListener promotionListener) {
        return this.stepBuilderFactory.get(UNIPROTKB_INDEX_STEP)
                .listener(promotionListener)
                .listener(convertibleEntryChunkListener)
                .<ConvertibleEntry, ConvertibleEntry>chunk(uniProtKBIndexingProperties.getChunkSize())
                .faultTolerant()
//                .skipLimit(uniProtKBIndexingProperties.getSkipLimit())
                .retryLimit(uniProtKBIndexingProperties.getRetryLimit())
                .retry(HttpSolrClient.RemoteSolrException.class)
                .retry(UncategorizedSolrException.class)
                .retry(SolrServerException.class)
                .skip(HttpSolrClient.RemoteSolrException.class)
                .skip(UncategorizedSolrException.class)
                .skip(SolrServerException.class)
//                .retryPolicy(new RetryPolicy() {
//                    @Override
//                    public boolean canRetry(RetryContext retryContext) {
//                        return true;
//                    }
//
//                    @Override
//                    public RetryContext open(RetryContext retryContext) {
//                        return null;
//                    }
//
//                    @Override
//                    public void close(RetryContext retryContext) {
//
//                    }
//
//                    @Override
//                    public void registerThrowable(RetryContext retryContext, Throwable throwable) {
//
//                    }
//                })
//                .skipPolicy(new SkipPolicy() {
//                    @Override
//                    public boolean shouldSkip(Throwable throwable, int i) throws SkipLimitExceededException {
//                        return true;
//                    }
//                })
                .reader(entryItemReader)
                .processor(uniProtDocumentItemProcessor)
                .writer(uniProtDocumentItemWriter)
//                .listener(promotionListener)
                .listener(stepListener)
//                .listener(convertibleEntryChunkListener)
                .build();
    }
}