package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.batch.core.ChunkListener;
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
                              ChunkListener chunkListener,
                              ItemReader<ConvertableEntry> entryItemReader,
                              ItemProcessor<ConvertableEntry, ConvertableEntry> uniProtDocumentItemProcessor,
                              ItemWriter<ConvertableEntry> uniProtDocumentItemWriter) {
        return this.stepBuilderFactory.get(UNIPROTKB_INDEX_STEP)
                .<ConvertableEntry, ConvertableEntry>chunk(uniProtKBIndexingProperties.getChunkSize())
                .faultTolerant()
                .skipLimit(uniProtKBIndexingProperties.getSkipLimit())
                .retry(HttpSolrClient.RemoteSolrException.class)
                .retry(UncategorizedSolrException.class)
                .retry(SolrServerException.class)
                .retryLimit(uniProtKBIndexingProperties.getRetryLimit())
                .reader(entryItemReader)
                .processor(uniProtDocumentItemProcessor)
                .writer(uniProtDocumentItemWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new EntrySkipWriteListener())
                .build();
    }
}