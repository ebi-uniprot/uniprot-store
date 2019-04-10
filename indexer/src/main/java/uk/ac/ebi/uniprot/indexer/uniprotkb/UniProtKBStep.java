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
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.document.SolrCollection;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({UniProtKBConfig.class})
public class UniProtKBStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final SolrTemplate solrTemplate;
    private final UniProtKBIndexingProperties uniProtKBIndexingProperties;

    @Autowired
    public UniProtKBStep(StepBuilderFactory stepBuilderFactory,
                         SolrTemplate solrTemplate,
                         UniProtKBIndexingProperties uniProtKBIndexingProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.solrTemplate = solrTemplate;
        this.uniProtKBIndexingProperties = uniProtKBIndexingProperties;
    }

    @Bean(name = "IndexUniProtKBStep")
    public Step indexCrossRef(StepExecutionListener stepListener,
                              ChunkListener chunkListener,
                              ItemReader<UniProtEntry> uniProtEntryItemReader,
                              ItemProcessor<UniProtEntry, UniProtDocument> uniProtItemProcessor,
                              ItemWriter<UniProtDocument> uniProtDocumentItemWriter) {
        return this.stepBuilderFactory.get(Constants.UNIPROTKB_INDEX_STEP)
                .<UniProtEntry, UniProtDocument>chunk(uniProtKBIndexingProperties.getChunkSize())
                .faultTolerant()
                .skipLimit(uniProtKBIndexingProperties.getSkipLimit())
                .retry(HttpSolrClient.RemoteSolrException.class)
                .retry(UncategorizedSolrException.class)
                .retry(SolrServerException.class)
                .retryLimit(uniProtKBIndexingProperties.getRetryLimit())
                .reader(uniProtEntryItemReader)
                .processor(uniProtItemProcessor)
                .writer(uniProtDocumentItemWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean
    public ItemWriter<UniProtDocument> uniProtDocumentItemWriter() {
        return new SolrDocumentWriter<>(this.solrTemplate, SolrCollection.uniprot);
    }
}