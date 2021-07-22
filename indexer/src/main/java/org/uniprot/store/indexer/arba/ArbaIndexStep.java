package org.uniprot.store.indexer.arba;

import static org.uniprot.store.indexer.common.utils.Constants.ARBA_INDEX_STEP;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.arba.ArbaDocument;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
@Configuration
public class ArbaIndexStep {
    @Value(("${arba.indexing.chunkSize}"))
    private int chunkSize = 50;

    @Value(("${arba.indexing.xml.file}"))
    private String filePath;

    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public ArbaIndexStep(StepBuilderFactory stepBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Step indexArbaStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<UniRuleType> arbaReader,
            ItemProcessor<UniRuleType, ArbaDocument> arbaProcessor,
            ItemWriter<ArbaDocument> arbaWriter) {
        return this.stepBuilderFactory
                .get(ARBA_INDEX_STEP)
                .<UniRuleType, ArbaDocument>chunk(chunkSize)
                .reader(arbaReader)
                .processor(arbaProcessor)
                .writer(arbaWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new LogRateListener<ArbaDocument>())
                .build();
    }

    @Bean
    public ItemReader<UniRuleType> arbaReader() {
        return new ArbaXmlEntryReader(filePath);
    }

    @Bean
    public ItemProcessor<UniRuleType, ArbaDocument> arbaProcessor() {
        return new ArbaProcessor();
    }

    @Bean
    public ItemWriter<ArbaDocument> arbaWriter(UniProtSolrClient solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.arba);
    }
}
