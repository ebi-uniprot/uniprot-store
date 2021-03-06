package org.uniprot.store.indexer.proteome;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

/**
 * @author jluo
 * @date: 23 Apr 2019
 */
@Configuration
public class ProteomeIndexStep {
    private final StepBuilderFactory stepBuilderFactory;

    @Value(("${solr.indexing.chunkSize}"))
    private int chunkSize = 100;

    @Autowired
    public ProteomeIndexStep(StepBuilderFactory stepBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean("ProteomeIndexStep")
    public Step proteomeIndexViaXmlStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("proteomeXmlReader") ItemReader<Proteome> itemReader,
            @Qualifier("ProteomeDocumentProcessor")
                    ItemProcessor<Proteome, ProteomeDocument> proteomeEntryProcessor,
            @Qualifier("proteomeItemWriter") ItemWriter<ProteomeDocument> itemWriter) {
        return this.stepBuilderFactory
                .get("Proteome_Index_Step")
                .<Proteome, ProteomeDocument>chunk(chunkSize)
                .reader(itemReader)
                .processor(proteomeEntryProcessor)
                .writer(itemWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new LogRateListener<ProteomeDocument>())
                .build();
    }
}
