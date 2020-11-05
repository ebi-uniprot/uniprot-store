package org.uniprot.store.indexer.genecentric;

import java.io.IOException;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;

/**
 * @author lgonzales
 * @since 04/11/2020
 */
@Configuration
public class GeneCentricRelatedStep {

    private final StepBuilderFactory stepBuilderFactory;

    @Value(("${solr.indexing.chunkSize}"))
    private int chunkSize = 100;

    @Value(("${genecentric.related.fasta.files}"))
    private String geneCentricRelatedFiles;

    @Autowired
    public GeneCentricRelatedStep(StepBuilderFactory stepBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean("geneCentricRelatedIndexStep")
    public Step geneCentricRelatedIndexStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("geneCentricRelatedReader") ItemReader<GeneCentricEntry> itemReader,
            @Qualifier("geneCentricRelatedProcessor")
                    ItemProcessor<GeneCentricEntry, GeneCentricDocument> processor,
            @Qualifier("geneCentricRelatedWriter") ItemWriter<GeneCentricDocument> itemWriter) {
        return this.stepBuilderFactory
                .get("GeneCentric_Related_Index_Step")
                .<GeneCentricEntry, GeneCentricDocument>chunk(chunkSize)
                .reader(itemReader)
                .processor(processor)
                .writer(itemWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new LogRateListener<GeneCentricDocument>())
                .build();
    }

    @Bean(name = "geneCentricRelatedReader")
    public ItemReader<GeneCentricEntry> geneCentricRelatedReader(
            GeneCentricFastaItemReader geneCentricFlatFileReader) throws IOException {
        PathMatchingResourcePatternResolver resourceResolver =
                new PathMatchingResourcePatternResolver();
        MultiResourceItemReader<GeneCentricEntry> reader = new MultiResourceItemReader<>();
        reader.setResources(resourceResolver.getResources(geneCentricRelatedFiles));
        reader.setDelegate(geneCentricFlatFileReader);
        return reader;
    }

    @Bean("geneCentricRelatedProcessor")
    public ItemProcessor<GeneCentricEntry, GeneCentricDocument> geneCentricRelatedProcessor(
            UniProtSolrClient uniProtSolrClient,
            GeneCentricDocumentConverter geneCentricDocumentConverter) {
        return new GeneCentricRelatedProcessor(uniProtSolrClient, geneCentricDocumentConverter);
    }

    @Bean(name = "geneCentricRelatedWriter")
    public ItemWriter<GeneCentricDocument> geneCentricRelatedWriter(
            UniProtSolrClient solrOperations,
            GeneCentricDocumentConverter geneCentricDocumentConverter) {
        return new GeneCentricRelatedWriter(solrOperations, geneCentricDocumentConverter);
    }
}
