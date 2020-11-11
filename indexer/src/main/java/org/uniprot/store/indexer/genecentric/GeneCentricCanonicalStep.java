package org.uniprot.store.indexer.genecentric;

import java.io.IOException;

import org.springframework.batch.core.*;
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
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;
import org.uniprot.store.search.document.genecentric.GeneCentricDocumentConverter;

/**
 * @author lgonzales
 * @since 02/11/2020
 */
@Configuration
public class GeneCentricCanonicalStep {

    private final StepBuilderFactory stepBuilderFactory;

    @Value(("${solr.indexing.chunkSize}"))
    private int chunkSize = 100;

    @Value(("${genecentric.canonical.fasta.files}"))
    private String geneCentricCanonicalFiles;

    @Autowired
    public GeneCentricCanonicalStep(StepBuilderFactory stepBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean("geneCentricCanonicalIndexStep")
    public Step geneCentricCanonicalIndexStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("geneCentricCanonicalReader") ItemReader<GeneCentricEntry> itemReader,
            @Qualifier("geneCentricCanonicalProcessor")
                    ItemProcessor<GeneCentricEntry, GeneCentricDocument> processor,
            @Qualifier("geneCentricCanonicalWriter") ItemWriter<GeneCentricDocument> itemWriter,
            UniProtSolrClient uniProtSolrClient) {
        return this.stepBuilderFactory
                .get("GeneCentric_Canonical_Index_Step")
                .<GeneCentricEntry, GeneCentricDocument>chunk(chunkSize)
                .reader(itemReader)
                .processor(processor)
                .writer(itemWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new LogRateListener<GeneCentricDocument>())
                .listener(
                        new StepExecutionListener() {
                            @Override
                            public void beforeStep(StepExecution stepExecution) {
                                // no ops
                            }

                            @Override
                            public ExitStatus afterStep(StepExecution stepExecution) {
                                uniProtSolrClient.commit(SolrCollection.genecentric);
                                return stepExecution.getExitStatus();
                            }
                        })
                .build();
    }

    @Bean(name = "geneCentricCanonicalReader")
    public ItemReader<GeneCentricEntry> geneCentricCanonicalReader(
            GeneCentricFastaItemReader geneCentricFlatFileReader) throws IOException {
        PathMatchingResourcePatternResolver resourceResolver =
                new PathMatchingResourcePatternResolver();
        MultiResourceItemReader<GeneCentricEntry> reader = new MultiResourceItemReader<>();
        reader.setResources(resourceResolver.getResources(geneCentricCanonicalFiles));
        reader.setDelegate(geneCentricFlatFileReader);
        return reader;
    }

    @Bean("geneCentricCanonicalProcessor")
    public ItemProcessor<GeneCentricEntry, GeneCentricDocument> geneCentricCanonicalProcessor(
            GeneCentricDocumentConverter geneCentricDocumentConverter) {
        return new GeneCentricCanonicalProcessor(geneCentricDocumentConverter);
    }

    @Bean(name = "geneCentricCanonicalWriter")
    public ItemWriter<GeneCentricDocument> geneCentricCanonicalWriter(
            UniProtSolrClient solrOperations) {
        return new GeneCentricCanonicalWriter(solrOperations);
    }
}
