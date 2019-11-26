package org.uniprot.store.indexer.uniref;

import static org.uniprot.store.indexer.common.utils.Constants.UNIREF_INDEX_STEP;

import java.io.File;
import java.io.IOException;

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
import org.uniprot.core.cv.taxonomy.FileNodeIterable;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniref.UniRefDocument;

/**
 * @author jluo
 * @date: 14 Aug 2019
 */
@Configuration
public class UniRefIndexStep {
    @Value(("${uniref.indexing.chunkSize}"))
    private int chunkSize = 50;

    @Value(("${uniref.indexing.xml.file}"))
    private String unirefXmlFilename;

    @Value(("${uniprotkb.indexing.taxonomyFile}"))
    private String taxonomyFile;

    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public UniRefIndexStep(StepBuilderFactory stepBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean("UniRefIndexStep")
    public Step unirefIndexViaXmlStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<Entry> itemReader,
            ItemProcessor<Entry, UniRefDocument> itemProcessor,
            ItemWriter<UniRefDocument> itemWriter) {
        return this.stepBuilderFactory
                .get(UNIREF_INDEX_STEP)
                .<Entry, UniRefDocument>chunk(chunkSize)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new LogRateListener<UniRefDocument>())
                .build();
    }

    @Bean
    public ItemReader<Entry> unirefReader() throws IOException {
        return new UniRefXmlEntryReader(unirefXmlFilename);
    }

    @Bean
    public ItemProcessor<Entry, UniRefDocument> unirefEntryProcessor() {
        return new UniRefEntryProcessor(unirefEntryConverter());
    }

    private DocumentConverter<Entry, UniRefDocument> unirefEntryConverter() {
        return new UniRefDocumentConverter(createTaxonomyRepo());
    }

    @Bean
    public ItemWriter<UniRefDocument> unirefItemWriter(UniProtSolrOperations solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.uniref);
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }
}
