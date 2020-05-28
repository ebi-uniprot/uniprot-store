package org.uniprot.store.indexer.crossref.steps;

import java.io.IOException;

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
import org.uniprot.core.cv.xdb.CrossRefEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.crossref.processor.CrossRefProcessor;
import org.uniprot.store.indexer.crossref.readers.CrossRefReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.dbxref.CrossRefDocument;

@Configuration
public class CrossRefStep {

    @Autowired private StepBuilderFactory steps;

    @Autowired private UniProtSolrClient solrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.xref.file.path}"))
    private String filePath;

    @Bean(name = "IndexCrossRefStep")
    public Step indexCrossRef(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("crossRefReader") ItemReader<CrossRefEntry> xrefReader,
            @Qualifier("crossRefProcessor")
                    ItemProcessor<CrossRefEntry, CrossRefDocument> xrefProcessor,
            @Qualifier("crossRefWriter") ItemWriter<CrossRefDocument> xrefWriter) {
        return this.steps
                .get(Constants.CROSS_REF_INDEX_STEP)
                .<CrossRefEntry, CrossRefDocument>chunk(this.chunkSize)
                .reader(xrefReader)
                .processor(xrefProcessor)
                .writer(xrefWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "crossRefReader")
    public ItemReader<CrossRefEntry> xrefReader() throws IOException {
        return new CrossRefReader(this.filePath);
    }

    @Bean(name = "crossRefWriter")
    public ItemWriter<CrossRefDocument> xrefWriter() {
        return new SolrDocumentWriter<>(this.solrClient, SolrCollection.crossref);
    }

    @Bean(name = "crossRefProcessor")
    public ItemProcessor<CrossRefEntry, CrossRefDocument> xrefProcessor() {

        return new CrossRefProcessor();
    }
}
