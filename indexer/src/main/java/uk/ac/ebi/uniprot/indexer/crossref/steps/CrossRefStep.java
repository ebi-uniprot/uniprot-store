package uk.ac.ebi.uniprot.indexer.crossref.steps;


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
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.domain.crossref.CrossRefEntry;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.crossref.processor.CrossRefProcessor;
import uk.ac.ebi.uniprot.indexer.crossref.readers.CrossRefReader;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import java.io.IOException;

@Configuration
public class CrossRefStep {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private UniProtSolrOperations solrOperations;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.xref.file.path}"))
    private String filePath;

    @Bean(name = "IndexCrossRefStep")
    public Step indexCrossRef(StepExecutionListener stepListener, ChunkListener chunkListener,
                              @Qualifier("crossRefReader") ItemReader<CrossRefEntry> xrefReader,
                              @Qualifier("crossRefProcessor") ItemProcessor<CrossRefEntry, CrossRefDocument> xrefProcessor,
                              @Qualifier("crossRefWriter") ItemWriter<CrossRefDocument> xrefWriter) {
        return this.steps.get(Constants.CROSS_REF_INDEX_STEP)
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
        return new SolrDocumentWriter<>(this.solrOperations, SolrCollection.crossref);
    }

    @Bean(name = "crossRefProcessor")
    public ItemProcessor<CrossRefEntry, CrossRefDocument> xrefProcessor() {

        return new CrossRefProcessor();

    }
}
