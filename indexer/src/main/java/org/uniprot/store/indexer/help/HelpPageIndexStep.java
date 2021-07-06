package org.uniprot.store.indexer.help;

import java.io.IOException;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.help.HelpDocument;

/**
 * @author sahmad
 * @created 06/07/2021
 */
@Configuration
public class HelpPageIndexStep {
    private final StepBuilderFactory steps;
    private final UniProtSolrClient uniProtSolrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.help.page.files.directory}"))
    private String directoryPath;

    @Autowired
    public HelpPageIndexStep(StepBuilderFactory steps, UniProtSolrClient uniProtSolrClient) {
        this.steps = steps;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean(name = "IndexHelpPageStep")
    public Step indexHelpPageStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<HelpDocument> helpDocumentItemReader,
            ItemWriter<HelpDocument> helpDocumentWriter) {
        return this.steps
                .get(Constants.HELP_PAGE_INDEX_STEP)
                .<HelpDocument, HelpDocument>chunk(this.chunkSize)
                .reader(helpDocumentItemReader)
                .writer(helpDocumentWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "helpPageReader")
    public ItemReader<HelpDocument> helpDocumentItemReader() throws IOException {
        return new HelpPageItemReader(this.directoryPath);
    }

    @Bean(name = "helpPageWriter")
    public ItemWriter<HelpDocument> helpDocumentWriter() {
        return new SolrDocumentWriter<>(this.uniProtSolrClient, SolrCollection.help);
    }
}
