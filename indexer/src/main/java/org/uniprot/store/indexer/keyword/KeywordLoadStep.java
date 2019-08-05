package org.uniprot.store.indexer.keyword;


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
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.keyword.KeywordDocument;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author lgonzales
 */
@Configuration
public class KeywordLoadStep {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private UniProtSolrOperations solrOperations;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.keyword.file.path}"))
    private String filePath;

    @Bean(name = "IndexKeywordStep")
    public Step indexKeyword(StepExecutionListener stepListener, ChunkListener chunkListener,
                             @Qualifier("KeywordReader") ItemReader<KeywordEntry> keywordReader,
                             @Qualifier("KeywordProcessor") ItemProcessor<KeywordEntry, KeywordDocument> keywordProcessor,
                             @Qualifier("KeywordWriter") ItemWriter<KeywordDocument> keywordWriter) {
        return this.steps.get(Constants.KEYWORD_INDEX_STEP)
                .<KeywordEntry, KeywordDocument>chunk(this.chunkSize)
                .reader(keywordReader)
                .processor(keywordProcessor)
                .writer(keywordWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "KeywordReader")
    public ItemReader<KeywordEntry> keywordReader() throws IOException {
        return new KeywordLoadItemReader(this.filePath);
    }

    @Bean(name = "KeywordWriter")
    public ItemWriter<KeywordDocument> keywordWriter() {
        return new SolrDocumentWriter<>(this.solrOperations, SolrCollection.keyword);
    }

    @Bean(name = "KeywordProcessor")
    public ItemProcessor<KeywordEntry, KeywordDocument> keywordProcessor() throws SQLException {
        return new KeywordLoadProcessor(this.solrOperations);
    }

}
