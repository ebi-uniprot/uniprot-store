package org.uniprot.store.indexer.literature.steps;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.indexer.common.config.FlatFileRecordSeparatorPolicy;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.listener.SolrCommitStepListener;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.literature.processor.LiteratureLoadProcessor;
import org.uniprot.store.indexer.literature.reader.LiteratureLineMapper;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * @author lgonzales
 */
@Configuration
public class LiteratureLoadStep {

    @Autowired private StepBuilderFactory steps;

    @Autowired private UniProtSolrClient uniProtSolrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.literature.file.path}"))
    private Resource literatureFile;

    @Bean(name = "IndexLiteratureStep")
    public Step indexLiterature(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("LiteratureReader") ItemReader<LiteratureEntry> literatureReader,
            @Qualifier("LiteratureProcessor")
                    ItemProcessor<LiteratureEntry, LiteratureDocument> literatureProcessor,
            @Qualifier("LiteratureWriter") ItemWriter<LiteratureDocument> literatureWriter,
            UniProtSolrClient solrOperations) {
        return this.steps
                .get(Constants.LITERATURE_INDEX_STEP)
                .<LiteratureEntry, LiteratureDocument>chunk(this.chunkSize)
                .reader(literatureReader)
                .processor(literatureProcessor)
                .writer(literatureWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new SolrCommitStepListener(solrOperations, SolrCollection.literature))
                .build();
    }

    @Bean(name = "LiteratureReader")
    public FlatFileItemReader<LiteratureEntry> literatureReader() {
        FlatFileItemReader<LiteratureEntry> reader = new FlatFileItemReader<>();
        reader.setResource(literatureFile);
        reader.setLinesToSkip(1);
        reader.setLineMapper(getLiteratureLineMapper());
        reader.setRecordSeparatorPolicy(getLiteratureRecordSeparatorPolice());

        return reader;
    }

    @Bean(name = "LiteratureWriter")
    public ItemWriter<LiteratureDocument> literatureWriter() {
        return new SolrDocumentWriter<>(this.uniProtSolrClient, SolrCollection.literature);
    }

    @Bean(name = "LiteratureProcessor")
    public ItemProcessor<LiteratureEntry, LiteratureDocument> literatureProcessor() {
        return new LiteratureLoadProcessor(uniProtSolrClient);
    }

    private FlatFileRecordSeparatorPolicy getLiteratureRecordSeparatorPolice() {
        FlatFileRecordSeparatorPolicy policy = new FlatFileRecordSeparatorPolicy();
        policy.setSuffix("\n//");
        policy.setIgnoreWhitespace(true);
        return policy;
    }

    private LiteratureLineMapper getLiteratureLineMapper() {
        return new LiteratureLineMapper();
    }
}
