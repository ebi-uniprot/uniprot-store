package org.uniprot.store.indexer.literature.steps;

import java.io.IOException;

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
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.listener.SolrCommitStepListener;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.literature.processor.LiteratureMappingProcessor;
import org.uniprot.store.indexer.literature.reader.LiteratureMappingItemReader;
import org.uniprot.store.indexer.literature.reader.LiteratureMappingLineMapper;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * IMPORTANT: literature mapping file must be sorted by pubmed id, before start the index proccess
 *
 * <p>The command to sort is this: sort -k 3 add_bibl_info.tb > pir_map.txt PIR mapping source:
 * ftp://ftp.pir.georgetown.edu/databases/idmapping/.add_bibl_info/add_bibl_info.tb.gz
 *
 * @author lgonzales
 */
@Configuration
public class LiteratureMappingStep {

    @Autowired private StepBuilderFactory steps;

    @Autowired private UniProtSolrClient uniProtSolrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.literature.mapping.file.path}"))
    private Resource literatureMappingFile;

    @Bean(name = "LiteratureMappingStep")
    public Step indexLiteratureMapping(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("LiteratureMappingReader")
                    ItemReader<LiteratureEntry> literatureMappingReader,
            @Qualifier("LiteratureMappingProcessor")
                    ItemProcessor<LiteratureEntry, LiteratureDocument> literatureMappingProcessor,
            @Qualifier("LiteratureMappingWriter")
                    ItemWriter<LiteratureDocument> literatureMappingWriter,
            UniProtSolrClient solrOperations) {
        return this.steps
                .get(Constants.LITERATURE_MAPPING_INDEX_STEP)
                .<LiteratureEntry, LiteratureDocument>chunk(chunkSize)
                .reader(literatureMappingReader)
                .processor(literatureMappingProcessor)
                .writer(literatureMappingWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new SolrCommitStepListener(solrOperations, SolrCollection.literature))
                .build();
    }

    @Bean(name = "LiteratureMappingReader")
    public ItemReader<LiteratureEntry> literatureMappingReader() throws IOException {
        FlatFileItemReader<LiteratureEntry> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(literatureMappingFile);
        flatFileItemReader.setLineMapper(new LiteratureMappingLineMapper());

        LiteratureMappingItemReader reader = new LiteratureMappingItemReader();
        reader.setDelegate(flatFileItemReader);
        return reader;
    }

    @Bean(name = "LiteratureMappingWriter")
    public ItemWriter<LiteratureDocument> literatureMappingWriter() {
        return new SolrDocumentWriter<>(this.uniProtSolrClient, SolrCollection.literature);
    }

    @Bean(name = "LiteratureMappingProcessor")
    public ItemProcessor<LiteratureEntry, LiteratureDocument> literatureMappingProcessor() {
        return new LiteratureMappingProcessor(this.uniProtSolrClient);
    }
}
