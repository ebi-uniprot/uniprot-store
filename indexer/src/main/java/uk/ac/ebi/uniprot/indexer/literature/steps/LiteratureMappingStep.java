package uk.ac.ebi.uniprot.indexer.literature.steps;

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
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.domain.literature.LiteratureEntry;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.literature.processor.LiteratureMappingProcessor;
import uk.ac.ebi.uniprot.indexer.literature.reader.LiteratureMappingItemReader;
import uk.ac.ebi.uniprot.indexer.literature.reader.LiteratureMappingLineMapper;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.literature.LiteratureDocument;

/**
 * IMPORTANT: literature mapping file must be sorted by pubmed id, before start the index proccess
 * <p>
 * The command to sort is this: sort -k 3 add_bibl_info.tb > pir_map.txt
 * PIR mapping source: ftp://ftp.pir.georgetown.edu/databases/idmapping/.add_bibl_info/add_bibl_info.tb.gz
 *
 * @author lgonzales
 */
@Configuration
public class LiteratureMappingStep {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private SolrTemplate solrTemplate;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.literature.mapping.file.path}"))
    private String filePath;

    @Bean(name = "LiteratureMappingStep")
    public Step indexLiteratureMapping(StepExecutionListener stepListener, ChunkListener chunkListener,
                                       @Qualifier("LiteratureMappingReader") ItemReader<LiteratureEntry> literatureMappingReader,
                                       @Qualifier("LiteratureMappingProcessor") ItemProcessor<LiteratureEntry, LiteratureDocument> literatureMappingProcessor,
                                       @Qualifier("LiteratureMappingWriter") ItemWriter<LiteratureDocument> literatureMappingWriter) {
        return this.steps.get(Constants.LITERATURE_MAPPING_INDEX_STEP)
                .<LiteratureEntry, LiteratureDocument>chunk(chunkSize)
                .reader(literatureMappingReader)
                .processor(literatureMappingProcessor)
                .writer(literatureMappingWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "LiteratureMappingReader")
    public ItemReader<LiteratureEntry> literatureMappingReader() {
        FlatFileItemReader<LiteratureEntry> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new ClassPathResource(filePath));
        flatFileItemReader.setLineMapper(new LiteratureMappingLineMapper());

        LiteratureMappingItemReader reader = new LiteratureMappingItemReader();
        reader.setDelegate(flatFileItemReader);
        return reader;
    }

    @Bean(name = "LiteratureMappingWriter")
    public ItemWriter<LiteratureDocument> literatureMappingWriter() {
        return new SolrDocumentWriter<>(this.solrTemplate, SolrCollection.literature);
    }

    @Bean(name = "LiteratureMappingProcessor")
    public ItemProcessor<LiteratureEntry, LiteratureDocument> literatureMappingProcessor() {
        return new LiteratureMappingProcessor(this.solrTemplate);
    }

}
