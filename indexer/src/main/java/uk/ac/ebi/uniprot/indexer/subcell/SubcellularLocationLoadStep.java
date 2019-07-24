package uk.ac.ebi.uniprot.indexer.subcell;

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
import uk.ac.ebi.uniprot.cv.subcell.SubcellularLocationEntry;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.subcell.SubcellularLocationDocument;

import java.io.IOException;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
@Configuration
public class SubcellularLocationLoadStep {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private UniProtSolrOperations solrOperations;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.subcellularLocation.file.path}"))
    private String filePath;

    @Bean(name = "IndexSubcellularLocationStep")
    public Step indexSubcellularLocation(StepExecutionListener stepListener, ChunkListener chunkListener,
                                         @Qualifier("SubcellularLocationReader") ItemReader<SubcellularLocationEntry> subcellularLocationReader,
                                         @Qualifier("SubcellularLocationProcessor") ItemProcessor<SubcellularLocationEntry, SubcellularLocationDocument> subcellularLocationProcessor,
                                         @Qualifier("SubcellularLocationWriter") ItemWriter<SubcellularLocationDocument> subcellularLocationWriter) {
        return this.steps.get(Constants.SUBCELLULAR_LOCATION_INDEX_STEP)
                .<SubcellularLocationEntry, SubcellularLocationDocument>chunk(this.chunkSize)
                .reader(subcellularLocationReader)
                .processor(subcellularLocationProcessor)
                .writer(subcellularLocationWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "SubcellularLocationReader")
    public ItemReader<SubcellularLocationEntry> subcellularLocationReader() throws IOException {
        return new SubcellularLocationLoadItemReader(this.filePath);
    }

    @Bean(name = "SubcellularLocationWriter")
    public ItemWriter<SubcellularLocationDocument> subcellularLocationWriter() {
        return new SolrDocumentWriter<>(this.solrOperations, SolrCollection.subcellularlocation);
    }

    @Bean(name = "SubcellularLocationProcessor")
    public ItemProcessor<SubcellularLocationEntry, SubcellularLocationDocument> subcellularLocationProcessor() {
        return new SubcellularLocationLoadProcessor();
    }

}
