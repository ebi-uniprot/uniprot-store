package uk.ac.ebi.uniprot.indexer.crossref.steps;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.crossref.writers.CrossRefUniProtCountWriter;
import uk.ac.ebi.uniprot.indexer.crossref.readers.CrossRefUniProtCountReader;
import uk.ac.ebi.uniprot.search.document.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 *
 * @author sahmad
 */
@Configuration
public class CrossRefUniProtCountStep {
    private Integer chunkSize = 5; // keeping it small because the read query is very slow

    @Bean(name = "UniProtCountStep")
    public Step importUniProtCountStep(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                       ChunkListener chunkListener,
                                       @Qualifier("UniProtCountReader") ItemReader<CrossRefDocument> reader,
                                       @Qualifier("UniProtCountWriter") ItemWriter<CrossRefDocument> writer){
        return stepBuilders.get(Constants.UNIPROT_COUNT_STEP_NAME)
                .<CrossRefDocument, CrossRefDocument>chunk(chunkSize)
                .reader(reader)
                .writer(writer)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "UniProtCountReader")
    public ItemReader<CrossRefDocument> reader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        ItemReader<CrossRefDocument> reader = new CrossRefUniProtCountReader(readDataSource);
        return reader;
    }

    @Bean(name = "UniProtCountWriter")
    public ItemWriter<CrossRefDocument> writer(SolrTemplate solrTemplate) {
        return new CrossRefUniProtCountWriter(solrTemplate, SolrCollection.crossref);
    }
}

