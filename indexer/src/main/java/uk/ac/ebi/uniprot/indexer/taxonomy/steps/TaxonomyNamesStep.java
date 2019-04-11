package uk.ac.ebi.uniprot.indexer.taxonomy.steps;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;

import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyNamesReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.writers.TaxonomyNamesWriter;
import uk.ac.ebi.uniprot.search.document.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
@Configuration
public class TaxonomyNamesStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "TaxonomyNamesStep")
    public Step importTaxonomyNamesStep(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                         ChunkListener chunkListener,
                                         @Qualifier("itemTaxonomyNamesReader") ItemReader<TaxonomyDocument> reader,
                                         @Qualifier("itemTaxonomyNamesWriter") ItemWriter<TaxonomyDocument> writer){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_NAMES_STEP_NAME)
                .<TaxonomyDocument, TaxonomyDocument>chunk(chunkSize)
                .reader(reader)
                .writer(writer)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyNamesReader")
    public ItemReader<TaxonomyDocument> itemTaxonomyNamesReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(""); //TODO add the query to load names
        itemReader.setRowMapper(new TaxonomyNamesReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyNamesWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter(SolrTemplate solrTemplate) {
        return new TaxonomyNamesWriter(solrTemplate, SolrCollection.taxonomy);
    }

}
