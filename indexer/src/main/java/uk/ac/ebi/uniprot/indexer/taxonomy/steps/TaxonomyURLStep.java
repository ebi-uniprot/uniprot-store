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
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyURLReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.writers.TaxonomyURLWriter;
import uk.ac.ebi.uniprot.search.document.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
@Configuration
public class TaxonomyURLStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "TaxonomyURLStep")
    public Step importTaxonomyURLStep(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                            ChunkListener chunkListener,
                                            @Qualifier("itemTaxonomyURLReader") ItemReader<TaxonomyDocument> reader,
                                            @Qualifier("itemTaxonomyURLWriter") ItemWriter<TaxonomyDocument> writer){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_URL_STEP_NAME)
                .<TaxonomyDocument, TaxonomyDocument>chunk(chunkSize)
                .reader(reader)
                .writer(writer)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyURLReader")
    public ItemReader<TaxonomyDocument> itemTaxonomyURLReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql("select TAX_ID, URI from TAXONOMY.V_PUBLIC_URI  where tax_id < 11000");  //TODO: REMOVE WHERE < 11000
        itemReader.setRowMapper(new TaxonomyURLReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyURLWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter(SolrTemplate solrTemplate) {
        return new TaxonomyURLWriter(solrTemplate, SolrCollection.taxonomy);
    }
}
