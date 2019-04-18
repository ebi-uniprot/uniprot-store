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
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyStrainReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.writers.TaxonomyStrainWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
@Configuration
public class TaxonomyStrainStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "TaxonomyStrainStep")
    public Step importTaxonomyStrainStep(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                         ChunkListener chunkListener,
                                         @Qualifier("itemTaxonomyStrainReader") ItemReader<TaxonomyDocument> reader,
                                         @Qualifier("itemTaxonomyStrainWriter") ItemWriter<TaxonomyDocument> writer){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_STRAIN_STEP_NAME)
                .<TaxonomyDocument, TaxonomyDocument>chunk(chunkSize)
                .reader(reader)
                .writer(writer)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyStrainReader")
    public ItemReader<TaxonomyDocument> itemTaxonomyStrainReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql("SELECT s.tax_id," +
                "       sci.NAME as scientific_name," +
                "       syn.NAME as synonym_name " +
                "FROM" +
                "  TAXONOMY.tax_public p" +
                "    INNER JOIN TAXONOMY.sptr_strain s" +
                "      ON s.tax_id = p.tax_id" +
                "    LEFT JOIN (SELECT STRAIN_ID, NAME from TAXONOMY.SPTR_STRAIN_NAME where NAME_CLASS = 'synonym') syn" +
                "      ON syn.strain_id = s.strain_id" +
                "    LEFT JOIN (SELECT STRAIN_ID, NAME from TAXONOMY.SPTR_STRAIN_NAME where NAME_CLASS = 'scientific name') sci" +
                "      ON sci.strain_id = s.strain_id where p.tax_id < 11000");  //TODO: REMOVE WHERE < 11000
        itemReader.setRowMapper(new TaxonomyStrainReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyStrainWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter(SolrTemplate solrTemplate) {
        return new TaxonomyStrainWriter(solrTemplate, SolrCollection.taxonomy);
    }
}
