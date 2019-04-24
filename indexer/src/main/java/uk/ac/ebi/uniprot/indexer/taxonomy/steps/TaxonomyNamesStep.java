package uk.ac.ebi.uniprot.indexer.taxonomy.steps;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyNamesReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.writers.TaxonomyNamesWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
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

    @Bean(name = "taxonomyNames")
    public Step taxonomyNames(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                         ChunkListener chunkListener,
                                         ItemReader<TaxonomyDocument> itemTaxonomyNamesReader,
                                         ItemWriter<TaxonomyDocument> itemTaxonomyNamesWriter){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_NAMES_STEP_NAME)
                .<TaxonomyDocument, TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyNamesReader)
                .writer(itemTaxonomyNamesWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyNamesReader")
    public ItemReader<TaxonomyDocument> itemTaxonomyNamesReader(DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql("select * from taxonomy.V_PUBLIC_NAME nm inner join TAXONOMY.V_PUBLIC_NODE nd  on nm.TAX_ID = nd.TAX_ID" +
                " where nm.PRIORITY > 0 AND" +
                " (UPPER(nm.NAME) <> UPPER(nd.SPTR_COMMON) OR nd.SPTR_COMMON is null) AND" +
                " (UPPER(nm.NAME) <> UPPER(nd.SPTR_SCIENTIFIC) OR nd.SPTR_SCIENTIFIC is null) AND" +
                " (UPPER(nm.NAME) <> UPPER(nd.SPTR_SYNONYM) OR nd.SPTR_SYNONYM is null) AND" +
                " (UPPER(nm.NAME) <> UPPER(nd.TAX_CODE) OR nd.TAX_CODE is null)" +
                " AND nm.TAX_ID < 11000"); //TODO: REMOVE WHERE < 11000;
        itemReader.setRowMapper(new TaxonomyNamesReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyNamesWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyNamesWriter(SolrTemplate solrTemplate) {
        return new TaxonomyNamesWriter(solrTemplate, SolrCollection.taxonomy);
    }

}
