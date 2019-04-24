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
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyVirusHostReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.writers.TaxonomyVirusHostWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
@Configuration
public class TaxonomyVirusHostStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyVirusHost")
    public Step taxonomyVirusHost(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                            ChunkListener chunkListener,
                                            ItemReader<TaxonomyDocument> itemTaxonomyVirusHostReader,
                                            ItemWriter<TaxonomyDocument> itemTaxonomyVirusHostWriter){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_VIRUS_HOST_STEP_NAME)
                .<TaxonomyDocument, TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyVirusHostReader)
                .writer(itemTaxonomyVirusHostWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyVirusHostReader")
    public ItemReader<TaxonomyDocument> itemTaxonomyVirusHostReader(DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql("select TAX_ID, HOST_ID from TAXONOMY.V_PUBLIC_HOST where tax_id < 11000"); //TODO: REMOVE WHERE < 11000
        itemReader.setRowMapper(new TaxonomyVirusHostReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyVirusHostWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter(SolrTemplate solrTemplate) {
        return new TaxonomyVirusHostWriter(solrTemplate, SolrCollection.taxonomy);
    }
}
