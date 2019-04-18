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
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyCountReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.writers.TaxonomyCountWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
@Configuration
public class TaxonomyCountStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "TaxonomyCountStep")
    public Step importTaxonomyCountStep(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                       ChunkListener chunkListener,
                                       @Qualifier("itemTaxonomyCountReader") ItemReader<TaxonomyDocument> reader,
                                       @Qualifier("itemTaxonomyCountWriter") ItemWriter<TaxonomyDocument> writer){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_COUNT_STEP_NAME)
                .<TaxonomyDocument, TaxonomyDocument>chunk(chunkSize)
                .reader(reader)
                .writer(writer)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyCountReader")
    public ItemReader<TaxonomyDocument> itemTaxonomyCountReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql("select tax_id, entry_type, count(1) as protein_count " +
                "from SPTR.dbentry " +
                "where entry_type in (0,1) and deleted ='N' and merge_status<>'R' " +
                "AND TAX_ID < 11000 " + //TODO: REMOVE WHERE < 11000
                "group by tax_id, entry_type");
        itemReader.setRowMapper(new TaxonomyCountReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyCountWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyCountWriter(SolrTemplate solrTemplate) {
        return new TaxonomyCountWriter(solrTemplate, SolrCollection.taxonomy);
    }
}

