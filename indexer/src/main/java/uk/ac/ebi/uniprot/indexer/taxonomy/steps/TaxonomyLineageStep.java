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
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyLineageReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.writers.TaxonomyLineageWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
public class TaxonomyLineageStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyLineage")
    public Step taxonomyLineage(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                              ChunkListener chunkListener,
                              ItemReader<TaxonomyDocument> itemTaxonomyLineageReader,
                              ItemWriter<TaxonomyDocument> itemTaxonomyLineageWriter){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_LINEAGE_STEP_NAME)
                .<TaxonomyDocument, TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyLineageReader)
                .writer(itemTaxonomyLineageWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyLineageReader")
    public ItemReader<TaxonomyDocument> itemTaxonomyLineageReader(DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql("select SYS_CONNECT_BY_PATH(TAX_ID, '|') AS lineage_id," +
                "       SYS_CONNECT_BY_PATH(SPTR_SCIENTIFIC, '|') AS lineage_name," +
                "       SYS_CONNECT_BY_PATH(RANK, '|') AS lineage_rank," +
                "       SYS_CONNECT_BY_PATH(HIDDEN, '|') AS lineage_rank" +
                " from taxonomy.V_PUBLIC_NODE" +
                " WHERE TAX_ID = 1" +
                " START WITH TAX_ID < 11000" + //REMOVE WHERE < 11000
                " CONNECT BY PRIOR PARENT_ID = TAX_ID");
        itemReader.setRowMapper(new TaxonomyLineageReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyLineageWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyLineageWriter(SolrTemplate solrTemplate) {
        return new TaxonomyLineageWriter(solrTemplate, SolrCollection.taxonomy);
    }

}
