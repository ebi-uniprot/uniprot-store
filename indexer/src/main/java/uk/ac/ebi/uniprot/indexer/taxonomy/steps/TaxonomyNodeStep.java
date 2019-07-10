package uk.ac.ebi.uniprot.indexer.taxonomy.steps;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrOperations;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomySQLConstants;
import uk.ac.ebi.uniprot.indexer.taxonomy.processor.TaxonomyProcessor;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyNodeReader;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author lgonzales
 */
@Configuration
public class TaxonomyNodeStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyNode")
    public Step taxonomyNode(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                             ChunkListener chunkListener,
                             ItemReader<TaxonomyEntry> itemTaxonomyNodeReader,
                             ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyNodeProcessor,
                             ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter) {
        return stepBuilders.get(Constants.TAXONOMY_LOAD_NODE_STEP_NAME)
                .<TaxonomyEntry, TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyNodeReader)
                .processor(itemTaxonomyNodeProcessor)
                .writer(itemTaxonomyNodeWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyNodeReader")
    public ItemReader<TaxonomyEntry> itemTaxonomyNodeReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyEntry> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(TaxonomySQLConstants.SELECT_TAXONOMY_NODE_SQL);
        itemReader.setRowMapper(new TaxonomyNodeReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyNodeProcessor")
    public ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyNodeProcessor(@Qualifier("readDataSource") DataSource readDataSource, SolrOperations solrOperations) {
        return new TaxonomyProcessor(readDataSource, solrOperations);
    }

    @Bean(name = "itemTaxonomyNodeWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter(SolrOperations solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.taxonomy);
    }
}
