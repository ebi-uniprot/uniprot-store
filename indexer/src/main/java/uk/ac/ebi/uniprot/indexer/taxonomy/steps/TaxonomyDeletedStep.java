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
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomySQLConstants;
import uk.ac.ebi.uniprot.indexer.taxonomy.processor.TaxonomyMergedDeletedProcessor;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyDeletedReader;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
@Configuration
public class TaxonomyDeletedStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyDeleted")
    public Step taxonomyDeleted(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                          ChunkListener chunkListener,
                                          ItemReader<TaxonomyEntry> itemTaxonomyDeletedReader,
                                          ItemProcessor<TaxonomyEntry,TaxonomyDocument> itemTaxonomyDeletedProcessor,
                                          ItemWriter<TaxonomyDocument> itemTaxonomyDeletedWriter) throws SQLException, IOException {
        return stepBuilders.get(Constants.TAXONOMY_LOAD_DELETED_STEP_NAME)
                .<TaxonomyEntry,TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyDeletedReader)
                .processor(itemTaxonomyDeletedProcessor)
                .writer(itemTaxonomyDeletedWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyDeletedReader")
    public ItemReader<TaxonomyEntry> itemTaxonomyDeletedReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyEntry> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(TaxonomySQLConstants.SELECT_TAXONOMY_DELETED_SQL);
        itemReader.setRowMapper(new TaxonomyDeletedReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyDeletedProcessor")
    public ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyDeletedProcessor(){
        return new TaxonomyMergedDeletedProcessor();
    }

    @Bean(name = "itemTaxonomyDeletedWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyDeletedWriter(SolrTemplate solrTemplate) {
        return new SolrDocumentWriter<>(solrTemplate, SolrCollection.taxonomy);
    }

}
