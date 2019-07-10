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
import uk.ac.ebi.uniprot.indexer.taxonomy.processor.TaxonomyMergedDeletedProcessor;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyMergeReader;
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
public class TaxonomyMergedStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyMerged")
    public Step taxonomyMerged(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                         ChunkListener chunkListener,
                                         ItemReader<TaxonomyEntry> itemTaxonomyMergedReader,
                                         ItemProcessor<TaxonomyEntry,TaxonomyDocument> itemTaxonomyMergedProcessor,
                                         ItemWriter<TaxonomyDocument> itemTaxonomyMergedWriter) throws SQLException, IOException {
        return stepBuilders.get(Constants.TAXONOMY_LOAD_MERGED_STEP_NAME)
                .<TaxonomyEntry,TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyMergedReader)
                .processor(itemTaxonomyMergedProcessor)
                .writer(itemTaxonomyMergedWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }


    @Bean(name = "itemTaxonomyMergedReader")
    public ItemReader<TaxonomyEntry> itemTaxonomyMergedReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyEntry> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(TaxonomySQLConstants.SELECT_TAXONOMY_MERGED_SQL);
        itemReader.setRowMapper(new TaxonomyMergeReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyMergedProcessor")
    public ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyMergedProcessor(){
        return new TaxonomyMergedDeletedProcessor();
    }

    @Bean(name = "itemTaxonomyMergedWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyMergedWriter(SolrOperations solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.taxonomy);
    }

}
