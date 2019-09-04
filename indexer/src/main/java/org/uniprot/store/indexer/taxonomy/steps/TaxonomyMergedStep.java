package org.uniprot.store.indexer.taxonomy.steps;

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
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.listener.SolrCommitStepListener;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.taxonomy.TaxonomySQLConstants;
import org.uniprot.store.indexer.taxonomy.processor.TaxonomyMergedDeletedProcessor;
import org.uniprot.store.indexer.taxonomy.readers.TaxonomyMergeReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

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
                                         ItemWriter<TaxonomyDocument> itemTaxonomyMergedWriter,
                                         UniProtSolrOperations solrOperations) throws SQLException, IOException {
        return stepBuilders.get(Constants.TAXONOMY_LOAD_MERGED_STEP_NAME)
                .<TaxonomyEntry,TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyMergedReader)
                .processor(itemTaxonomyMergedProcessor)
                .writer(itemTaxonomyMergedWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new SolrCommitStepListener(solrOperations))
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
    public ItemWriter<TaxonomyDocument> itemTaxonomyMergedWriter(UniProtSolrOperations solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.taxonomy);
    }

}
