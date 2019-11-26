package org.uniprot.store.indexer.taxonomy.steps;

import java.io.IOException;
import java.sql.SQLException;

import javax.sql.DataSource;

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
import org.uniprot.store.indexer.taxonomy.readers.TaxonomyDeletedReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

/** @author lgonzales */
@Configuration
public class TaxonomyDeletedStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyDeleted")
    public Step taxonomyDeleted(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<TaxonomyEntry> itemTaxonomyDeletedReader,
            ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyDeletedProcessor,
            ItemWriter<TaxonomyDocument> itemTaxonomyDeletedWriter,
            UniProtSolrOperations solrOperations)
            throws SQLException, IOException {
        return stepBuilders
                .get(Constants.TAXONOMY_LOAD_DELETED_STEP_NAME)
                .<TaxonomyEntry, TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyDeletedReader)
                .processor(itemTaxonomyDeletedProcessor)
                .writer(itemTaxonomyDeletedWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new SolrCommitStepListener(solrOperations))
                .build();
    }

    @Bean(name = "itemTaxonomyDeletedReader")
    public ItemReader<TaxonomyEntry> itemTaxonomyDeletedReader(
            @Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyEntry> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(TaxonomySQLConstants.SELECT_TAXONOMY_DELETED_SQL);
        itemReader.setRowMapper(new TaxonomyDeletedReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyDeletedProcessor")
    public ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyDeletedProcessor() {
        return new TaxonomyMergedDeletedProcessor();
    }

    @Bean(name = "itemTaxonomyDeletedWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyDeletedWriter(
            UniProtSolrOperations solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.taxonomy);
    }
}
