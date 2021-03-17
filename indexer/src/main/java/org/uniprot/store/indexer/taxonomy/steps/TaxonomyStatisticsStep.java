package org.uniprot.store.indexer.taxonomy.steps;

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
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.listener.SolrCommitStepListener;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.taxonomy.TaxonomySQLConstants;
import org.uniprot.store.indexer.taxonomy.processor.TaxonomyStatisticsProcessor;
import org.uniprot.store.indexer.taxonomy.readers.TaxonomyStatisticsReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

/** @author lgonzales */
@Configuration
public class TaxonomyStatisticsStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyStatistics")
    public Step taxonomyStatistics(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<TaxonomyStatisticsReader.TaxonomyCount> itemTaxonomyStatisticsReader,
            ItemProcessor<TaxonomyStatisticsReader.TaxonomyCount, TaxonomyDocument>
                    itemTaxonomyStatisticsProcessor,
            ItemWriter<TaxonomyDocument> itemTaxonomyStatisticsWriter,
            UniProtSolrClient solrOperations) {
        return stepBuilders
                .get(Constants.TAXONOMY_LOAD_STATISTICS_STEP_NAME)
                .<TaxonomyStatisticsReader.TaxonomyCount, TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyStatisticsReader)
                .processor(itemTaxonomyStatisticsProcessor)
                .writer(itemTaxonomyStatisticsWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new SolrCommitStepListener(solrOperations, SolrCollection.taxonomy))
                .build();
    }

    @Bean(name = "itemTaxonomyStatisticsReader")
    public ItemReader<TaxonomyStatisticsReader.TaxonomyCount> itemTaxonomyStatisticsReader(
            @Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyStatisticsReader.TaxonomyCount> itemReader =
                new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(getStatisticsSQL());
        itemReader.setRowMapper(new TaxonomyStatisticsReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyStatisticsProcessor")
    public ItemProcessor<TaxonomyStatisticsReader.TaxonomyCount, TaxonomyDocument>
            itemTaxonomyStatisticsProcessor() {
        return new TaxonomyStatisticsProcessor();
    }

    @Bean(name = "itemTaxonomyStatisticsWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyStatisticsWriter(
            UniProtSolrClient solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.taxonomy);
    }

    protected String getStatisticsSQL() {
        return TaxonomySQLConstants.COUNT_PROTEINS_SQL;
    }
}
