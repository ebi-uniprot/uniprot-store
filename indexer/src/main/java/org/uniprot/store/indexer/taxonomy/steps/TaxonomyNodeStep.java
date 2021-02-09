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
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.listener.SolrCommitStepListener;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.taxonomy.TaxonomySQLConstants;
import org.uniprot.store.indexer.taxonomy.processor.TaxonomyProcessor;
import org.uniprot.store.indexer.taxonomy.readers.TaxonomyNodeReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

/** @author lgonzales */
@Configuration
public class TaxonomyNodeStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyNode")
    public Step taxonomyNode(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<TaxonomyEntry> itemTaxonomyNodeReader,
            ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyNodeProcessor,
            ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter,
            UniProtSolrClient solrOperations) {
        return stepBuilders
                .get(Constants.TAXONOMY_LOAD_NODE_STEP_NAME)
                .<TaxonomyEntry, TaxonomyDocument>chunk(chunkSize)
                .reader(itemTaxonomyNodeReader)
                .processor(itemTaxonomyNodeProcessor)
                .writer(itemTaxonomyNodeWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(new SolrCommitStepListener(solrOperations, SolrCollection.taxonomy))
                .build();
    }

    @Bean(name = "itemTaxonomyNodeReader")
    public ItemReader<TaxonomyEntry> itemTaxonomyNodeReader(
            @Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyEntry> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(TaxonomySQLConstants.SELECT_TAXONOMY_NODE_SQL);
        itemReader.setRowMapper(new TaxonomyNodeReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyNodeProcessor")
    public ItemProcessor<TaxonomyEntry, TaxonomyDocument> itemTaxonomyNodeProcessor(
            @Qualifier("readDataSource") DataSource readDataSource,
            UniProtSolrClient solrOperations) {
        return new TaxonomyProcessor(readDataSource, solrOperations);
    }

    @Bean(name = "itemTaxonomyNodeWriter")
    public ItemWriter<TaxonomyDocument> itemTaxonomyNodeWriter(UniProtSolrClient solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.taxonomy);
    }
}
