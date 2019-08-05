package org.uniprot.store.indexer.literature.steps;

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
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.literature.LiteratureSQLConstants;
import org.uniprot.store.indexer.literature.processor.LiteratureStatisticsProcessor;
import org.uniprot.store.indexer.literature.reader.LiteratureStatisticsReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author lgonzales
 */
@Configuration
public class LiteratureStatisticsStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "LiteratureStatistics")
    public Step literatureStatistics(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                     ChunkListener chunkListener,
                                     ItemReader<LiteratureStatisticsReader.LiteratureCount> itemLiteratureStatisticsReader,
                                     ItemProcessor<LiteratureStatisticsReader.LiteratureCount, LiteratureDocument> itemLiteratureStatisticsProcessor,
                                     ItemWriter<LiteratureDocument> itemLiteratureStatisticsWriter) {
        return stepBuilders.get(Constants.LITERATURE_LOAD_STATISTICS_STEP_NAME)
                .<LiteratureStatisticsReader.LiteratureCount, LiteratureDocument>chunk(chunkSize)
                .reader(itemLiteratureStatisticsReader)
                .processor(itemLiteratureStatisticsProcessor)
                .writer(itemLiteratureStatisticsWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemLiteratureStatisticsReader")
    public ItemReader<LiteratureStatisticsReader.LiteratureCount> itemLiteratureStatisticsReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<LiteratureStatisticsReader.LiteratureCount> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(getStatisticsSQL());
        itemReader.setRowMapper(new LiteratureStatisticsReader());

        return itemReader;
    }

    @Bean(name = "itemLiteratureStatisticsProcessor")
    public ItemProcessor<LiteratureStatisticsReader.LiteratureCount, LiteratureDocument> itemLiteratureStatisticsProcessor() {
        return new LiteratureStatisticsProcessor();
    }

    @Bean(name = "itemLiteratureStatisticsWriter")
    public ItemWriter<LiteratureDocument> itemLiteratureStatisticsWriter(UniProtSolrOperations solrOperations) {
        return new SolrDocumentWriter<>(solrOperations, SolrCollection.literature);
    }

    protected String getStatisticsSQL() {
        return LiteratureSQLConstants.LITERATURE_STATISTICS_SQL;
    }

}
