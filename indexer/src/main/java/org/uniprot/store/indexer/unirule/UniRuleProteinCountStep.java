package org.uniprot.store.indexer.unirule;

import java.sql.SQLException;

import javax.sql.DataSource;

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
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * @author sahmad
 * @created 21 May 2020
 */
@Configuration
public class UniRuleProteinCountStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean
    public Step uniRuleProteinCountSQLStep(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<UniRuleProteinCountReader.UniRuleProteinCount> uniRuleProteinCountReader,
            ItemWriter<UniRuleProteinCountReader.UniRuleProteinCount> uniRuleProteinCountWriter) {
        return stepBuilders
                .get(Constants.UNIRULE_PROTEIN_COUNT_STEP)
                .<UniRuleProteinCountReader.UniRuleProteinCount,
                        UniRuleProteinCountReader.UniRuleProteinCount>
                        chunk(chunkSize)
                .reader(uniRuleProteinCountReader)
                .writer(uniRuleProteinCountWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean
    public ItemReader<UniRuleProteinCountReader.UniRuleProteinCount> uniRuleProteinCountReader(
            @Qualifier("readDataSource") DataSource readDataSource) throws SQLException {

        JdbcCursorItemReader<UniRuleProteinCountReader.UniRuleProteinCount> itemReader =
                new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(getStatisticsSQL());
        itemReader.setRowMapper(new UniRuleProteinCountReader());

        return itemReader;
    }

    @Bean
    public ItemWriter<UniRuleProteinCountReader.UniRuleProteinCount> uniRuleProteinCountWriter() {
        return new UniRuleProteinCountWriter();
    }

    protected String getStatisticsSQL() {
        return UniRuleProteinCountReader.UNIRULE_PROTEIN_COUNT_QUERY;
    }
}
