package org.uniprot.store.indexer.arba;

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
 * @author lgonzales
 * @since 16/07/2021
 */
@Configuration
public class ArbaProteinCountStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean
    public Step arbaProteinCountSQLStep(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<ArbaProteinCountReader.ArbaProteinCount> arbaProteinCountReader,
            ItemWriter<ArbaProteinCountReader.ArbaProteinCount> arbaProteinCountWriter) {
        return stepBuilders
                .get(Constants.ARBA_PROTEIN_COUNT_STEP)
                .<ArbaProteinCountReader.ArbaProteinCount, ArbaProteinCountReader.ArbaProteinCount>
                        chunk(chunkSize)
                .reader(arbaProteinCountReader)
                .writer(arbaProteinCountWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean
    public ItemReader<ArbaProteinCountReader.ArbaProteinCount> arbaProteinCountReader(
            @Qualifier("readDataSource") DataSource readDataSource) throws SQLException {

        JdbcCursorItemReader<ArbaProteinCountReader.ArbaProteinCount> itemReader =
                new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(getStatisticsSQL());
        itemReader.setRowMapper(new ArbaProteinCountReader());

        return itemReader;
    }

    @Bean
    public ItemWriter<ArbaProteinCountReader.ArbaProteinCount> arbaProteinCountWriter() {
        return new ArbaProteinCountWriter();
    }

    protected String getStatisticsSQL() {
        return ArbaProteinCountReader.ARBA_PROTEIN_COUNT_QUERY;
    }
}
