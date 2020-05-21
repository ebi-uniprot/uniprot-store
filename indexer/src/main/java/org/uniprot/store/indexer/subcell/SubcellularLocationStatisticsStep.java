package org.uniprot.store.indexer.subcell;

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
 * @since 2019-07-12
 */
@Configuration
public class SubcellularLocationStatisticsStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "subcellularLocationStatistics")
    public Step subcellularLocationStatistics(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<SubcellularLocationStatisticsReader.SubcellularLocationCount>
                    itemSubcellularLocationStatisticsReader,
            ItemWriter<SubcellularLocationStatisticsReader.SubcellularLocationCount>
                    itemSubcellularLocationStatisticsWriter) {
        return stepBuilders
                .get(Constants.SUBCELLULAR_LOCATION_LOAD_STATISTICS_STEP_NAME)
                .<SubcellularLocationStatisticsReader.SubcellularLocationCount,
                        SubcellularLocationStatisticsReader.SubcellularLocationCount>
                        chunk(chunkSize)
                .reader(itemSubcellularLocationStatisticsReader)
                .writer(itemSubcellularLocationStatisticsWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemSubcellularLocationStatisticsReader")
    public ItemReader<SubcellularLocationStatisticsReader.SubcellularLocationCount>
            itemSubcellularLocationStatisticsReader(
                    @Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<SubcellularLocationStatisticsReader.SubcellularLocationCount>
                itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(getStatisticsSQL());
        itemReader.setRowMapper(new SubcellularLocationStatisticsReader());

        return itemReader;
    }

    @Bean(name = "itemSubcellularLocationStatisticsWriter")
    public ItemWriter<SubcellularLocationStatisticsReader.SubcellularLocationCount>
            itemSubcellularLocationStatisticsWriter() {
        return new SubcellularLocationStatisticsWriter();
    }

    protected String getStatisticsSQL() {
        return SubcellularLocationStatisticsReader.SUBCELLULAR_LOCATION_STATISTICS_QUERY;
    }
}
