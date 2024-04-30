package org.uniprot.store.indexer.disease;

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
 */
@Configuration
public class DiseaseProteinCountStep {
    private static final String QUERY_TO_GET_COUNT_PER_DISEASE =
            "SELECT ID as diseaseId, REVIEWED_PROTEIN_COUNT as proteinCount "
                    + "FROM SPTR.MV_DATA_SOURCE_STATS WHERE DATA_TYPE = 'Disease'";

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "DiseaseProteinCountStep")
    public Step diseaseProteinCountStep(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<DiseaseProteinCountReader.DiseaseProteinCount> diseaseProteinCountReader,
            ItemWriter<DiseaseProteinCountReader.DiseaseProteinCount> diseaseProteinCountWriter) {
        return stepBuilders
                .get(Constants.DISEASE_PROTEIN_COUNT_STEP)
                .<DiseaseProteinCountReader.DiseaseProteinCount,
                        DiseaseProteinCountReader.DiseaseProteinCount>
                        chunk(chunkSize)
                .reader(diseaseProteinCountReader)
                .writer(diseaseProteinCountWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean
    public ItemReader<DiseaseProteinCountReader.DiseaseProteinCount> diseaseProteinCountReader(
            @Qualifier("readDataSource") DataSource readDataSource) {
        JdbcCursorItemReader<DiseaseProteinCountReader.DiseaseProteinCount> itemReader =
                new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(QUERY_TO_GET_COUNT_PER_DISEASE);
        itemReader.setRowMapper(new DiseaseProteinCountReader());

        return itemReader;
    }

    @Bean
    public ItemWriter<DiseaseProteinCountReader.DiseaseProteinCount> diseaseProteinCountWriter() {
        return new DiseaseProteinCountWriter();
    }
}
