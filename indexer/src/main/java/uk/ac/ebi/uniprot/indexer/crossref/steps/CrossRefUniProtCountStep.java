package uk.ac.ebi.uniprot.indexer.crossref.steps;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.crossref.readers.CrossRefUniProtCountReader;
import uk.ac.ebi.uniprot.indexer.crossref.writers.CrossRefUniProtCountWriter;

import javax.sql.DataSource;

/**
 * @author sahmad
 */
@Configuration
public class CrossRefUniProtCountStep {

    private static final String QUERY_TO_GET_XREF_PROTEIN_COUNT =
            "select dn.abbreviation AS abbrev, db.entry_type AS entryType, count(distinct db.accession) AS proteinCount " +
                    "from sptr.dbentry db JOIN  " +
                    "sptr.dbentry_2_database d2d ON db.dbentry_id = d2d.dbentry_id " +
                    "join sptr.database_name dn ON dn.database_id = d2d.database_id " +
                    "where " +
                    "db.entry_type in (0,1) " +
                    "and db.deleted = 'N' " +
                    "and db.merge_status <>'R' " +
                    "group by dn.abbreviation, db.entry_type ";

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "CrossRefUniProtKBCountStep")
    public Step importUniProtCountStep(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                       ChunkListener chunkListener,
                                       @Qualifier("UniProtCountReader") ItemReader<CrossRefUniProtCountReader.CrossRefProteinCount> reader,
                                       @Qualifier("UniProtCountWriter") ItemWriter<CrossRefUniProtCountReader.CrossRefProteinCount> writer,
                                       @Qualifier("crossRefPromotionListener") ExecutionContextPromotionListener promotionListener) {
        return stepBuilders.get(Constants.CROSS_REF_UNIPROT_COUNT_STEP_NAME)
                .<CrossRefUniProtCountReader.CrossRefProteinCount, CrossRefUniProtCountReader.CrossRefProteinCount>chunk(chunkSize)
                .reader(reader)
                .writer(writer)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(promotionListener)
                .build();
    }

    @Bean(name ="UniProtCountReader")
    public ItemReader<CrossRefUniProtCountReader.CrossRefProteinCount> diseaseProteinCountReader(
            @Qualifier("readDataSource") DataSource readDataSource) {

        JdbcCursorItemReader<CrossRefUniProtCountReader.CrossRefProteinCount> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(QUERY_TO_GET_XREF_PROTEIN_COUNT);
        itemReader.setRowMapper(new CrossRefUniProtCountReader());

        return itemReader;
    }

    @Bean(name = "UniProtCountWriter")
    public ItemWriter<CrossRefUniProtCountReader.CrossRefProteinCount> writer() {
        return new CrossRefUniProtCountWriter();
    }

    @Bean(name = "crossRefPromotionListener")
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(new String[]{Constants.CROSS_REF_PROTEIN_COUNT_KEY});
        return executionContextPromotionListener;
    }
}

