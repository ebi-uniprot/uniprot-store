package org.uniprot.store.indexer.disease;

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
import org.springframework.data.solr.core.SolrTemplate;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.keyword.KeywordSQLConstants;
import org.uniprot.store.indexer.keyword.KeywordStatisticsProcessor;
import org.uniprot.store.indexer.keyword.KeywordStatisticsReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.keyword.KeywordDocument;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author sahmad
 */
@Configuration
public class DiseaseProteinCountStep {
    private static final String QUERY_TO_GET_COUNT_PER_DISEASE = "SELECT DISEASE_IDENTIFIER as diseaseId, COUNT(ACCESSION) as proteinCount" +
            "  FROM" +
            "  (   " +
            "    SELECT DISTINCT db.ACCESSION, db.ENTRY_TYPE, TRIM(SUBSTR(css.TEXT, 0, INSTR(css.TEXT, ' (') )) DISEASE_IDENTIFIER" +
            "    FROM" +
            "      SPTR.DBENTRY db " +
            "      JOIN SPTR.COMMENT_BLOCK cb ON db.DBENTRY_ID = cb.DBENTRY_ID" +
            "      JOIN SPTR.CV_COMMENT_TOPICS ct ON ct.COMMENT_TOPICS_ID = cb.COMMENT_TOPICS_ID" +
            "      JOIN SPTR.COMMENT_STRUCTURE cs ON cb.COMMENT_BLOCK_ID = cs.COMMENT_BLOCK_ID" +
            "      JOIN SPTR.CV_CC_STRUCTURE_TYPE cst ON cs.CC_STRUCTURE_TYPE_ID = cst.CC_STRUCTURE_TYPE_ID" +
            "      JOIN SPTR.COMMENT_SUBSTRUCTURE css ON cs.COMMENT_STRUCTURE_ID = css.COMMENT_STRUCTURE_ID" +
            "    WHERE ct.TOPIC = 'DISEASE'" +
            "      AND cst.\"TYPE\" = 'DISEASE'" +
            "      AND db.ENTRY_TYPE = 0 " +
            "      AND db.DELETED = 'N'" +
            "      AND db.MERGE_STATUS <> 'R'" +
            "  )" +
            "  GROUP BY DISEASE_IDENTIFIER";

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "DiseaseProteinCountStep")
    public Step diseaseProteinCountStep(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                  ChunkListener chunkListener,
                                  ItemReader<DiseaseProteinCountReader.DiseaseProteinCount> diseaseProteinCountReader,
                                  ItemWriter<DiseaseProteinCountReader.DiseaseProteinCount> diseaseProteinCountWriter) {
        return stepBuilders.get(Constants.DISEASE_PROTEIN_COUNT_STEP)
                .<DiseaseProteinCountReader.DiseaseProteinCount, DiseaseProteinCountReader.DiseaseProteinCount>chunk(chunkSize)
                .reader(diseaseProteinCountReader)
                .writer(diseaseProteinCountWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean
    public ItemReader<DiseaseProteinCountReader.DiseaseProteinCount> diseaseProteinCountReader(@Qualifier("readDataSource") DataSource readDataSource) {
        JdbcCursorItemReader<DiseaseProteinCountReader.DiseaseProteinCount> itemReader = new JdbcCursorItemReader<>();
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
