package uk.ac.ebi.uniprot.indexer.taxonomy.steps;

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
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomyEntryWriter;
import uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomySQLConstants;
import uk.ac.ebi.uniprot.indexer.taxonomy.processor.TaxonomyStatisticsProcessor;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyStatisticsReader;
import uk.ac.ebi.uniprot.search.SolrCollection;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
@Configuration
public class TaxonomyStatisticsStep {

    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean(name = "taxonomyStatistics")
    public Step taxonomyStatistics(StepBuilderFactory stepBuilders, StepExecutionListener stepListener,
                                       ChunkListener chunkListener,
                                       ItemReader<TaxonomyStatisticsReader.TaxonomyCount> itemTaxonomyStatisticsReader,
                                       ItemProcessor<TaxonomyStatisticsReader.TaxonomyCount,TaxonomyEntry> itemTaxonomyStatisticsProcessor,
                                       ItemWriter<TaxonomyEntry> itemTaxonomyStatisticsWriter){
        return stepBuilders.get(Constants.TAXONOMY_LOAD_STATISTICS_STEP_NAME)
                .<TaxonomyStatisticsReader.TaxonomyCount, TaxonomyEntry>chunk(chunkSize)
                .reader(itemTaxonomyStatisticsReader)
                .processor(itemTaxonomyStatisticsProcessor)
                .writer(itemTaxonomyStatisticsWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "itemTaxonomyStatisticsReader")
    public ItemReader<TaxonomyStatisticsReader.TaxonomyCount> itemTaxonomyStatisticsReader(@Qualifier("readDataSource") DataSource readDataSource) throws SQLException {
        JdbcCursorItemReader<TaxonomyStatisticsReader.TaxonomyCount> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(readDataSource);
        itemReader.setSql(getStatisticsSQL());
        itemReader.setRowMapper(new TaxonomyStatisticsReader());

        return itemReader;
    }

    @Bean(name = "itemTaxonomyStatisticsProcessor")
    public ItemProcessor<TaxonomyStatisticsReader.TaxonomyCount,TaxonomyEntry> itemTaxonomyStatisticsProcessor(SolrTemplate solrTemplate) {
        return new TaxonomyStatisticsProcessor(solrTemplate);
    }

    @Bean(name = "itemTaxonomyStatisticsWriter")
    public ItemWriter<TaxonomyEntry> itemTaxonomyStatisticsWriter(SolrTemplate solrTemplate) {
        return new TaxonomyEntryWriter(solrTemplate, SolrCollection.taxonomy);
    }

    protected String getStatisticsSQL(){
        return TaxonomySQLConstants.COUNT_PROTEINS_SQL;
    }
}

