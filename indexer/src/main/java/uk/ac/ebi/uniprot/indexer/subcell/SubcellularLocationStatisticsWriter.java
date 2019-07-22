package uk.ac.ebi.uniprot.indexer.subcell;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-07-15
 */
public class SubcellularLocationStatisticsWriter implements ItemWriter<SubcellularLocationStatisticsReader.SubcellularLocationCount> {

    private StepExecution stepExecution;

    @Override
    public void write(List<? extends SubcellularLocationStatisticsReader.SubcellularLocationCount> items) throws Exception {
        if (this.stepExecution != null) {

            JobExecution jobExecution = this.stepExecution.getJobExecution();
            ExecutionContext executionContext = jobExecution.getExecutionContext();

            Map<String, SubcellularLocationStatisticsReader.SubcellularLocationCount> statisticsMap =
                    (Map<String, SubcellularLocationStatisticsReader.SubcellularLocationCount>) executionContext.get(Constants.SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY);

            if (statisticsMap == null) {
                statisticsMap = new HashMap<>();
            }

            statisticsMap.putAll(items.stream()
                    .collect(Collectors.toMap(SubcellularLocationStatisticsReader.SubcellularLocationCount::getSubcellularLocationId,
                            xrefProtCount -> xrefProtCount)));

            executionContext.put(Constants.SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY, statisticsMap);
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

}
