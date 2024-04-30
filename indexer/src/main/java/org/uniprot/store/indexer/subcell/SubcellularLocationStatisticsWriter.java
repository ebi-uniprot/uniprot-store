package org.uniprot.store.indexer.subcell;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.utils.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 2019-07-15
 */
@Slf4j
public class SubcellularLocationStatisticsWriter
        implements ItemWriter<SubcellularLocationStatisticsReader.SubcellularLocationCount> {

    private StepExecution stepExecution;

    @Override
    public void write(
            List<? extends SubcellularLocationStatisticsReader.SubcellularLocationCount> items)
            throws Exception {
        if (this.stepExecution != null) {

            JobExecution jobExecution = this.stepExecution.getJobExecution();
            ExecutionContext executionContext = jobExecution.getExecutionContext();

            Map<String, SubcellularLocationStatisticsReader.SubcellularLocationCount>
                    statisticsMap =
                            (Map<
                                            String,
                                            SubcellularLocationStatisticsReader
                                                    .SubcellularLocationCount>)
                                    executionContext.get(
                                            Constants.SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY);

            if (statisticsMap == null) {
                statisticsMap = new HashMap<>();
            }

            statisticsMap.putAll(
                    items.stream()
                            .collect(
                                    Collectors.toMap(
                                            SubcellularLocationStatisticsReader
                                                            .SubcellularLocationCount
                                                    ::getSubcellularLocationId,
                                            xrefProtCount -> xrefProtCount)));

            executionContext.put(Constants.SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY, statisticsMap);
        } else {
            log.warn(
                    "IMPORTANT: SubcellularLocationStatisticsWriter.stepExecution is null, unable to write statistics");
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        log.info("setStepExecution before step execution");
        this.stepExecution = stepExecution;
    }
}
