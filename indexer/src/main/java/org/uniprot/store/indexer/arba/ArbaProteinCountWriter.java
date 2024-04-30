package org.uniprot.store.indexer.arba;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
 * @since 16/07/2021
 */
@Slf4j
public class ArbaProteinCountWriter implements ItemWriter<ArbaProteinCountReader.ArbaProteinCount> {

    private StepExecution stepExecution;

    @Override
    public void write(List<? extends ArbaProteinCountReader.ArbaProteinCount> items)
            throws Exception {
        if (this.stepExecution != null) {

            JobExecution jobExecution = this.stepExecution.getJobExecution();
            ExecutionContext executionContext = jobExecution.getExecutionContext();

            Map<String, ArbaProteinCountReader.ArbaProteinCount> arbaProteinCountMap =
                    (Map<String, ArbaProteinCountReader.ArbaProteinCount>)
                            executionContext.get(Constants.ARBA_PROTEIN_COUNT_CACHE_KEY);

            if (arbaProteinCountMap == null) {
                arbaProteinCountMap = new HashMap<>();
            }

            arbaProteinCountMap.putAll(
                    items.stream()
                            .collect(
                                    Collectors.toMap(
                                            ArbaProteinCountReader.ArbaProteinCount::getRuleId,
                                            Function.identity())));

            executionContext.put(Constants.ARBA_PROTEIN_COUNT_CACHE_KEY, arbaProteinCountMap);
        } else {
            log.warn("Unable to put ARBA Protein Count Map in the cache.");
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
