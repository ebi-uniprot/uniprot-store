package org.uniprot.store.indexer.unirule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * @author sahmad
 * @created 21 May 2020
 */
@Slf4j
public class UniRuleProteinCountWriter
        implements ItemWriter<UniRuleProteinCountReader.UniRuleProteinCount> {

    private StepExecution stepExecution;

    @Override
    public void write(List<? extends UniRuleProteinCountReader.UniRuleProteinCount> items)
            throws Exception {
        if (this.stepExecution != null) {

            JobExecution jobExecution = this.stepExecution.getJobExecution();
            ExecutionContext executionContext = jobExecution.getExecutionContext();

            Map<String, UniRuleProteinCountReader.UniRuleProteinCount> uniRuleProteinCountMap =
                    (Map<String, UniRuleProteinCountReader.UniRuleProteinCount>)
                            executionContext.get(Constants.UNIRULE_PROTEIN_COUNT_CACHE_KEY);

            if (uniRuleProteinCountMap == null) {
                uniRuleProteinCountMap = new HashMap<>();
            }

            uniRuleProteinCountMap.putAll(
                    items.stream()
                            .collect(
                                    Collectors.toMap(
                                            UniRuleProteinCountReader.UniRuleProteinCount
                                                    ::getOldRuleId,
                                            Function.identity())));

            executionContext.put(Constants.UNIRULE_PROTEIN_COUNT_CACHE_KEY, uniRuleProteinCountMap);
        } else {
            log.warn("Unable to put UniRule Protein Count Map in the cache.");
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
