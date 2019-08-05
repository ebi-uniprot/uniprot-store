package org.uniprot.store.indexer.disease;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.utils.Constants;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author sahmad
 * put the disease protein count in a map to be used by next step
 */
public class DiseaseProteinCountWriter implements ItemWriter<DiseaseProteinCountReader.DiseaseProteinCount> {

    private StepExecution stepExecution;

    @Override
    public void write(List<? extends DiseaseProteinCountReader.DiseaseProteinCount> disProtCountList) throws Exception {

        if (this.stepExecution != null) { // null if being called from unit test

            JobExecution jobExecution = this.stepExecution.getJobExecution();
            ExecutionContext executionContext = jobExecution.getExecutionContext();

            Map<String, DiseaseProteinCountReader.DiseaseProteinCount> diseaseIdProteinCountMap;

            if (executionContext.get(Constants.DISEASE_PROTEIN_COUNT_KEY) == null) { // create a map

                diseaseIdProteinCountMap = disProtCountList.stream()
                        .collect(Collectors.toMap(disProtCount -> disProtCount.getDiseaseId(), disProtCount -> disProtCount));

            } else { // update the existing map

                diseaseIdProteinCountMap = (Map<String, DiseaseProteinCountReader.DiseaseProteinCount>)
                        executionContext.get(Constants.DISEASE_PROTEIN_COUNT_KEY);

                diseaseIdProteinCountMap.putAll(disProtCountList.stream()
                        .collect(Collectors.toMap(disProtCount -> disProtCount.getDiseaseId(), disProtCount -> disProtCount)));
            }

            executionContext.put(Constants.DISEASE_PROTEIN_COUNT_KEY, diseaseIdProteinCountMap);
        }

    }

    @BeforeStep // set the stepExecution to pass data from this step to another step. See above executionContext.put() call
    public void setStepExecution(final StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
