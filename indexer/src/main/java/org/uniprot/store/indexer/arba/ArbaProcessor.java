package org.uniprot.store.indexer.arba;

import java.util.Map;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.arba.ArbaDocument;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
@Slf4j
public class ArbaProcessor implements ItemProcessor<UniRuleType, ArbaDocument> {
    private final ArbaDocumentConverter documentConverter;
    // cache from ArbaProteinCountWriter Step
    private Map<String, ArbaProteinCountReader.ArbaProteinCount> arbaProteinCountMap;

    public ArbaProcessor() {
        this.documentConverter = new ArbaDocumentConverter();
    }

    @Override
    public ArbaDocument process(UniRuleType arbaType) {
        long proteinsAnnotatedCount = getProteinsAnnotatedCount(arbaType);
        this.documentConverter.setProteinsAnnotatedCount(proteinsAnnotatedCount);
        return this.documentConverter.convert(arbaType);
    }

    @BeforeStep
    public void getStepExecution(
            final StepExecution stepExecution) { // get the cached data from previous step

        this.arbaProteinCountMap =
                (Map<String, ArbaProteinCountReader.ArbaProteinCount>)
                        stepExecution
                                .getJobExecution()
                                .getExecutionContext()
                                .get(Constants.ARBA_PROTEIN_COUNT_CACHE_KEY);
    }

    private long getProteinsAnnotatedCount(UniRuleType arbaType) {
        long totalProteinCount = 0L;
        ArbaProteinCountReader.ArbaProteinCount ruleProteinCount =
                this.arbaProteinCountMap.get(arbaType.getId());
        if (Objects.nonNull(ruleProteinCount)) {
            totalProteinCount = ruleProteinCount.getProteinCount();
        } else {
            log.warn("No protein count found for ARBA rule id {}", arbaType.getId());
        }
        return totalProteinCount;
    }
}
