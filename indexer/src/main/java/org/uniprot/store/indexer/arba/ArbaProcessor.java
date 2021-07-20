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
        long proteinsAnnotatedCount = computeProteinsAnnotatedCount(arbaType);
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
                                .get(Constants.UNIRULE_PROTEIN_COUNT_CACHE_KEY);
    }

    private long computeProteinsAnnotatedCount(UniRuleType arbaType) {
        long totalProteinCount = 0L;
        if (Objects.nonNull(arbaType.getInformation())
                && Objects.nonNull(arbaType.getInformation().getOldRuleNum())) {
            String oldRuleId = arbaType.getInformation().getOldRuleNum();
            ArbaProteinCountReader.ArbaProteinCount ruleProteinCount =
                    this.arbaProteinCountMap.get(oldRuleId);
            if (ruleProteinCount != null) {
                totalProteinCount =
                        ruleProteinCount.getReviewedProteinCount()
                                + ruleProteinCount.getUnreviewedProteinCount();
            } else {
                log.warn("No protein count found for old rule id {}", oldRuleId);
            }

        } else {
            log.warn("No old rule id found for Arba Id {}", arbaType.getId());
        }

        return totalProteinCount;
    }
}
