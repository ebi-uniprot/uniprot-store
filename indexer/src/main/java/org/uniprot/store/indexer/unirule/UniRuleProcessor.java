package org.uniprot.store.indexer.unirule;

import java.util.Map;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

/**
 * @author sahmad
 * @date: 14 May 2020
 */
@Slf4j
public class UniRuleProcessor implements ItemProcessor<UniRuleType, UniRuleDocument> {
    private final UniRuleDocumentConverter documentConverter;
    // cache from UniRuleProteinCountWriter Step
    private Map<String, UniRuleProteinCountReader.UniRuleProteinCount> uniRuleProteinCountMap;

    public UniRuleProcessor() {
        this.documentConverter = new UniRuleDocumentConverter();
    }

    @Override
    public UniRuleDocument process(UniRuleType uniRuleType) {
        long proteinsAnnotatedCount = computeProteinsAnnotatedCount(uniRuleType);
        this.documentConverter.setProteinsAnnotatedCount(proteinsAnnotatedCount);
        return this.documentConverter.convert(uniRuleType);
    }

    @BeforeStep
    public void getStepExecution(
            final StepExecution stepExecution) { // get the cached data from previous step

        this.uniRuleProteinCountMap =
                (Map<String, UniRuleProteinCountReader.UniRuleProteinCount>)
                        stepExecution
                                .getJobExecution()
                                .getExecutionContext()
                                .get(Constants.UNIRULE_PROTEIN_COUNT_CACHE_KEY);
    }

    private long computeProteinsAnnotatedCount(UniRuleType uniRuleType) {
        long totalProteinCount = 0L;
        String oldRuleId = uniRuleType.getInformation().getOldRuleNum();
        if (Objects.nonNull(oldRuleId)) {
            UniRuleProteinCountReader.UniRuleProteinCount ruleProteinCount =
                    this.uniRuleProteinCountMap.get(oldRuleId);
            if (ruleProteinCount != null) {
                totalProteinCount =
                        ruleProteinCount.getReviewedProteinCount()
                                + ruleProteinCount.getUnreviewedProteinCount();
            } else {
                log.warn("No protein count found for old rule id {}", oldRuleId);
            }

        } else {
            log.warn("No old rule id found for UniRule Id {}", uniRuleType.getId());
        }

        return totalProteinCount;
    }
}
