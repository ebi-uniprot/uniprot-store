package org.uniprot.store.indexer.unirule;

import java.util.Map;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
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

    public UniRuleProcessor(TaxonomyRepo taxonomyRepo) {
        this.documentConverter = new UniRuleDocumentConverter(taxonomyRepo);
    }

    @Override
    public UniRuleDocument process(UniRuleType uniRuleType) {
        Pair<Long, Long> reviewedUnreviewedPair = computeProteinsAnnotatedCount(uniRuleType);
        this.documentConverter.setReviewedProteinCount(reviewedUnreviewedPair.getLeft());
        this.documentConverter.setUnreviewedProteinCount(reviewedUnreviewedPair.getRight());
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

    private Pair<Long, Long> computeProteinsAnnotatedCount(UniRuleType uniRuleType) {
        Pair<Long, Long> reviewedUnreviewedPair = Pair.of(0L, 0L);
        String oldRuleId = uniRuleType.getInformation().getOldRuleNum();
        if (Objects.nonNull(oldRuleId)) {
            UniRuleProteinCountReader.UniRuleProteinCount ruleProteinCount =
                    this.uniRuleProteinCountMap.get(oldRuleId);
            if (Objects.nonNull(ruleProteinCount)) {
                reviewedUnreviewedPair =
                        Pair.of(
                                ruleProteinCount.getReviewedProteinCount(),
                                ruleProteinCount.getUnreviewedProteinCount());
            } else {
                log.warn("No protein count found for old rule id {}", oldRuleId);
            }

        } else {
            log.warn("No old rule id found for UniRule Id {}", uniRuleType.getId());
        }

        return reviewedUnreviewedPair;
    }
}
