package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

/**
 * It aggregate Inactive UniprotEntry when is the DEMERGE scenario
 *
 * @author lgonzales
 * @since 2019-12-20
 */
public class InactiveEntryAggregationMapper
        implements Function2<UniProtKBEntry, UniProtKBEntry, UniProtKBEntry> {

    private static final long serialVersionUID = -7346523459708364644L;

    /**
     * @param entry1 Inactive UniprotEntry with the same accession
     * @param entry2 Inactive UniprotEntry with the same accession
     * @return Uniprot Entry with an aggregation when is the DEMERGE scenario.
     */
    @Override
    public UniProtKBEntry call(UniProtKBEntry entry1, UniProtKBEntry entry2) throws Exception {
        UniProtKBEntry mergedEntry = null;
        if (isThereAnyNullEntry(entry1, entry2)) {
            mergedEntry = getNotNullEntry(entry1, entry2);
        } else {
            List<String> mergedAccessions = new ArrayList<>();
            mergedAccessions.addAll(entry1.getInactiveReason().getMergeDemergeTos());
            mergedAccessions.addAll(entry2.getInactiveReason().getMergeDemergeTos());

            EntryInactiveReasonBuilder inactiveReasonBuilder =
                    EntryInactiveReasonBuilder.from(entry1.getInactiveReason());
            inactiveReasonBuilder.mergeDemergeTosSet(mergedAccessions);
            inactiveReasonBuilder.type(InactiveReasonType.DEMERGED);

            mergedEntry =
                    new UniProtKBEntryBuilder(
                                    entry1.getPrimaryAccession(),
                                    entry1.getUniProtkbId(),
                                    inactiveReasonBuilder.build())
                            .build();
        }

        return mergedEntry;
    }

    private UniProtKBEntry getNotNullEntry(UniProtKBEntry entry1, UniProtKBEntry entry2) {
        UniProtKBEntry result = entry1;
        if (result == null) {
            result = entry2;
        }
        return result;
    }

    private boolean isThereAnyNullEntry(UniProtKBEntry entry1, UniProtKBEntry entry2) {
        return entry1 == null || entry2 == null;
    }
}
