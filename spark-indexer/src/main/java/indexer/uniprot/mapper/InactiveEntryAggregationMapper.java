package indexer.uniprot.mapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.uniprot.InactiveReasonType;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.builder.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;

/**
 * It aggregate Inactive UniprotEntry when is the DEMERGE scenario
 *
 * @author lgonzales
 * @since 2019-12-20
 */
public class InactiveEntryAggregationMapper
        implements Function2<UniProtEntry, UniProtEntry, UniProtEntry> {

    private static final long serialVersionUID = -7346523459708364644L;

    /**
     * @param entry1 Inactive UniprotEntry with the same accession
     * @param entry2 Inactive UniprotEntry with the same accession
     * @return Uniprot Entry with an aggregation when is the DEMERGE scenario.
     */
    @Override
    public UniProtEntry call(UniProtEntry entry1, UniProtEntry entry2) throws Exception {
        UniProtEntry mergedEntry = null;
        if (isThereAnyNullEntry(entry1, entry2)) {
            mergedEntry = getNotNullEntry(entry1, entry2);
        } else {
            List<String> mergedAccessions = new ArrayList<>();
            mergedAccessions.addAll(entry1.getInactiveReason().getMergeDemergeTo());
            mergedAccessions.addAll(entry2.getInactiveReason().getMergeDemergeTo());

            EntryInactiveReasonBuilder inactiveReasonBuilder =
                    new EntryInactiveReasonBuilder().from(entry1.getInactiveReason());
            inactiveReasonBuilder.mergeDemergeTo(mergedAccessions);
            inactiveReasonBuilder.type(InactiveReasonType.DEMERGED);

            mergedEntry =
                    new UniProtEntryBuilder(
                                    entry1.getPrimaryAccession(),
                                    entry1.getUniProtId(),
                                    inactiveReasonBuilder.build())
                            .build();
        }

        return mergedEntry;
    }

    private UniProtEntry getNotNullEntry(UniProtEntry entry1, UniProtEntry entry2) {
        UniProtEntry result = entry1;
        if (result == null) {
            result = entry2;
        }
        return result;
    }

    private boolean isThereAnyNullEntry(UniProtEntry entry1, UniProtEntry entry2) {
        return entry1 == null || entry2 == null;
    }
}
