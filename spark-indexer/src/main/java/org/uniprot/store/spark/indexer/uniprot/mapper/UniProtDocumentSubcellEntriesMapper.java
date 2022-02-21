package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 08/02/2022
 */
public class UniProtDocumentSubcellEntriesMapper
        implements Function<
                Tuple2<UniProtDocument, Optional<Iterable<SubcellularLocationEntry>>>,
                UniProtDocument> {
    @Override
    public UniProtDocument call(
            Tuple2<UniProtDocument, Optional<Iterable<SubcellularLocationEntry>>> tuple)
            throws Exception {
        UniProtDocument document = tuple._1;
        if (tuple._2.isPresent()) {
            Iterable<SubcellularLocationEntry> subcellEntries = tuple._2.get();
            Set<SubcellularIdValue> relatedSubcells = new HashSet<>();
            for (SubcellularLocationEntry subcellEntry : subcellEntries) {
                populateRelatedSubcells(subcellEntry, relatedSubcells);
            }
            updateUniProtDocumentWithSubcell(document, relatedSubcells);
        }
        return document;
    }

    private void populateRelatedSubcells(
            SubcellularLocationEntry currentSubcellEntry,
            Set<SubcellularIdValue> allRelatedSubcells) {

        SubcellularIdValue currentSubcellIdVal = convertToSubcellularIdValue(currentSubcellEntry);
        if (!allRelatedSubcells.contains(currentSubcellIdVal)) {
            allRelatedSubcells.add(currentSubcellIdVal);
            List<SubcellularLocationEntry> isAs = currentSubcellEntry.getIsA();
            List<SubcellularLocationEntry> partOfs = currentSubcellEntry.getPartOf();
            List<SubcellularLocationEntry> currentSubcellRelatedEntries = new ArrayList<>(isAs);
            currentSubcellRelatedEntries.addAll(partOfs);
            for (SubcellularLocationEntry relatedEntry : currentSubcellRelatedEntries) {
                populateRelatedSubcells(relatedEntry, allRelatedSubcells);
            }
        }
    }

    private SubcellularIdValue convertToSubcellularIdValue(
            SubcellularLocationEntry currentSubcell) {
        return new SubcellularIdValue.SubcellularIdValueBuilder()
                .id(currentSubcell.getId())
                .value(currentSubcell.getName())
                .build();
    }

    private void updateUniProtDocumentWithSubcell(
            UniProtDocument document, Set<SubcellularIdValue> related) {
        Set<String> ids =
                related.stream().map(SubcellularIdValue::getId).collect(Collectors.toSet());
        Set<String> values =
                related.stream().map(SubcellularIdValue::getValue).collect(Collectors.toSet());
        document.content.addAll(ids);
        document.subcellLocationTerm.addAll(ids);
        document.subcellLocationTerm.addAll(values);
    }
}
