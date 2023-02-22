package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    static final String CC_SUBCELL_EXP = "cc_scl_term_exp";
    private static final String SUBCELL_PREFIX = "SL-";

    @Override
    public UniProtDocument call(
            Tuple2<UniProtDocument, Optional<Iterable<SubcellularLocationEntry>>> tuple)
            throws Exception {
        UniProtDocument document = tuple._1;
        if (tuple._2.isPresent()) {
            Iterable<SubcellularLocationEntry> subcellEntries = tuple._2.get();
            Map<String, Set<SubcellularIdValue>> subcellMap = new HashMap<>();
            for (SubcellularLocationEntry subcellEntry : subcellEntries) {
                Set<SubcellularIdValue> relatedIds = populateRelatedSubcells(List.of(subcellEntry));
                subcellMap.put(subcellEntry.getId(), relatedIds);
            }
            updateUniProtDocumentWithSubcell(document, subcellMap);
        }
        return document;
    }

    private Set<SubcellularIdValue> populateRelatedSubcells(
            List<SubcellularLocationEntry> currentSubcellEntries) {
        Set<SubcellularIdValue> relatedSubCells = new HashSet<>();
        for (SubcellularLocationEntry currentSubcellEntry : currentSubcellEntries) {
            SubcellularIdValue currentSubcellIdVal =
                    convertToSubcellularIdValue(currentSubcellEntry);
            relatedSubCells.add(currentSubcellIdVal);
            relatedSubCells.addAll(populateRelatedSubcells(currentSubcellEntry.getIsA()));
            relatedSubCells.addAll(populateRelatedSubcells(currentSubcellEntry.getPartOf()));
        }
        return relatedSubCells;
    }

    private SubcellularIdValue convertToSubcellularIdValue(
            SubcellularLocationEntry currentSubcell) {
        return new SubcellularIdValue.SubcellularIdValueBuilder()
                .id(currentSubcell.getId())
                .value(currentSubcell.getName())
                .build();
    }

    private void updateUniProtDocumentWithSubcell(
            UniProtDocument document, Map<String, Set<SubcellularIdValue>> relatedMap) {
        Set<String> terms =
                relatedMap.values().stream()
                        .flatMap(Collection::stream)
                        .flatMap(idValue -> Stream.of(idValue.getId(), idValue.getValue()))
                        .collect(Collectors.toSet());

        document.content.addAll(terms);
        document.subcellLocationTerm.addAll(terms);
        document.subcellLocationTerm.addAll(terms);
        if (document.commentMap.containsKey(CC_SUBCELL_EXP)) {
            Collection<String> expSubCells = document.commentMap.get(CC_SUBCELL_EXP);
            Set<String> experimentalTerms =
                    expSubCells.stream()
                            .filter(id -> id.startsWith(SUBCELL_PREFIX))
                            .map(relatedMap::get)
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .flatMap(idValue -> Stream.of(idValue.getId(), idValue.getValue()))
                            .collect(Collectors.toSet());
            expSubCells.addAll(experimentalTerms);
        }
    }
}
