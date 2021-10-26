package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

public class ChebiToUniProtDocument
        implements Function<
                Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>>, UniProtDocument> {
    private static final long serialVersionUID = -5590195948492465026L;
    public static final String CHEBI_PREFIX = "CHEBI:";

    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple2)
            throws Exception {
        UniProtDocument doc = tuple2._1;
        try {
            if (tuple2._2.isPresent()) {
                Iterable<ChebiEntry> chebiEntries = tuple2._2.get();
                Map<String, ChebiEntry> mappedChebi = new HashMap<>();
                chebiEntries.forEach(
                        chebiEntry -> {
                            mappedChebi.put(CHEBI_PREFIX + chebiEntry.getId(), chebiEntry);
                            if (Utils.notNullNotEmpty(chebiEntry.getInchiKey())) {
                                doc.inchikey.add(chebiEntry.getInchiKey());
                            }
                        });
                if (Utils.notNullNotEmpty(doc.cofactorChebi)) {
                    Set<String> relatedCofactors =
                            doc.cofactorChebi.stream()
                                    .filter(id -> id.startsWith(CHEBI_PREFIX))
                                    .map(mappedChebi::get)
                                    .filter(Objects::nonNull)
                                    .flatMap(id -> id.getRelatedIds().stream())
                                    .map(entry -> CHEBI_PREFIX + entry.getId())
                                    .collect(Collectors.toSet());
                    doc.cofactorChebi.addAll(relatedCofactors);

                    doc.chebi.addAll(
                            doc.cofactorChebi.stream()
                                    .filter(id -> id.startsWith(CHEBI_PREFIX))
                                    .collect(Collectors.toSet()));
                }

                Collection<String> catalytic =
                        doc.commentMap.get(
                                "cc_"
                                        + CommentType.CATALYTIC_ACTIVITY
                                                .name()
                                                .toLowerCase(Locale.ROOT));
                if (Utils.notNullNotEmpty(catalytic)) {
                    Set<String> relatedCatalytic =
                            catalytic.stream()
                                    .filter(id -> id.startsWith(CHEBI_PREFIX))
                                    .map(mappedChebi::get)
                                    .filter(Objects::nonNull)
                                    .flatMap(id -> id.getRelatedIds().stream())
                                    .map(entry -> CHEBI_PREFIX + entry.getId())
                                    .collect(Collectors.toSet());
                    catalytic.addAll(relatedCatalytic);

                    doc.chebi.addAll(
                            catalytic.stream()
                                    .filter(id -> id.startsWith(CHEBI_PREFIX))
                                    .collect(Collectors.toSet()));
                }
            }
        } catch (Exception e) {
            throw new SparkIndexException(
                    "Error at ChebiToUniProtDocument with accession: " + doc.accession, e);
        }
        return doc;
    }
}
