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
    public static final String CC_CATALYTIC_ACTIVITY =
            "cc_" + CommentType.CATALYTIC_ACTIVITY.name().toLowerCase(Locale.ROOT);

    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple2)
            throws Exception {
        UniProtDocument doc = tuple2._1;
        try {
            if (tuple2._2.isPresent()) {
                Iterable<ChebiEntry> chebiEntries = tuple2._2.get();
                Map<String, ChebiEntry> mappedChebi = new HashMap<>();
                chebiEntries.forEach(entry -> mappedChebi.put(CHEBI_PREFIX + entry.getId(), entry));
                if (Utils.notNullNotEmpty(doc.cofactorChebi)) {
                    addCofactorChebi(doc, mappedChebi);
                }

                if (doc.commentMap.containsKey(CC_CATALYTIC_ACTIVITY)) {
                    addCatalyticActivityChebi(doc, mappedChebi);
                }
            }
        } catch (Exception e) {
            throw new SparkIndexException(
                    "Error at ChebiToUniProtDocument with accession: " + doc.accession, e);
        }
        return doc;
    }

    private void addCatalyticActivityChebi(
            UniProtDocument doc, Map<String, ChebiEntry> mappedChebi) {
        Collection<String> catalytic = doc.commentMap.get(CC_CATALYTIC_ACTIVITY);
        Set<String> relatedCatalytic =
                catalytic.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .flatMap(id -> id.getRelatedIds().stream())
                        .map(entry -> CHEBI_PREFIX + entry.getId())
                        .collect(Collectors.toSet());

        Set<String> catalyticInchiKey =
                catalytic.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .map(ChebiEntry::getInchiKey)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        doc.inchikey.addAll(catalyticInchiKey);

        Set<String> catalyticRelatedInchiKey =
                catalytic.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .flatMap(id -> id.getRelatedIds().stream())
                        .filter(Objects::nonNull)
                        .map(ChebiEntry::getInchiKey)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        doc.inchikey.addAll(catalyticRelatedInchiKey);

        catalytic.addAll(relatedCatalytic);

        doc.chebi.addAll(
                catalytic.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .collect(Collectors.toSet()));

        catalytic.addAll(catalyticInchiKey);
        catalytic.addAll(catalyticRelatedInchiKey);
    }

    private void addCofactorChebi(UniProtDocument doc, Map<String, ChebiEntry> mappedChebi) {
        Set<String> relatedCofactors =
                doc.cofactorChebi.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .flatMap(id -> id.getRelatedIds().stream())
                        .map(entry -> CHEBI_PREFIX + entry.getId())
                        .collect(Collectors.toSet());

        Set<String> cofactorsInchiKey =
                doc.cofactorChebi.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .map(ChebiEntry::getInchiKey)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        doc.inchikey.addAll(cofactorsInchiKey);

        Set<String> cofactorsRelatedInchiKey =
                doc.cofactorChebi.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .flatMap(id -> id.getRelatedIds().stream())
                        .filter(Objects::nonNull)
                        .map(ChebiEntry::getInchiKey)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        doc.inchikey.addAll(cofactorsRelatedInchiKey);

        doc.cofactorChebi.addAll(relatedCofactors);

        doc.chebi.addAll(
                doc.cofactorChebi.stream()
                        .filter(id -> id.startsWith(CHEBI_PREFIX))
                        .collect(Collectors.toSet()));

        doc.cofactorChebi.addAll(cofactorsInchiKey);
        doc.cofactorChebi.addAll(cofactorsRelatedInchiKey);
    }
}
