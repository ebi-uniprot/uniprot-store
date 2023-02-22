package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;
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
    public static final String CC_CATALYTIC_ACTIVITY_EXP =
            "cc_" + CommentType.CATALYTIC_ACTIVITY.name().toLowerCase(Locale.ROOT) + "_exp";
    public static final String FT_BINDING =
            "ft_" + UniprotKBFeatureType.BINDING.name().toLowerCase(Locale.ROOT);
    public static final String FT_BINDING_EXP =
            "ft_" + UniprotKBFeatureType.BINDING.name().toLowerCase(Locale.ROOT) + "_exp";
    public static final String CC_COFACTOR_CHEBI_EXP = "cc_cofactor_chebi_exp";

    private static final Collection<String> CHEBI_COMMENT_FIELDS = List.of(CC_CATALYTIC_ACTIVITY, CC_CATALYTIC_ACTIVITY_EXP, CC_COFACTOR_CHEBI_EXP);

    private static final Collection<String> CHEBI_FEATURE_FIELDS = List.of(FT_BINDING, FT_BINDING_EXP);

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

                for(String commentFieldName: CHEBI_COMMENT_FIELDS) {
                    if (doc.commentMap.containsKey(commentFieldName)) {
                        addCommentMapChebi(doc, mappedChebi, commentFieldName);
                    }
                }

                for(String featureFieldName: CHEBI_FEATURE_FIELDS) {
                    if (doc.featuresMap.containsKey(featureFieldName)) {
                        addBindingChebi(doc, mappedChebi, featureFieldName);
                    }
                }
            }
        } catch (Exception e) {
            throw new SparkIndexException(
                    "Error at ChebiToUniProtDocument with accession: " + doc.accession, e);
        }
        return doc;
    }

    private void addBindingChebi(
            UniProtDocument doc, Map<String, ChebiEntry> mappedChebi, String fieldName) {
        Collection<String> bindings = doc.featuresMap.get(fieldName);
        addChebi(bindings, doc, mappedChebi);
    }

    private void addCommentMapChebi(
            UniProtDocument doc, Map<String, ChebiEntry> mappedChebi, String fieldName) {
        Collection<String> commentValues = doc.commentMap.get(fieldName);
        addChebi(commentValues, doc, mappedChebi);
    }

    private void addChebi(
            Collection<String> chebiRelatedItem,
            UniProtDocument doc,
            Map<String, ChebiEntry> mappedChebi) {
        Set<String> relatedChebi =
                chebiRelatedItem.stream()
                        .filter(id -> Objects.nonNull(id) && id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .flatMap(id -> id.getRelatedIds().stream())
                        .map(entry -> CHEBI_PREFIX + entry.getId())
                        .collect(Collectors.toSet());

        Set<String> chebiInchiKey =
                chebiRelatedItem.stream()
                        .filter(id -> Objects.nonNull(id) && id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .map(ChebiEntry::getInchiKey)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        doc.inchikey.addAll(chebiInchiKey);

        Set<String> chebiRelatedInchiKey =
                chebiRelatedItem.stream()
                        .filter(id -> Objects.nonNull(id) && id.startsWith(CHEBI_PREFIX))
                        .map(mappedChebi::get)
                        .filter(Objects::nonNull)
                        .flatMap(id -> id.getRelatedIds().stream())
                        .filter(Objects::nonNull)
                        .map(ChebiEntry::getInchiKey)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        doc.inchikey.addAll(chebiRelatedInchiKey);

        chebiRelatedItem.addAll(relatedChebi);

        doc.chebi.addAll(
                chebiRelatedItem.stream()
                        .filter(id -> Objects.nonNull(id) && id.startsWith(CHEBI_PREFIX))
                        .collect(Collectors.toSet()));

        chebiRelatedItem.addAll(chebiInchiKey);
        chebiRelatedItem.addAll(chebiRelatedInchiKey);
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
