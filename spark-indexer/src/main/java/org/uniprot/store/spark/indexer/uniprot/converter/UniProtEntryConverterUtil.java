package org.uniprot.store.spark.indexer.uniprot.converter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.uniprot.core.Value;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.comment.AlternativeProductsComment;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.comment.IsoformSequenceStatus;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
public class UniProtEntryConverterUtil {

    private UniProtEntryConverterUtil() {}

    private static final int SORT_FIELD_MAX_LENGTH = 30;
    private static final int SPELLCHECK_MIN_LENGTH = 4;

    static Set<String> extractEvidence(List<Evidence> evidences) {
        Set<String> result =
                evidences.stream()
                        .flatMap(UniProtEntryConverterUtil::addExtractedEvidenceItem)
                        .collect(Collectors.toSet());
        return result;
    }

    private static Stream<String> addExtractedEvidenceItem(Evidence evidence) {
        List<String> result = new ArrayList<>();
        result.add(evidence.getEvidenceCode().name());

        result.addAll(
                evidence.getEvidenceCode().getCategories().stream()
                        .map(category -> category.name().toLowerCase())
                        .collect(Collectors.toList()));

        return result.stream();
    }

    static boolean canAddExperimental(
            boolean typeAddExperimental,
            String commentVal,
            Boolean reviewed,
            Set<String> evidences) {
        return hasExperimentalEvidence(evidences)
                || (evidences.isEmpty()
                        && typeAddExperimental
                        && (reviewed != null && reviewed)
                        && canAddExperimentalByAnnotationText(commentVal));
    }

    static boolean canAddExperimentalByAnnotationText(String textValue) {
        String lowerCaseTextValue = textValue.toLowerCase();
        return !lowerCaseTextValue.contains("(by similarity).")
                && !lowerCaseTextValue.contains("(probable).")
                && !lowerCaseTextValue.contains("(potential).");
    }

    static boolean hasExperimentalEvidence(Collection<String> evidences) {
        return evidences.contains(EvidenceCode.ECO_0000269.name());
    }

    static String createSuggestionMapKey(SuggestDictionary dict, String id) {
        return dict.name() + ":" + id;
    }

    static List<String> getXrefId(String id, String dbname) {
        List<String> values = new ArrayList<>();
        values.add(id);
        values.add(dbname + "-" + id);
        if (id.indexOf('.') >= 0) {
            String idMain = id.substring(0, id.indexOf('.'));
            values.add(idMain);
            values.add(dbname + "-" + idMain);
        }
        return values;
    }

    public static String truncatedSortValue(String value) {
        if (Utils.notNull(value) && value.length() > SORT_FIELD_MAX_LENGTH) {
            return value.substring(0, SORT_FIELD_MAX_LENGTH);
        } else {
            return value;
        }
    }

    public static void populateSuggestions(Collection<String> values, UniProtDocument document) {
        Set<String> length4orMore =
                values.stream()
                        .filter(Objects::nonNull)
                        .filter(val -> val.length() >= SPELLCHECK_MIN_LENGTH)
                        .collect(Collectors.toSet());
        document.suggests.addAll(length4orMore);
    }

    static void addValueListToStringList(Collection<String> list, List<? extends Value> values) {
        if (Utils.notNullNotEmpty(values)) {
            for (Value v : values) {
                addValueToStringList(list, v);
            }
        }
    }

    static void addValueToStringList(Collection<String> list, Value value) {
        if (Utils.notNull(value) && (!value.getValue().isEmpty())) {
            list.add(value.getValue());
        }
    }

    static boolean isCanonicalIsoform(UniProtKBEntry uniProtkbEntry) {
        return uniProtkbEntry.getCommentsByType(CommentType.ALTERNATIVE_PRODUCTS).stream()
                        .map(comment -> (AlternativeProductsComment) comment)
                        .flatMap(comment -> comment.getIsoforms().stream())
                        .filter(
                                isoform ->
                                        isoform.getIsoformSequenceStatus()
                                                == IsoformSequenceStatus.DISPLAYED)
                        .flatMap(isoform -> isoform.getIsoformIds().stream())
                        .filter(
                                isoformId ->
                                        isoformId
                                                .getValue()
                                                .equals(
                                                        uniProtkbEntry
                                                                .getPrimaryAccession()
                                                                .getValue()))
                        .count()
                == 1L;
    }
}
