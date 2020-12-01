package org.uniprot.store.indexer.uniprotkb.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.uniprot.core.Value;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.comment.AlternativeProductsComment;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.comment.IsoformSequenceStatus;
import org.uniprot.core.uniprotkb.description.*;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.store.search.document.suggest.SuggestDictionary;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
public class UniProtEntryConverterUtil {

    private UniProtEntryConverterUtil(){}

    private static final int SORT_FIELD_MAX_LENGTH = 30;

    static Set<String> extractEvidence(List<Evidence> evidences) {
        return evidences.stream()
                .flatMap(UniProtEntryConverterUtil::addExtractedEvidenceItem)
                .collect(Collectors.toSet());
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

    public static String createSuggestionMapKey(SuggestDictionary dict, String id) {
        return dict.name() + ":" + id;
    }

    static List<String> getXrefId(String id, String dbname) {
        List<String> values = new ArrayList<>();
        values.add(id);
        values.add(dbname + "-" + id);
        if (id.indexOf(".") > 0) {
            String idMain = id.substring(0, id.indexOf("."));
            values.add(idMain);
            values.add(dbname + "-" + idMain);
        }
        return values;
    }

    static String truncatedSortValue(String value) {
        if (value != null && value.length() > SORT_FIELD_MAX_LENGTH) {
            return value.substring(0, SORT_FIELD_MAX_LENGTH);
        } else {
            return value;
        }
    }

    static void addValueListToStringList(Collection<String> list, List<? extends Value> values) {
        if (values != null) {
            for (Value v : values) {
                addValueToStringList(list, v);
            }
        }
    }

    static void addValueToStringList(Collection<String> list, Value value) {
        if ((value != null) && (!value.getValue().isEmpty())) {
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

    public static List<String> extractProteinDescriptionValues(ProteinDescription description) {
        List<String> values = new ArrayList<>();
        if (description.hasRecommendedName()) {
            values.addAll(getProteinNameNames(description.getRecommendedName()));
        }
        if (description.hasSubmissionNames()) {
            description.getSubmissionNames().stream()
                    .map(UniProtEntryConverterUtil::getProteinSubNameNames)
                    .forEach(values::addAll);
        }
        if (description.hasAlternativeNames()) {
            description.getAlternativeNames().stream()
                    .map(UniProtEntryConverterUtil::getProteinNameNames)
                    .forEach(values::addAll);
        }
        if (description.hasContains()) {
            description.getContains().stream()
                    .map(UniProtEntryConverterUtil::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if (description.hasIncludes()) {
            description.getIncludes().stream()
                    .map(UniProtEntryConverterUtil::getProteinSectionValues)
                    .forEach(values::addAll);
        }
        if (description.hasAllergenName()) {
            values.add(description.getAllergenName().getValue());
        }
        if (description.hasBiotechName()) {
            values.add(description.getBiotechName().getValue());
        }
        if (description.hasCdAntigenNames()) {
            description.getCdAntigenNames().stream().map(Value::getValue).forEach(values::add);
        }
        if (description.hasInnNames()) {
            description.getInnNames().stream().map(Value::getValue).forEach(values::add);
        }
        return values;
    }

    private static List<String> getProteinSectionValues(ProteinSection proteinSection) {
        List<String> names = new ArrayList<>();
        if (proteinSection.hasRecommendedName()) {
            names.addAll(getProteinNameNames(proteinSection.getRecommendedName()));
        }
        if (proteinSection.hasAlternativeNames()) {
            proteinSection.getAlternativeNames().stream()
                    .map(UniProtEntryConverterUtil::getProteinNameNames)
                    .forEach(names::addAll);
        }
        if (proteinSection.hasCdAntigenNames()) {
            proteinSection.getCdAntigenNames().stream().map(Value::getValue).forEach(names::add);
        }
        if (proteinSection.hasAllergenName()) {
            names.add(proteinSection.getAllergenName().getValue());
        }
        if (proteinSection.hasInnNames()) {
            proteinSection.getInnNames().stream().map(Value::getValue).forEach(names::add);
        }
        if (proteinSection.hasBiotechName()) {
            names.add(proteinSection.getBiotechName().getValue());
        }
        return names;
    }

    private static List<String> getProteinNameNames(ProteinName proteinName) {
        List<String> names = new ArrayList<>();
        if (proteinName.hasFullName()) {
            names.add(proteinName.getFullName().getValue());
        }
        if (proteinName.hasShortNames()) {
            proteinName.getShortNames().stream().map(Name::getValue).forEach(names::add);
        }
        return names;
    }

    private static List<String> getProteinSubNameNames(ProteinSubName proteinAltName) {
        List<String> names = new ArrayList<>();
        if (proteinAltName.hasFullName()) {
            names.add(proteinAltName.getFullName().getValue());
        }
        return names;
    }
}
