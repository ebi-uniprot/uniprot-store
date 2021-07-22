package org.uniprot.store.indexer.common.aa;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Value;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.uniprotkb.Keyword;
import org.uniprot.core.unirule.Annotation;
import org.uniprot.core.unirule.Condition;
import org.uniprot.core.unirule.ConditionSet;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil;
import org.uniprot.store.search.document.DocumentConversionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @created 22/07/2021
 */
public class AARuleDocumentConverterUtils {
    private static final String CC_UNDERSCORE = "cc_";
    private static final String SINGLE_SPACE = " ";
    private static final String UNDERSCORE = "_";
    private static final String CONDITION_TYPE_TAXON = "taxon";
    private static final String CONDITION_TYPE_SCIENTIFIC_ORGANISM = "scientific organism";

    private AARuleDocumentConverterUtils() {}

    public static byte[] getAARuleObj(UniRuleEntry uniRuleEntry, ObjectMapper objectMapper) {
        try {
            return objectMapper.writeValueAsBytes(uniRuleEntry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException(
                    "Unable to parse AA rule entry to binary json: ", e);
        }
    }

    public static Map<String, Set<String>> getComments(List<AARuleDocumentComment> docComments) {
        return docComments.stream()
                .collect(
                        Collectors.toMap(
                                docComment ->
                                        convertCommentDisplayNameToSolrField(
                                                docComment.getName()), // cc_xyz format
                                AARuleDocumentComment::getValues,
                                (list1, list2) -> {
                                    list1.addAll(list2);
                                    return list1;
                                }));
    }

    public static Set<String> getConditionValues(UniRuleEntry uniObj) {
        return uniObj.getMainRule().getConditionSets().stream()
                .map(ConditionSet::getConditions)
                .flatMap(Collection::stream)
                .map(Condition::getConditionValues)
                .flatMap(Collection::stream)
                .map(Value::getValue)
                .collect(Collectors.toSet());
    }

    public static Set<String> getKeywords(UniRuleEntry uniObj) {
        Set<String> keywords = new HashSet<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            keywords =
                    annotations.stream()
                            .map(Annotation::getKeyword)
                            .filter(Objects::nonNull)
                            .map(AARuleDocumentConverterUtils::extractKeywords)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
        }

        return keywords;
    }

    public static Set<String> getGeneNames(UniRuleEntry uniObj) {
        Set<String> geneNames = new HashSet<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            geneNames =
                    annotations.stream()
                            .map(Annotation::getGene)
                            .filter(Objects::nonNull)
                            .map(AARuleDocumentConverterUtils::extractGeneNames)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
        }
        return geneNames;
    }

    public static Set<String> getGoTerms(UniRuleEntry uniObj) {
        Set<String> goTerms = new HashSet<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            goTerms =
                    annotations.stream()
                            .map(Annotation::getDbReference)
                            .filter(Objects::nonNull)
                            .map(CrossReference::getId)
                            .collect(Collectors.toSet());
        }

        return goTerms;
    }

    public static Set<String> getProteinNames(UniRuleEntry uniObj) {
        Set<String> proteinNames = new HashSet<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            proteinNames =
                    annotations.stream()
                            .map(Annotation::getProteinDescription)
                            .filter(Objects::nonNull)
                            .map(UniProtEntryConverterUtil::extractProteinDescriptionValues)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
        }
        return proteinNames;
    }

    public static Set<String> getOrganismNames(UniRuleEntry uniObj) {
        return extractConditionValues(
                CONDITION_TYPE_SCIENTIFIC_ORGANISM, uniObj.getMainRule().getConditionSets());
    }

    public static Set<String> getTaxonomyNames(UniRuleEntry uniObj) {
        return extractConditionValues(
                CONDITION_TYPE_TAXON, uniObj.getMainRule().getConditionSets());
    }

    private static Set<String> extractKeywords(Keyword keyword) {
        Set<String> keywords = new HashSet<>();
        keywords.add(keyword.getId());
        keywords.add(keyword.getName());
        KeywordCategory kc = keyword.getCategory();
        if (!keywords.contains(kc.getId())) {
            keywords.add(kc.getId());
            keywords.add(kc.getName());
        }
        return keywords;
    }

    private static Set<String> extractGeneNames(Gene gene) {
        Set<String> geneNames = new HashSet<>();

        if (Objects.nonNull(gene.getGeneName())) {
            geneNames.add(gene.getGeneName().getValue());
        }

        geneNames.addAll(extractValues(gene.getSynonyms()));
        geneNames.addAll(extractValues(gene.getOrderedLocusNames()));
        geneNames.addAll(extractValues(gene.getOrfNames()));

        return geneNames;
    }

    private static <T extends Value> Set<String> extractValues(List<T> values) {
        Set<String> names = new HashSet<>();

        if (Utils.notNullNotEmpty(values)) {
            names.addAll(
                    values.stream()
                            .map(Value::getValue)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet()));
        }
        return names;
    }

    private static Set<String> extractConditionValues(
            String type, List<ConditionSet> conditionSets) {
        return conditionSets.stream()
                .map(ConditionSet::getConditions)
                .flatMap(Collection::stream)
                .filter(
                        condition ->
                                type.equals(condition.getType())
                                        && Utils.notNullNotEmpty(condition.getConditionValues()))
                .map(condition -> extractValues(condition.getConditionValues()))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private static String convertCommentDisplayNameToSolrField(String displayName) {
        StringBuilder builder = new StringBuilder(CC_UNDERSCORE);
        builder.append(displayName);
        return builder.toString().replace(SINGLE_SPACE, UNDERSCORE);
    }
}
