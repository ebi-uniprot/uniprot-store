package org.uniprot.store.indexer.common.aa;

import static org.uniprot.core.uniprotkb.comment.CommentType.CATALYTIC_ACTIVITY;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.ECNumber;
import org.uniprot.core.Value;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.uniprotkb.Keyword;
import org.uniprot.core.uniprotkb.comment.CatalyticActivityComment;
import org.uniprot.core.uniprotkb.comment.Reaction;
import org.uniprot.core.unirule.Annotation;
import org.uniprot.core.unirule.Condition;
import org.uniprot.core.unirule.ConditionSet;
import org.uniprot.core.unirule.ConditionValue;
import org.uniprot.core.unirule.Information;
import org.uniprot.core.unirule.Rule;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.common.utils.UniProtAARuleUtils;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
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

    public static Set<String> extractEcs(UniRuleEntry uniRuleEntry) {
        // ecs from main rule
        Set<String> ecs = extractEcsFromRule(uniRuleEntry.getMainRule());
        // ecs from other rules
        Set<String> otherRulesEcs =
                uniRuleEntry.getOtherRules().stream()
                        .map(AARuleDocumentConverterUtils::extractEcsFromRule)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        // add them together
        ecs.addAll(otherRulesEcs);
        return ecs;
    }

    public static byte[] getAARuleObj(UniRuleEntry uniRuleEntry, ObjectMapper objectMapper) {
        try {
            return objectMapper.writeValueAsBytes(uniRuleEntry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException(
                    "Unable to parse AA rule entry to binary json: ", e);
        }
    }

    public static Map<String, Set<String>> getComments(List<AARuleDocumentComment> docComments) {
        Map<String, Set<String>> commentTypeValues =
                docComments.stream()
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
        // extract notes in a map
        Map<String, Set<String>> notesValues =
                docComments.stream()
                        .filter(docComment -> Utils.notNullNotEmpty(docComment.getNotes()))
                        .collect(
                                Collectors.toMap(
                                        docComment ->
                                                convertCommentDisplayNameToSolrField(
                                                        docComment.getName()
                                                                + "_note"), // cc_xyz_note format
                                        AARuleDocumentComment::getNotes,
                                        (set1, set2) -> {
                                            set1.addAll(set2);
                                            return set1;
                                        }));

        commentTypeValues.putAll(notesValues);
        return commentTypeValues;
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

    public static Set<String> getFamilies(List<AARuleDocumentComment> aaRuleDocumentComments) {
        return aaRuleDocumentComments.stream()
                .map(AARuleDocumentComment::getFamilies)
                .filter(Utils::notNullNotEmpty)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    public static Set<String> getSuperKingdoms(UniRuleEntry uniObj, TaxonomyRepo taxonomyRepo) {
        Set<Integer> taxonIds =
                uniObj.getMainRule().getConditionSets().stream()
                        .map(ConditionSet::getConditions)
                        .flatMap(Collection::stream)
                        .filter(
                                condition ->
                                        CONDITION_TYPE_TAXON.equals(condition.getType())
                                                && Utils.notNullNotEmpty(
                                                        condition.getConditionValues()))
                        .map(condition -> extractValueCvId(condition.getConditionValues()))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        return getSuperKingdoms(taxonIds, taxonomyRepo);
    }

    public static Set<String> getRuleIds(UniRuleEntry uniRuleEntry) {
        Set<String> ruleIds = new HashSet<>();
        ruleIds.add(uniRuleEntry.getUniRuleId().getValue());
        Information info = uniRuleEntry.getInformation();
        if (Utils.notNullNotEmpty(info.getOldRuleNum())) {
            ruleIds.add(info.getOldRuleNum());
        }
        return ruleIds;
    }

    private static Set<String> getSuperKingdoms(Set<Integer> taxonIds, TaxonomyRepo taxonomyRepo) {
        Set<String> superKingdoms = new HashSet<>();
        for (Integer id : taxonIds) {
            List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, id);
            for (TaxonomicNode node : nodes) {
                List<String> taxons = TaxonomyRepoUtil.extractTaxonFromNode(node);
                taxons.stream()
                        .filter(Superkingdom::isSuperkingdom)
                        .findFirst()
                        .ifPresent(superKingdoms::add);
            }
        }
        return superKingdoms;
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

    private static <T extends ConditionValue> Set<Integer> extractValueCvId(List<T> values) {
        Set<Integer> cvIds = new HashSet<>();

        if (Utils.notNullNotEmpty(values)) {
            cvIds.addAll(
                    values.stream()
                            .map(ConditionValue::getCvId)
                            .filter(Objects::nonNull)
                            .map(Integer::parseInt)
                            .collect(Collectors.toSet()));
        }
        return cvIds;
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

    private static Set<String> extractEcsFromRule(Rule rule) {
        // ec can be found in protein description and comment of type catalytic activity
        Set<String> proteinDescEcs =
                rule.getAnnotations().stream()
                        .map(Annotation::getProteinDescription)
                        .filter(Objects::nonNull)
                        .map(UniProtAARuleUtils::extractProteinDescriptionEcs)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        Set<String> commentEcs = extractEcsFromRuleComment(rule);
        proteinDescEcs.addAll(commentEcs);
        return proteinDescEcs;
    }

    private static Set<String> extractEcsFromRuleComment(Rule rule) {
        List<Annotation> annotations = rule.getAnnotations();
        Set<String> ecs = new HashSet<>();
        if (Utils.notNullNotEmpty(annotations)) {
            ecs =
                    annotations.stream()
                            .map(Annotation::getComment)
                            .filter(Objects::nonNull)
                            .filter(comment -> comment.getCommentType() == CATALYTIC_ACTIVITY)
                            .map(CatalyticActivityComment.class::cast)
                            .map(CatalyticActivityComment::getReaction)
                            .filter(Reaction::hasEcNumber)
                            .map(Reaction::getEcNumber)
                            .map(ECNumber::getValue)
                            .collect(Collectors.toSet());
        }
        return ecs;
    }
}
