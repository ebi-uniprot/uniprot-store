package org.uniprot.store.indexer.unirule;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Value;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.json.parser.unirule.UniRuleJsonConfig;
import org.uniprot.core.uniprotkb.Keyword;
import org.uniprot.core.unirule.*;
import org.uniprot.core.unirule.impl.UniRuleEntryBuilder;
import org.uniprot.core.util.EnumDisplay;
import org.uniprot.core.util.Utils;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.core.xml.unirule.UniRuleEntryConverter;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @date: 12 May 2020 Converts the xml type {@link UniRuleType} to {@link UniRuleDocument}
 */
public class UniRuleDocumentConverter implements DocumentConverter<UniRuleType, UniRuleDocument> {
    private static final String CC_UNDERSCORE = "cc_";
    private static final String SINGLE_SPACE = " ";
    private static final String UNDERSCORE = "_";
    private static final String CONDITION_TYPE_TAXON = "taxon";
    private static final String CONDITION_TYPE_SCIENTIFIC_ORGANISM = "scientific organism";
    private final UniRuleEntryConverter converter;
    private Long proteinsAnnotatedCount;
    private final ObjectMapper objectMapper;

    public UniRuleDocumentConverter() {
        this.converter = new UniRuleEntryConverter();
        objectMapper = UniRuleJsonConfig.getInstance().getFullObjectMapper();
    }

    // inject the protein count before calling convert
    public void setProteinsAnnotatedCount(Long proteinsAnnotatedCount) {
        this.proteinsAnnotatedCount = proteinsAnnotatedCount;
    }

    public ObjectMapper getObjectMapper() {
        return this.objectMapper;
    }

    @Override
    public UniRuleDocument convert(UniRuleType xmlObj) {
        UniRuleEntryBuilder uniRuleBuilder =
                UniRuleEntryBuilder.from(this.converter.fromXml(xmlObj));
        UniRuleEntry uniObj =
                uniRuleBuilder.proteinsAnnotatedCount(this.proteinsAnnotatedCount).build();
        return convertToDocument(uniObj);
    }

    public UniRuleDocument convertToDocument(UniRuleEntry uniObj) {
        // extract values from uniObj to create solr document
        String uniRuleId = uniObj.getUniRuleId().getValue();
        Set<String> conditionValues = getConditionValues(uniObj);
        Set<String> featureTypes = getFeatureTypes(uniObj);
        Set<String> keywords = getKeywords(uniObj);
        Set<String> geneNames = getGeneNames(uniObj);
        Set<String> goTerms = getGoTerms(uniObj);
        Set<String> proteinNames = getProteinNames(uniObj);
        Set<String> organismNames = getOrganismNames(uniObj);
        Set<String> taxonomyNames = getTaxonomyNames(uniObj);
        List<UniRuleDocumentComment> uniRuleDocComments = convertToUniRuleDocumentComments(uniObj);
        Map<String, Set<String>> commentTypeValues = getComments(uniRuleDocComments);
        Set<String> content = new HashSet<>(conditionValues);
        content.add(uniRuleId);
        content.addAll(featureTypes);
        content.addAll(keywords);
        content.addAll(geneNames);
        content.addAll(goTerms);
        content.addAll(proteinNames);
        content.addAll(organismNames);
        content.addAll(taxonomyNames);
        content.addAll(getCommentsValues(uniRuleDocComments));
        ByteBuffer uniRuleObj = ByteBuffer.wrap(getUniRuleObj(uniObj));

        // build the solr document
        UniRuleDocument.UniRuleDocumentBuilder builder = UniRuleDocument.builder();
        builder.uniRuleId(uniRuleId).featureTypes(featureTypes);
        builder.conditionValues(conditionValues);
        builder.keywords(keywords).geneNames(geneNames);
        builder.goTerms(goTerms).proteinNames(proteinNames);
        builder.organismNames(organismNames).taxonomyNames(taxonomyNames);
        builder.commentTypeValues(commentTypeValues);
        builder.content(content).uniRuleObj(uniRuleObj);
        return builder.build();
    }

    private Set<String> getConditionValues(UniRuleEntry uniObj) {
        return uniObj.getMainRule().getConditionSets().stream()
                .map(ConditionSet::getConditions)
                .flatMap(Collection::stream)
                .map(Condition::getConditionValues)
                .flatMap(Collection::stream)
                .map(Value::getValue)
                .collect(Collectors.toSet());
    }

    private Set<String> getFeatureTypes(UniRuleEntry uniObj) {
        Set<String> featureTypes = new HashSet<>();

        if (Utils.notNullNotEmpty(uniObj.getSamFeatureSets())) {
            featureTypes =
                    uniObj.getSamFeatureSets().stream()
                            .map(SamFeatureSet::getSamTrigger)
                            .map(SamTrigger::getSamTriggerType)
                            .filter(Objects::nonNull)
                            .map(EnumDisplay::getDisplayName)
                            .collect(Collectors.toSet());
        }

        if (Utils.notNullNotEmpty(uniObj.getPositionFeatureSets())) {
            Set<String> positionalFeatureTypes =
                    uniObj.getPositionFeatureSets().stream()
                            .map(PositionFeatureSet::getPositionalFeatures)
                            .flatMap(Collection::stream)
                            .map(PositionalFeature::getType)
                            .collect(Collectors.toSet());

            featureTypes.addAll(positionalFeatureTypes);
        }

        return featureTypes;
    }

    private Set<String> getKeywords(UniRuleEntry uniObj) {
        Set<String> keywords = new HashSet<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            keywords =
                    annotations.stream()
                            .map(Annotation::getKeyword)
                            .filter(Objects::nonNull)
                            .map(this::extractKeywords)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
        }

        return keywords;
    }

    private Set<String> getGeneNames(UniRuleEntry uniObj) {
        Set<String> geneNames = new HashSet<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            geneNames =
                    annotations.stream()
                            .map(Annotation::getGene)
                            .filter(Objects::nonNull)
                            .map(this::extractGeneNames)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
        }
        return geneNames;
    }

    private Set<String> getGoTerms(UniRuleEntry uniObj) {
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

    private Set<String> getProteinNames(UniRuleEntry uniObj) {
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

    private Set<String> getOrganismNames(UniRuleEntry uniObj) {
        return extractConditionValues(
                CONDITION_TYPE_SCIENTIFIC_ORGANISM, uniObj.getMainRule().getConditionSets());
    }

    private Set<String> getTaxonomyNames(UniRuleEntry uniObj) {
        return extractConditionValues(
                CONDITION_TYPE_TAXON, uniObj.getMainRule().getConditionSets());
    }

    private List<UniRuleDocumentComment> convertToUniRuleDocumentComments(UniRuleEntry uniObj) {
        List<UniRuleDocumentComment> docComments = new ArrayList<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            docComments =
                    annotations.stream()
                            .map(Annotation::getComment)
                            .filter(Objects::nonNull)
                            .map(UniRuleCommentConverter::convertToDocumentComment)
                            .collect(Collectors.toList());
        }
        return docComments;
    }

    private Map<String, Set<String>> getComments(List<UniRuleDocumentComment> docComments) {
        return docComments.stream()
                .collect(
                        Collectors.toMap(
                                docComment ->
                                        convertCommentDisplayNameToSolrField(
                                                docComment.getName()), // cc_xyz format
                                UniRuleDocumentComment::getValues,
                                (list1, list2) -> {
                                    list1.addAll(list2);
                                    return list1;
                                }));
    }

    private static String convertCommentDisplayNameToSolrField(String displayName) {
        StringBuilder builder = new StringBuilder(CC_UNDERSCORE);
        builder.append(displayName);
        return builder.toString().replace(SINGLE_SPACE, UNDERSCORE);
    }

    private Set<String> getCommentsValues(List<UniRuleDocumentComment> docComments) {
        return docComments.stream()
                .map(this::mergeCommentNameValues)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private Set<String> mergeCommentNameValues(UniRuleDocumentComment uniRuleDocumentComment) {
        Set<String> values = new HashSet<>(uniRuleDocumentComment.getValues());
        values.add(uniRuleDocumentComment.getName());
        return values;
    }

    private byte[] getUniRuleObj(UniRuleEntry uniRuleEntry) {
        try {
            return this.objectMapper.writeValueAsBytes(uniRuleEntry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException(
                    "Unable to parse uniRule entry to binary json: ", e);
        }
    }

    private Set<String> extractConditionValues(String type, List<ConditionSet> conditionSets) {
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

    private Set<String> extractGeneNames(Gene gene) {
        Set<String> geneNames = new HashSet<>();

        if (Objects.nonNull(gene.getGeneName())) {
            geneNames.add(gene.getGeneName().getValue());
        }

        geneNames.addAll(extractValues(gene.getSynonyms()));
        geneNames.addAll(extractValues(gene.getOrderedLocusNames()));
        geneNames.addAll(extractValues(gene.getOrfNames()));

        return geneNames;
    }

    private <T extends Value> Set<String> extractValues(List<T> values) {
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

    private Set<String> extractKeywords(Keyword keyword) {
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
}
