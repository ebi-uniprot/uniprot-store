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
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @date: 12 May 2020
 */
public class UniRuleDocumentConverter implements DocumentConverter<UniRuleType, UniRuleDocument> {
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
        // extract values from uniObj to create solr document
        String uniRuleId = uniObj.getUniRuleId().getValue();
        List<String> conditionValues = getConditionValues(uniObj);
        List<String> featureTypes = getFeatureTypes(uniObj);
        List<String> keywords = getKeywords(uniObj);
        List<String> geneNames = getGeneNames(uniObj);
        List<String> goTerms = getGoTerms(uniObj);
        List<String> proteinNames = getProteinNames(uniObj);
        List<String> organismNames = getOrganismNames(uniObj);
        List<String> taxonomyNames = getTaxonomyNames(uniObj);
        List<UniRuleDocumentComment> uniRuleDocComments = convertToUniRuleDocumentComments(uniObj);
        Map<String, Collection<String>> commentTypeValues = getComments(uniRuleDocComments);
        List<String> content = new ArrayList<>(conditionValues);
        content.add(uniRuleId);
        content.addAll(featureTypes);
        content.addAll(keywords);
        content.addAll(geneNames);
        content.addAll(goTerms);
        content.addAll(proteinNames);
        content.addAll(organismNames);
        content.addAll(taxonomyNames);
        content.addAll(getCommentsStringValues(uniRuleDocComments));
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

    private List<String> getConditionValues(UniRuleEntry uniObj) {
        return uniObj.getMainRule().getConditionSets().stream()
                .map(ConditionSet::getConditions)
                .flatMap(Collection::stream)
                .map(Condition::getConditionValues)
                .flatMap(Collection::stream)
                .map(Value::getValue)
                .collect(Collectors.toList());
    }

    private List<String> getFeatureTypes(UniRuleEntry uniObj) {
        List<String> featureTypes = new ArrayList<>();

        if (Utils.notNullNotEmpty(uniObj.getSamFeatureSets())) {
            featureTypes =
                    uniObj.getSamFeatureSets().stream()
                            .map(SamFeatureSet::getSamTrigger)
                            .map(SamTrigger::getSamTriggerType)
                            .filter(Objects::nonNull)
                            .map(EnumDisplay::getDisplayName)
                            .collect(Collectors.toList());
        }

        if (Utils.notNullNotEmpty(uniObj.getPositionFeatureSets())) {
            List<String> positionalFeatureTypes =
                    uniObj.getPositionFeatureSets().stream()
                            .map(PositionFeatureSet::getPositionalFeatures)
                            .flatMap(Collection::stream)
                            .map(PositionalFeature::getType)
                            .collect(Collectors.toList());

            featureTypes.addAll(positionalFeatureTypes);
        }

        return featureTypes;
    }

    private List<String> getKeywords(UniRuleEntry uniObj) {
        List<String> keywords = new ArrayList<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            keywords =
                    annotations.stream()
                            .map(Annotation::getKeyword)
                            .filter(Objects::nonNull)
                            .map(this::extractKeywords)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
        }

        return keywords;
    }

    private List<String> getGeneNames(UniRuleEntry uniObj) {
        List<String> geneNames = new ArrayList<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            geneNames =
                    annotations.stream()
                            .map(Annotation::getGene)
                            .filter(Objects::nonNull)
                            .map(this::extractGeneNames)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
        }
        return geneNames;
    }

    private List<String> getGoTerms(UniRuleEntry uniObj) {
        List<String> goTerms = new ArrayList<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            goTerms =
                    annotations.stream()
                            .map(Annotation::getDbReference)
                            .filter(Objects::nonNull)
                            .map(CrossReference::getId)
                            .collect(Collectors.toList());
        }

        return goTerms;
    }

    private List<String> getProteinNames(UniRuleEntry uniObj) {
        List<String> proteinNames = new ArrayList<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            proteinNames =
                    annotations.stream()
                            .map(Annotation::getProteinDescription)
                            .filter(Objects::nonNull)
                            .map(UniProtEntryConverterUtil::extractProteinDescriptionValues)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
        }
        return proteinNames;
    }

    private List<String> getOrganismNames(UniRuleEntry uniObj) {
        return extractConditionValues(
                CONDITION_TYPE_SCIENTIFIC_ORGANISM, uniObj.getMainRule().getConditionSets());
    }

    private List<String> getTaxonomyNames(UniRuleEntry uniObj) {
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

    private Map<String, Collection<String>> getComments(List<UniRuleDocumentComment> docComments) {
        return docComments.stream()
                .collect(
                        Collectors.toMap(
                                UniRuleDocumentComment::getName,
                                UniRuleDocumentComment::getValues));
    }

    private List<String> getCommentsStringValues(List<UniRuleDocumentComment> docComments) {
        return docComments.stream()
                .map(UniRuleDocumentComment::getStringValue)
                .collect(Collectors.toList());
    }

    private byte[] getUniRuleObj(UniRuleEntry uniRuleEntry) {
        try {
            return this.objectMapper.writeValueAsBytes(uniRuleEntry);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse uniRule entry to binary json: ", e);
        }
    }

    private List<String> extractConditionValues(String type, List<ConditionSet> conditionSets) {
        return conditionSets.stream()
                .map(ConditionSet::getConditions)
                .flatMap(Collection::stream)
                .filter(
                        condition ->
                                type.equals(condition.getType())
                                        && Utils.notNullNotEmpty(condition.getConditionValues()))
                .map(condition -> extractValues(condition.getConditionValues()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<String> extractGeneNames(Gene gene) {
        List<String> geneNames = new ArrayList<>();

        if (Objects.nonNull(gene.getGeneName())) {
            geneNames.add(gene.getGeneName().getValue());
        }

        geneNames.addAll(extractValues(gene.getSynonyms()));
        geneNames.addAll(extractValues(gene.getOrderedLocusNames()));
        geneNames.addAll(extractValues(gene.getOrfNames()));

        return geneNames;
    }

    private <T extends Value> List<String> extractValues(List<T> values) {
        List<String> names = new ArrayList<>();

        if (Utils.notNullNotEmpty(values)) {
            names.addAll(
                    values.stream()
                            .map(Value::getValue)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList()));
        }
        return names;
    }

    private List<String> extractKeywords(Keyword keyword) {
        List<String> keywords = new ArrayList<>();
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
