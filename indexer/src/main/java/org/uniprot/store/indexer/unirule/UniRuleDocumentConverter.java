package org.uniprot.store.indexer.unirule;

import static org.uniprot.store.indexer.common.aa.AARuleCommentConverter.convertToAARuleDocumentComments;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.extractEcs;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getAARuleObj;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getComments;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getConditionValues;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getFamilies;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getGeneNames;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getGoTerms;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getKeywords;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getOrganismNames;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getProteinNames;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getTaxonomyNames;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.core.json.parser.unirule.UniRuleJsonConfig;
import org.uniprot.core.unirule.PositionFeatureSet;
import org.uniprot.core.unirule.PositionalFeature;
import org.uniprot.core.unirule.SamFeatureSet;
import org.uniprot.core.unirule.SamTrigger;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.unirule.impl.UniRuleEntryBuilder;
import org.uniprot.core.util.EnumDisplay;
import org.uniprot.core.util.Utils;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.core.xml.unirule.UniRuleEntryConverter;
import org.uniprot.store.indexer.common.aa.AARuleDocumentComment;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @date: 12 May 2020 Converts the xml type {@link UniRuleType} to {@link UniRuleDocument}
 */
public class UniRuleDocumentConverter implements DocumentConverter<UniRuleType, UniRuleDocument> {
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
        List<AARuleDocumentComment> uniRuleDocComments = convertToAARuleDocumentComments(uniObj);
        Map<String, Set<String>> commentTypeValues = getComments(uniRuleDocComments);
        ByteBuffer uniRuleObj = ByteBuffer.wrap(getAARuleObj(uniObj, this.objectMapper));
        Set<String> ecNumbers = extractEcs(uniObj);

        // build the solr document
        UniRuleDocument.UniRuleDocumentBuilder builder = UniRuleDocument.builder();
        builder.uniRuleId(uniRuleId).featureTypes(featureTypes);
        builder.conditionValues(conditionValues);
        builder.keywords(keywords).geneNames(geneNames);
        builder.goTerms(goTerms).proteinNames(proteinNames);
        builder.organismNames(organismNames).taxonomyNames(taxonomyNames);
        builder.commentTypeValues(commentTypeValues);
        builder.uniRuleObj(uniRuleObj);
        builder.ecNumbers(ecNumbers);
        builder.families(getFamilies(uniRuleDocComments));
        return builder.build();
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
}
