package org.uniprot.store.indexer.arba;

import static org.uniprot.store.indexer.common.aa.AARuleCommentConverter.convertToAARuleDocumentComments;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getAARuleObj;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getComments;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getConditionValues;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getGeneNames;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getGoTerms;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getKeywords;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getOrganismNames;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getProteinNames;
import static org.uniprot.store.indexer.common.aa.AARuleDocumentConverterUtils.getTaxonomyNames;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.uniprot.core.json.parser.unirule.UniRuleJsonConfig;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.unirule.impl.UniRuleEntryBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.core.xml.unirule.UniRuleEntryConverter;
import org.uniprot.store.indexer.common.aa.AARuleDocumentComment;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.arba.ArbaDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
public class ArbaDocumentConverter implements DocumentConverter<UniRuleType, ArbaDocument> {
    private final UniRuleEntryConverter converter;
    private Long proteinsAnnotatedCount;
    private final ObjectMapper objectMapper;

    public ArbaDocumentConverter() {
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
    public ArbaDocument convert(UniRuleType xmlObj) {
        UniRuleEntryBuilder arbaBuilder = UniRuleEntryBuilder.from(this.converter.fromXml(xmlObj));
        UniRuleEntry uniObj =
                arbaBuilder.proteinsAnnotatedCount(this.proteinsAnnotatedCount).build();
        return convertToDocument(uniObj);
    }

    public ArbaDocument convertToDocument(UniRuleEntry uniObj) {
        // extract values from uniObj to create solr document
        if (Utils.notNullNotEmpty(uniObj.getSamFeatureSets())
                || Utils.notNullNotEmpty(uniObj.getPositionFeatureSets())) {
            throw new IllegalArgumentException(
                    "samFeatureSets and positionFeature are not supported!");
        }

        String arbaId = uniObj.getUniRuleId().getValue();
        Set<String> conditionValues = getConditionValues(uniObj);
        Set<String> keywords = getKeywords(uniObj);
        Set<String> geneNames = getGeneNames(uniObj);
        Set<String> goTerms = getGoTerms(uniObj);
        Set<String> proteinNames = getProteinNames(uniObj);
        Set<String> organismNames = getOrganismNames(uniObj);
        Set<String> taxonomyNames = getTaxonomyNames(uniObj);
        List<AARuleDocumentComment> arbaDocComments = convertToAARuleDocumentComments(uniObj);
        Map<String, Set<String>> commentTypeValues = getComments(arbaDocComments);
        ByteBuffer arbaObj = ByteBuffer.wrap(getAARuleObj(uniObj, this.objectMapper));

        // build the solr document
        ArbaDocument.ArbaDocumentBuilder builder = ArbaDocument.builder();
        builder.ruleId(arbaId);
        builder.conditionValues(conditionValues);
        builder.keywords(keywords).geneNames(geneNames);
        builder.goTerms(goTerms).proteinNames(proteinNames);
        builder.organismNames(organismNames).taxonomyNames(taxonomyNames);
        builder.commentTypeValues(commentTypeValues);
        builder.ruleObj(arbaObj);
        return builder.build();
    }
}
