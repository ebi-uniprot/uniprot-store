package org.uniprot.store.indexer.proteome;

import static org.uniprot.core.util.Utils.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.core.xml.jaxb.proteome.ComponentType;
import org.uniprot.core.xml.jaxb.proteome.ProteomeType;
import org.uniprot.core.xml.proteome.ProteomeConverter;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * @author jluo
 * @date: 23 Apr 2019
 */
public class ProteomeEntryConverter implements DocumentConverter<ProteomeType, ProteomeDocument> {
    private final TaxonomyRepo taxonomyRepo;
    private final ProteomeConverter proteomeConverter;
    private final ObjectMapper objectMapper;

    public ProteomeEntryConverter(TaxonomyRepo taxonomyRepo) {
        this.taxonomyRepo = taxonomyRepo;
        proteomeConverter = new ProteomeConverter();
        this.objectMapper = ProteomeJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public ProteomeDocument convert(ProteomeType source) {
        ProteomeDocument document = new ProteomeDocument();
        document.upid = source.getUpid();
        setOrganism(source, document);
        setLineageTaxon(source.getTaxonomy(), document);
        updateProteomeType(document, source);
        document.genomeAccession = fetchGenomeAccessions(source);
        document.genomeAssembly = fetchGenomeAssemblyId(source);
        document.content.add(document.upid);
        document.content.add(source.getDescription());
        document.content.addAll(document.organismTaxon);
        document.taxLineageIds.forEach(val -> document.content.add(val.toString()));

        document.proteomeStored = ByteBuffer.wrap(getBinaryObject(source));
        if (source.getAnnotationScore() != null) {
            updateAnnotationScore(document, source);
        }
        return document;
    }

    private void updateAnnotationScore(ProteomeDocument document, ProteomeType source) {
        document.score = source.getAnnotationScore().getNormalizedAnnotationScore();
    }

    private void updateProteomeType(ProteomeDocument document, ProteomeType source) {
        if ((source.getExcluded() != null)
                && (source.getExcluded().getExclusionReason() != null)
                && (!source.getExcluded().getExclusionReason().isEmpty())) {
            document.proteomeType = 5;
            document.isExcluded = true;
        } else if ((source.getRedundantTo() != null) && (!source.getRedundantTo().isEmpty())) {
            document.proteomeType = 4;
            document.isRedundant = true;
        } else if (source.isIsReferenceProteome()) {
            // if Representative And Reference it will be marked as reference proteomeType at the
            // end. should we make this field multiple values?
            document.proteomeType = 1;
            document.isReferenceProteome = true;
        } else if (source.isIsRepresentativeProteome()) {
            document.proteomeType = 2;
            // representative is also flagged as reference proteomes
            document.isReferenceProteome = true;
        } else {
            // Normal Proteome
            document.proteomeType = 3;
        }
    }

    private void setOrganism(ProteomeType source, ProteomeDocument document) {
        int taxonomyId = (int) source.getTaxonomy();
        if (taxonomyId > 0) {
            document.organismTaxId = taxonomyId;
            Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
            if (taxonomicNode.isPresent()) {
                TaxonomicNode node = taxonomicNode.get();
                List<String> extractedTaxoNode = TaxonomyRepoUtil.extractTaxonFromNode(node);
                document.organismName.addAll(extractedTaxoNode);
                document.organismSort = node.scientificName();
            }
        }
    }

    private void setLineageTaxon(Long taxId, ProteomeDocument document) {
        if (taxId != null) {
            List<TaxonomicNode> nodes =
                    TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, taxId.intValue());
            nodes.forEach(
                    node -> {
                        int id = node.id();
                        document.taxLineageIds.add(id);
                        List<String> taxons = TaxonomyRepoUtil.extractTaxonFromNode(node);
                        document.organismTaxon.addAll(taxons);
                    });
        }
        document.organismTaxon.stream()
                .filter(Superkingdom::isSuperkingdom)
                .findFirst()
                .ifPresent(superKingdom -> document.superkingdom = superKingdom);
    }

    private List<String> fetchGenomeAssemblyId(ProteomeType source) {
        List<String> result = new ArrayList<>();
        if (notNull(source.getGenomeAssembly())) {
            result.add(source.getGenomeAssembly().getGenomeAssembly());
        }
        return result;
    }

    private byte[] getBinaryObject(ProteomeType source) {
        ProteomeEntry proteome = this.proteomeConverter.fromXml(source);
        ProteomeEntryBuilder builder = ProteomeEntryBuilder.from(proteome);
        if (notNull(proteome.getTaxonomy())) {
            Optional<TaxonomicNode> taxonomicNode =
                    taxonomyRepo.retrieveNodeUsingTaxID((int) proteome.getTaxonomy().getTaxonId());
            if (taxonomicNode.isPresent()) {
                builder.taxonomy(
                        getTaxonomy(taxonomicNode.get(), proteome.getTaxonomy().getTaxonId()));
                List<TaxonomyLineage> lineageList = getLineage(taxonomicNode.get().id());
                builder.taxonLineagesSet(lineageList);

                //add superKingdom from lineage
                lineageList.stream()
                        .map(TaxonomyLineage::getScientificName)
                        .filter(Superkingdom::isSuperkingdom) //to avoid exception in typeOf
                        .map(Superkingdom::typeOf)
                        .findFirst()
                        .ifPresent(builder::superkingdom);
            }
        }
        ProteomeEntry modifiedProteome = builder.build();
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(modifiedProteome);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException("Unable to parse proteome to binary json: ", e);
        }
        return binaryEntry;
    }

    private Taxonomy getTaxonomy(TaxonomicNode node, long taxId) {

        TaxonomyBuilder builder = new TaxonomyBuilder();
        builder.taxonId(taxId).scientificName(node.scientificName());
        if (!Strings.isNullOrEmpty(node.commonName())) builder.commonName(node.commonName());
        if (!Strings.isNullOrEmpty(node.mnemonic())) builder.mnemonic(node.mnemonic());
        if (!Strings.isNullOrEmpty(node.synonymName())) {
            builder.synonymsAdd(node.synonymName());
        }
        return builder.build();
    }

    private List<TaxonomyLineage> getLineage(int taxId) {
        List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, taxId);
        List<TaxonomyLineage> lineage =
                nodes.stream()
                        .skip(1)
                        .map(
                                node ->
                                        new TaxonomyLineageBuilder()
                                                .taxonId(node.id())
                                                .scientificName(node.scientificName())
                                                .build())
                        .collect(Collectors.toList());
        return Lists.reverse(lineage);
    }

    private List<String> fetchGenomeAccessions(ProteomeType source) {
        return source.getComponent().stream()
                .map(ComponentType::getGenomeAccession)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
