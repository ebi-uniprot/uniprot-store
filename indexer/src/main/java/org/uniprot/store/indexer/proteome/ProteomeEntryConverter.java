package org.uniprot.store.indexer.proteome;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.builder.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.core.uniprot.taxonomy.Taxonomy;
import org.uniprot.core.uniprot.taxonomy.builder.TaxonomyBuilder;
import org.uniprot.core.xml.jaxb.proteome.DbReferenceType;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.core.xml.proteome.ProteomeConverter;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * @author jluo
 * @date: 23 Apr 2019
 */
public class ProteomeEntryConverter implements DocumentConverter<Proteome, ProteomeDocument> {
    private static final String GC_SET_ACC = "GCSetAcc";
    private final TaxonomyRepo taxonomyRepo;
    private final ProteomeConverter proteomeConverter;
    private final ObjectMapper objectMapper;

    public ProteomeEntryConverter(TaxonomyRepo taxonomyRepo) {
        this.taxonomyRepo = taxonomyRepo;
        proteomeConverter = new ProteomeConverter();
        this.objectMapper = ProteomeJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public ProteomeDocument convert(Proteome source) {
        ProteomeDocument document = new ProteomeDocument();
        document.upid = source.getUpid();
        setOrganism(source, document);
        setLineageTaxon(source.getTaxonomy().intValue(), document);
        updateProteomeType(document, source);
        document.genomeAccession = fetchGenomeAccessions(source);
        document.superkingdom = source.getSuperregnum().name();
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

    private void updateAnnotationScore(ProteomeDocument document, Proteome source) {
        document.score = source.getAnnotationScore().getNormalizedAnnotationScore();
    }

    private void updateProteomeType(ProteomeDocument document, Proteome source) {
        if (source.isIsReferenceProteome()) {
            document.proteomeType = 1;
            document.isReferenceProteome = true;
        } else if (source.isIsRepresentativeProteome()) {
            document.proteomeType = 2;
            document.isReferenceProteome = true;
        } else if ((source.getRedundantTo() != null) && (!source.getRedundantTo().isEmpty())) {
            document.proteomeType = 4;
            document.isRedundant = true;
        } else document.proteomeType = 3;
    }

    private void setOrganism(Proteome source, ProteomeDocument document) {
        int taxonomyId = source.getTaxonomy().intValue();
        document.organismTaxId = taxonomyId;
        document.taxLineageIds.add(taxonomyId);
        Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
        if (taxonomicNode.isPresent()) {
            TaxonomicNode node = taxonomicNode.get();
            List<String> extractedTaxoNode = TaxonomyRepoUtil.extractTaxonFromNode(node);
            document.organismName.addAll(extractedTaxoNode);
            document.organismTaxon.addAll(extractedTaxoNode);
        } else {
            document.organismName.add(source.getName());
            document.organismTaxon.add(source.getName());
        }
    }

    private void setLineageTaxon(int taxId, ProteomeDocument document) {
        if (taxId > 0) {
            List<TaxonomicNode> nodes = TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, taxId);
            nodes.forEach(
                    node -> {
                        int id = node.id();
                        document.taxLineageIds.add(id);
                        List<String> taxons = TaxonomyRepoUtil.extractTaxonFromNode(node);
                        document.organismTaxon.addAll(taxons);
                    });
        }
    }

    private List<String> fetchGenomeAssemblyId(Proteome source) {
        return source.getDbReference().stream()
                .filter(val -> val.getType().equals(GC_SET_ACC))
                .map(DbReferenceType::getId)
                .collect(Collectors.toList());
    }

    private byte[] getBinaryObject(Proteome source) {
        ProteomeEntry proteome = this.proteomeConverter.fromXml(source);
        ProteomeEntryBuilder builder = ProteomeEntryBuilder.from(proteome);
        builder.canonicalProteinsSet(Collections.emptyList());
        Optional<TaxonomicNode> taxonomicNode =
                taxonomyRepo.retrieveNodeUsingTaxID((int) proteome.getTaxonomy().getTaxonId());
        if (taxonomicNode.isPresent()) {
            builder.taxonomy(getTaxonomy(taxonomicNode.get(), proteome.getTaxonomy().getTaxonId()));
            builder.taxonLineagesSet(getLineage(taxonomicNode.get().id()));
        }
        ProteomeEntry modifiedProteome = builder.build();
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(modifiedProteome);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse proteome to binary json: ", e);
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

    private List<String> fetchGenomeAccessions(Proteome source) {
        return source.getComponent().stream()
                .map(val -> val.getGenomeAccession())
                .flatMap(val -> val.stream())
                .collect(Collectors.toList());
    }
}
