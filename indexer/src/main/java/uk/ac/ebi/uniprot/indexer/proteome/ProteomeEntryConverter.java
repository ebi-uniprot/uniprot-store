package uk.ac.ebi.uniprot.indexer.proteome;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomicNode;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.domain.proteome.ProteomeEntry;
import uk.ac.ebi.uniprot.domain.proteome.builder.ProteomeEntryBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyLineage;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyLineageBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.taxonomy.Taxonomy;
import uk.ac.ebi.uniprot.domain.uniprot.taxonomy.builder.TaxonomyBuilder;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.json.parser.proteome.ProteomeJsonConfig;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.ProteomeType;
import uk.ac.ebi.uniprot.xml.proteome.ProteomeConverter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author jluo
 * @date: 23 Apr 2019
 */

public class ProteomeEntryConverter implements DocumentConverter<ProteomeType, ProteomeDocument> {
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
    public ProteomeDocument convert(ProteomeType source) {
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
        updateAnnotationScore(document, source);
        return document;
    }

    private void updateAnnotationScore(ProteomeDocument document, ProteomeType source) {
        document.score = source.getAnnotationScore().getNormalizedAnnotationScore();
    }

    private void updateProteomeType(ProteomeDocument document, ProteomeType source) {
        if (source.isIsReferenceProteome()) {
            document.proteomeType = 1;
            document.isReferenceProteome = true;
        } else if (source.isIsRepresentativeProteome()) {
            document.proteomeType = 2;
            document.isReferenceProteome = true;
        } else if ((source.getRedundantTo() != null) && (!source.getRedundantTo().isEmpty())) {
            document.proteomeType = 4;
            document.isRedundant = true;
        } else
            document.proteomeType = 3;
    }

    private void setOrganism(ProteomeType source, ProteomeDocument document) {
        int taxonomyId = source.getTaxonomy().intValue();
        document.organismTaxId = taxonomyId;
        document.taxLineageIds.add(taxonomyId);
        Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
        if (taxonomicNode.isPresent()) {
            TaxonomicNode node = taxonomicNode.get();
            List<String> extractedTaxoNode = extractTaxonode(node);
            document.organismName.addAll(extractedTaxoNode);
            document.organismTaxon.addAll(extractedTaxoNode);
        } else {
            document.organismName.add(source.getName());
            document.organismTaxon.add(source.getName());
        }

    }

    private void setLineageTaxon(int taxId, ProteomeDocument document) {
        if (taxId > 0) {
            Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);

            while (taxonomicNode.isPresent()) {
                TaxonomicNode node = taxonomicNode.get();
                document.taxLineageIds.add(node.id());
                document.organismTaxon.addAll(extractTaxonode(node));
                taxonomicNode = getParentTaxon(node.id());
            }
        }
    }

    private Optional<TaxonomicNode> getParentTaxon(int taxId) {
        Optional<TaxonomicNode> optionalNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);
        return optionalNode.filter(TaxonomicNode::hasParent).map(TaxonomicNode::parent);
    }

    private List<String> extractTaxonode(TaxonomicNode node) {
        List<String> taxonmyItems = new ArrayList<>();
        if (node.scientificName() != null && !node.scientificName().isEmpty()) {
            taxonmyItems.add(node.scientificName());
        }
        if (node.commonName() != null && !node.commonName().isEmpty()) {
            taxonmyItems.add(node.commonName());
        }
        if (node.synonymName() != null && !node.synonymName().isEmpty()) {
            taxonmyItems.add(node.synonymName());
        }
        if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
            taxonmyItems.add(node.mnemonic());
        }
        return taxonmyItems;
    }

    private List<String> fetchGenomeAssemblyId(ProteomeType source) {
        return source.getDbReference().stream()
                .filter(val -> val.getType().equals(GC_SET_ACC))
                .map(val -> val.getId())
                .collect(Collectors.toList());
    }

    private byte[] getBinaryObject(ProteomeType source) {
        ProteomeEntry proteome = this.proteomeConverter.fromXml(source);
        ProteomeEntryBuilder builder = ProteomeEntryBuilder.newInstance().from(proteome);
        builder.canonicalProteins(Collections.emptyList());
        Optional<TaxonomicNode> taxonomicNode = taxonomyRepo
                .retrieveNodeUsingTaxID((int) proteome.getTaxonomy().getTaxonId());
        if (taxonomicNode.isPresent()) {
            builder.taxonomy(getTaxonomy(taxonomicNode.get(), proteome.getTaxonomy().getTaxonId()));
            builder.taxonLineage(getLineage(taxonomicNode.get().id()));
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

        TaxonomyBuilder builder = TaxonomyBuilder.newInstance();
        builder.taxonId(taxId).scientificName(node.scientificName());
        if (!Strings.isNullOrEmpty(node.commonName()))
            builder.commonName(node.commonName());
        if (!Strings.isNullOrEmpty(node.mnemonic()))
            builder.mnemonic(node.mnemonic());
        if (!Strings.isNullOrEmpty(node.synonymName())) {
            builder.addSynonyms(node.synonymName());
        }
        return builder.build();

    }

    private List<TaxonomyLineage> getLineage(int taxId) {
        List<TaxonomyLineage> lineage = new ArrayList<>();
        Optional<TaxonomicNode> taxonomicNode = getParentTaxon(taxId);

        while (taxonomicNode.isPresent()) {
            TaxonomicNode node = taxonomicNode.get();
            lineage.add(
                    new TaxonomyLineageBuilder()
                            .taxonId(node.id())
                            .scientificName(node.scientificName())

                            .build()
            );
            taxonomicNode = getParentTaxon(node.id());
        }
        return Lists.reverse(lineage);
    }

    private List<String> fetchGenomeAccessions(ProteomeType source) {
        return source.getComponent().stream().map(val -> val.getGenomeAccession()).flatMap(val -> val.stream())
                .collect(Collectors.toList());
    }

}

