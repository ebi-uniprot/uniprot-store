package org.uniprot.store.indexer.proteome;

import static org.uniprot.core.util.Utils.*;

import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.xml.jaxb.proteome.ComponentType;
import org.uniprot.core.xml.jaxb.proteome.ProteomeType;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author jluo
 * @date: 23 Apr 2019
 */
public class ProteomeDocumentConverter
        implements DocumentConverter<ProteomeType, ProteomeDocument> {
    private final TaxonomyRepo taxonomyRepo;
    private final ObjectMapper objectMapper;

    public ProteomeDocumentConverter(TaxonomyRepo taxonomyRepo) {
        this.taxonomyRepo = taxonomyRepo;
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

    private List<String> fetchGenomeAccessions(ProteomeType source) {
        return source.getComponent().stream()
                .map(ComponentType::getGenomeAccession)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public byte[] getBinaryObject(ProteomeEntry entry) {
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(entry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException("Unable to parse proteome to binary json: ", e);
        }
        return binaryEntry;
    }
}
