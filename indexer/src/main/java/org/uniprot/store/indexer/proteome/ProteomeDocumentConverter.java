package org.uniprot.store.indexer.proteome;

import static org.uniprot.core.util.Utils.*;

import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.proteome.CPDStatus;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.util.Utils;
import org.uniprot.core.xml.jaxb.proteome.ComponentType;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.core.xml.jaxb.proteome.ScorePropertyType;
import org.uniprot.core.xml.jaxb.proteome.ScoreType;
import org.uniprot.core.xml.proteome.ScoreBuscoConverter;
import org.uniprot.core.xml.proteome.ScoreCPDConverter;
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
public class ProteomeDocumentConverter implements DocumentConverter<Proteome, ProteomeDocument> {
    private final TaxonomyRepo taxonomyRepo;
    private final ObjectMapper objectMapper;

    public ProteomeDocumentConverter(TaxonomyRepo taxonomyRepo) {
        this.taxonomyRepo = taxonomyRepo;
        this.objectMapper = ProteomeJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public ProteomeDocument convert(Proteome source) {
        ProteomeDocument document = new ProteomeDocument();
        document.upid = source.getUpid();
        setOrganism(source, document);
        setLineageTaxon(source.getTaxonomy(), document);
        updateProteome(document, source);
        document.genomeAccession = fetchGenomeAccessions(source);
        document.genomeAssembly = fetchGenomeAssemblyId(source);
        document.proteinCount = fetchProteinCount(source.getComponent());
        document.busco = fetchBusco(source.getScores());
        document.cpd = fetchCPD(source.getScores());
        if (source.getAnnotationScore() != null) {
            updateAnnotationScore(document, source);
        }
        return document;
    }

    private int fetchCPD(List<ScoreType> scores) {
        return scores.stream()
                .filter(scoreType -> scoreType.getName().equalsIgnoreCase(ScoreCPDConverter.NAME))
                .flatMap(busco -> busco.getProperty().stream())
                .filter(prop -> ScoreCPDConverter.PROPERTY_STATUS.equals(prop.getName()))
                .map(ScorePropertyType::getValue)
                .map(CPDStatus::fromValue)
                .map(CPDStatus::getId)
                .findFirst()
                .orElse(CPDStatus.UNKNOWN.getId());
    }

    private float fetchBusco(List<ScoreType> scores) {
        Map<String, String> buscoProperties = scores.stream()
                .filter(scoreType -> scoreType.getName().equalsIgnoreCase(ScoreBuscoConverter.NAME))
                .flatMap(busco -> busco.getProperty().stream())
                .collect(Collectors.toMap(ScorePropertyType::getName, ScorePropertyType::getValue));
        float buscoCompletedPercentage = 0f;
        if(buscoProperties.containsKey(ScoreBuscoConverter.PROPERTY_TOTAL) &&
                buscoProperties.containsKey(ScoreBuscoConverter.PROPERTY_COMPLETED)){
            int total = Integer.parseInt(buscoProperties.get(ScoreBuscoConverter.PROPERTY_TOTAL));
            int completed = Integer.parseInt(buscoProperties.get(ScoreBuscoConverter.PROPERTY_COMPLETED));
            buscoCompletedPercentage = completed * 100f / total;
        }
        return buscoCompletedPercentage;
    }

    private int fetchProteinCount(List<ComponentType> component) {
        return component.stream()
                .filter(c -> Utils.notNull(c.getProteinCount()))
                .mapToInt(ComponentType::getProteinCount)
                .sum();
    }

    private void updateAnnotationScore(ProteomeDocument document, Proteome source) {
        document.score = source.getAnnotationScore().getNormalizedAnnotationScore();
    }

    private void updateProteome(ProteomeDocument document, Proteome source) {
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

    private void setOrganism(Proteome source, ProteomeDocument document) {
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

    private List<String> fetchGenomeAssemblyId(Proteome source) {
        List<String> result = new ArrayList<>();
        if (notNull(source.getGenomeAssembly())) {
            result.add(source.getGenomeAssembly().getGenomeAssembly());
        }
        return result;
    }

    private List<String> fetchGenomeAccessions(Proteome source) {
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
