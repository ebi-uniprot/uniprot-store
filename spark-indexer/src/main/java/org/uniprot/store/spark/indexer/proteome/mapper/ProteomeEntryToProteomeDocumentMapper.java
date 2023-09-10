package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.uniprot.core.util.Utils.notNull;
import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil.truncatedSortValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.proteome.*;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

@Slf4j
public class ProteomeEntryToProteomeDocumentMapper
        implements Function<ProteomeEntry, ProteomeDocument> {
    private static final long serialVersionUID = -3661895534731010610L;

    @Override
    public ProteomeDocument call(ProteomeEntry proteomeEntry) throws Exception {
        ProteomeDocument document = new ProteomeDocument();
        document.upid = proteomeEntry.getId().getValue();
        document.organismTaxId = (int) proteomeEntry.getTaxonomy().getTaxonId();
        document.strain = proteomeEntry.getStrain();
        document.score = proteomeEntry.getAnnotationScore();
        document.genomeAccession = getGenomeAccession(proteomeEntry);
        document.genomeAssembly = getGenomeAssembly(proteomeEntry);
        document.proteinCount = getProteinCount(proteomeEntry.getComponents());
        ProteomeCompletenessReport proteomeCompletenessReport =
                proteomeEntry.getProteomeCompletenessReport();
        document.busco = getBuscoPercentage(proteomeCompletenessReport.getBuscoReport());
        document.cpd = getCPD(proteomeCompletenessReport.getCPDReport());
        updateProteomeDocument(document, proteomeEntry);
        updateOrganismFields(document, (TaxonomyEntry) proteomeEntry.getTaxonomy());
        document.proteomeStored = ByteBuffer.wrap(SerializationUtils.serialize(proteomeEntry));
        return document;
    }

    private void updateOrganismFields(ProteomeDocument proteomeDocument, TaxonomyEntry organism) {
        List<String> organismNames = getOrganismNames(organism);
        proteomeDocument.organismName.addAll(organismNames);
        proteomeDocument.organismTaxon.addAll(organismNames);
        proteomeDocument.organismSort = truncatedSortValue(String.join(" ", organismNames));
        proteomeDocument.taxLineageIds.add(Math.toIntExact(organism.getTaxonId()));

        if (organism.hasLineage()) {
            organism.getLineages()
                    .forEach(lineage -> updateLineageTaxonomy(proteomeDocument, lineage));
        } else {
            log.warn("Unable to find organism lineage for: " + proteomeDocument.organismTaxId);
        }
    }

    private List<String> getOrganismNames(TaxonomyEntry organism) {
        List<String> organismNames = new LinkedList<>();
        organismNames.add(organism.getScientificName());
        if (organism.hasCommonName()) {
            organismNames.add(organism.getCommonName());
        }
        if (organism.hasSynonyms()) {
            organismNames.addAll(organism.getSynonyms());
        }
        if (organism.hasMnemonic()) {
            organismNames.add(organism.getMnemonic());
        }
        return organismNames;
    }

    private void updateLineageTaxonomy(ProteomeDocument document, TaxonomyLineage lineage) {
        document.taxLineageIds.add(Math.toIntExact(lineage.getTaxonId()));
        document.organismTaxon.add(lineage.getScientificName());
        if (lineage.hasCommonName()) {
            document.organismTaxon.add(lineage.getCommonName());
        }
        document.organismTaxon.stream()
                .filter(Superkingdom::isSuperkingdom)
                .findFirst()
                .ifPresent(superKingdom -> document.superkingdom = superKingdom);
    }

    private Float getBuscoPercentage(BuscoReport buscoReport) {
        float buscoCompletedPercentage = 0f;
        if (buscoReport != null && buscoReport.getTotal() > 0) {
            buscoCompletedPercentage = buscoReport.getComplete() * 100f / buscoReport.getTotal();
        }
        return buscoCompletedPercentage;
    }

    private int getCPD(CPDReport cpdReport) {
        return cpdReport != null ? cpdReport.getStatus().getId() : 0;
    }

    private int getProteinCount(List<Component> components) {
        return components.stream()
                .filter(c -> Utils.notNull(c.getProteinCount()))
                .mapToInt(Component::getProteinCount)
                .sum();
    }

    private void updateProteomeDocument(ProteomeDocument document, ProteomeEntry proteomeEntry) {
        ProteomeType proteomeType = proteomeEntry.getProteomeType();

        switch (proteomeType) {
            case REFERENCE:
            case REPRESENTATIVE:
            case REFERENCE_AND_REPRESENTATIVE:
                document.proteomeType = 1;
                document.isReferenceProteome = true;
                break;
            case EXCLUDED:
                document.proteomeType = 4;
                document.isExcluded = true;
                break;
            case REDUNDANT:
                document.proteomeType = 3;
                document.isRedundant = true;
                break;
            default:
                document.proteomeType = 2;
        }
    }

    private List<String> getGenomeAssembly(ProteomeEntry proteomeEntry) {
        List<String> result = new ArrayList<>();
        if (notNull(proteomeEntry.getGenomeAssembly())) {
            result.add(proteomeEntry.getGenomeAssembly().getAssemblyId());
        }
        return result;
    }

    private List<String> getGenomeAccession(ProteomeEntry proteomeEntry) {
        return proteomeEntry.getComponents().stream()
                .map(c -> c.getGenomeAnnotation().getSource())
                .collect(Collectors.toList());
    }
}