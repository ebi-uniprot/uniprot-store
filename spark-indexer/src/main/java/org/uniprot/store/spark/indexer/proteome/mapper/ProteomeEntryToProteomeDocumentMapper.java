package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.uniprot.core.util.Utils.notNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.proteome.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

public class ProteomeEntryToProteomeDocumentMapper
        implements Function<ProteomeEntry, ProteomeDocument> {
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
        document.busco = getBusco(proteomeCompletenessReport.getBuscoReport());
        document.cpd = getCPD(proteomeCompletenessReport.getCPDReport());
        updateProteome(document, proteomeEntry);
        return document;
    }

    private Float getBusco(BuscoReport buscoReport) {
        float buscoCompletedPercentage = 0f;
        if (buscoReport.getTotal() > 0) {
            buscoCompletedPercentage = buscoReport.getComplete() * 100f / buscoReport.getTotal();
        }
        return buscoCompletedPercentage;
    }

    private int getCPD(CPDReport cpdReport) {
        return cpdReport.getStatus().getId();
    }

    private int getProteinCount(List<Component> component) {
        return component.stream()
                .filter(c -> Utils.notNull(c.getProteinCount()))
                .mapToInt(org.uniprot.core.proteome.Component::getProteinCount)
                .sum();
    }

    private void updateProteome(ProteomeDocument document, ProteomeEntry proteomeEntry) {
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