package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.uniprot.core.util.Utils.notNull;

import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.CrossReference;
import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.proteome.*;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
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
        document.strain = proteomeEntry.getStrain();
        Optional.ofNullable(proteomeEntry.getAnnotationScore())
                .ifPresent(score -> document.score = score);
        document.genomeAccession = getGenomeAccession(proteomeEntry);
        document.genomeAssembly = getGenomeAssembly(proteomeEntry);
        document.proteinCount = getProteinCount(proteomeEntry.getComponents());
        Optional.ofNullable(proteomeEntry.getProteomeCompletenessReport())
                .ifPresent(
                        report -> {
                            document.busco = getBuscoPercentage(report.getBuscoReport());
                            document.cpd = getCPD(report.getCPDReport());
                        });
        updateProteomeType(document, proteomeEntry);
        Optional<Taxonomy> taxonomy = Optional.ofNullable(proteomeEntry.getTaxonomy());
        document.organismTaxId = taxonomy.map(tx -> (int) tx.getTaxonId()).orElse(0);
        taxonomy.ifPresent(
                tax -> updateOrganismFields(document, tax, proteomeEntry.getTaxonLineages()));
        Optional.ofNullable(proteomeEntry.getSuperkingdom())
                .ifPresent(sk -> document.superkingdom = StringUtils.capitalize(sk.getName()));
        document.proteomeStored =
                ProteomeJsonConfig.getInstance()
                        .getFullObjectMapper()
                        .writeValueAsBytes(proteomeEntry);
        return document;
    }

    private void updateOrganismFields(
            ProteomeDocument proteomeDocument,
            Taxonomy organism,
            List<TaxonomyLineage> taxonLineages) {
        List<String> organismNames = getOrganismNames(organism);
        proteomeDocument.organismName.addAll(organismNames);
        proteomeDocument.organismTaxon.addAll(organismNames);
        proteomeDocument.organismSort = organism.getScientificName();
        proteomeDocument.taxLineageIds.add(Math.toIntExact(organism.getTaxonId()));

        if (!taxonLineages.isEmpty()) {
            taxonLineages.forEach(lineage -> updateLineageTaxonomy(proteomeDocument, lineage));
        } else {
            log.warn("Unable to find organism lineage for: " + proteomeDocument.organismTaxId);
        }
    }

    private List<String> getOrganismNames(Taxonomy organism) {
        List<String> organismNames = new LinkedList<>();
        organismNames.add(organism.getScientificName());
        if (organism.hasCommonName()) {
            organismNames.add(organism.getCommonName());
        }
        if (organism.hasSynonyms()) {
            organismNames.addAll(organism.getSynonyms());
        }
        return organismNames;
    }

    private void updateLineageTaxonomy(ProteomeDocument document, TaxonomyLineage lineage) {
        document.taxLineageIds.add(Math.toIntExact(lineage.getTaxonId()));
        document.organismTaxon.add(lineage.getScientificName());
        if (lineage.hasCommonName()) {
            document.organismTaxon.add(lineage.getCommonName());
        }
    }

    private Float getBuscoPercentage(BuscoReport buscoReport) {
        Float buscoCompletedPercentage = null;
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

    private void updateProteomeType(ProteomeDocument document, ProteomeEntry proteomeEntry) {
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
                .map(
                        c ->
                                c.getProteomeCrossReferences().stream()
                                        .filter(
                                                cr ->
                                                        ProteomeDatabase.GENOME_ACCESSION.equals(
                                                                cr.getDatabase()))
                                        .collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .map(CrossReference::getId)
                .collect(Collectors.toList());
    }
}
