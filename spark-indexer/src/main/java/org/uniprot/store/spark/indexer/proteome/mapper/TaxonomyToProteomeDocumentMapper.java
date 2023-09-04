package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil.truncatedSortValue;

import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

import scala.Tuple2;

@Slf4j
public class TaxonomyToProteomeDocumentMapper
        implements Function<Tuple2<ProteomeDocument, Optional<TaxonomyEntry>>, ProteomeDocument> {
    @Override
    public ProteomeDocument call(
            Tuple2<ProteomeDocument, Optional<TaxonomyEntry>> proteomeIdTaxEntryOptTuple)
            throws Exception {
        Optional<TaxonomyEntry> organism = proteomeIdTaxEntryOptTuple._2;
        ProteomeDocument proteomeDocument = proteomeIdTaxEntryOptTuple._1;

        if (organism.isPresent()) {
            updateOrganismFields(proteomeDocument, organism.get());
        } else {
            log.warn("No Organism Id exists for proteome with id " + proteomeDocument.upid);
        }
        return proteomeDocument;
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

    private void updateLineageTaxonomy(ProteomeDocument doc, TaxonomyLineage lineage) {
        doc.taxLineageIds.add(Math.toIntExact(lineage.getTaxonId()));
        doc.organismTaxon.add(lineage.getScientificName());
        if (lineage.hasCommonName()) {
            doc.organismTaxon.add(lineage.getCommonName());
        }
    }
}
