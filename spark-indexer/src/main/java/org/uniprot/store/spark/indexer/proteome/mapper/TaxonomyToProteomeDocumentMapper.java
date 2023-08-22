package org.uniprot.store.spark.indexer.proteome.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

import java.util.*;

import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil.truncatedSortValue;

@Slf4j
public class TaxonomyToProteomeDocumentMapper implements Function<ProteomeDocument, ProteomeDocument> {
    private final Map<String, TaxonomyEntry> taxonomyEntryMap;

    public TaxonomyToProteomeDocumentMapper(Map<String, TaxonomyEntry> taxonomyEntryMap) {
        this.taxonomyEntryMap = taxonomyEntryMap;
    }

    @Override
    public ProteomeDocument call(ProteomeDocument proteomeDocument) throws Exception {
        TaxonomyEntry organism = taxonomyEntryMap.get(String.valueOf(proteomeDocument.organismTaxId));

        if (Utils.notNull(organism)) {
            updateOrganismFields(proteomeDocument, organism);
        } else {
            log.warn(
                    getWarnMessage(
                            proteomeDocument.upid, taxonomyEntryMap.keySet(), proteomeDocument.organismTaxId));
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
            organism.getLineages().forEach(lineage -> updateLineageTaxonomy(proteomeDocument, lineage));
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

    private String getWarnMessage(String upid, Set<String> keys, int organismTaxId) {
        return "Unable to find organism id"
                + organismTaxId
                + " in mapped organisms "
                + keys
                + "for upid "
                + upid;
    }
}
