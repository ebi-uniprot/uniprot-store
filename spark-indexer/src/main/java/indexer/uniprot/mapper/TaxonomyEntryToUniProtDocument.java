package indexer.uniprot.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-11-12
 */
@Slf4j
public class TaxonomyEntryToUniProtDocument implements Function<Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>>, UniProtDocument> {
    private static final long serialVersionUID = -7030543979688452323L;

    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple) throws Exception {
        UniProtDocument doc = tuple._1;
        if (tuple._2.isPresent()) {
            Map<Long, TaxonomyEntry> taxonomyEntryMap = new HashMap<>();
            tuple._2.get().forEach(taxonomyEntry -> {
                taxonomyEntryMap.put(taxonomyEntry.getTaxonId(), taxonomyEntry);
            });

            TaxonomyEntry organism = taxonomyEntryMap.get((long) doc.organismTaxId);
            if (organism != null) {
                updateOrganismFields(doc, organism);
            } else {
                log.warn("Unable to find organism id " + doc.organismTaxId + " in mapped organisms " + taxonomyEntryMap.keySet());
            }

            if (Utils.notNullOrEmpty(doc.organismHostIds)) {
                doc.organismHostIds.forEach(taxId -> {
                    TaxonomyEntry organismHost = taxonomyEntryMap.get((long) taxId);
                    if (organismHost != null) {
                        doc.organismHostNames.addAll(getOrganismNames(organismHost));
                    } else {
                        log.warn("Unable to find organism host id " + taxId + " in mapped organisms " + taxonomyEntryMap.keySet());
                    }
                });
                doc.content.addAll(doc.organismHostNames);
            }
        } else {
            log.warn("Unable to join organism for " + doc.organismTaxId + " in document: " + doc.accession);
        }
        return doc;
    }

    private void updateOrganismFields(UniProtDocument doc, TaxonomyEntry organism) {
        doc.organismName.addAll(getOrganismNames(organism));
        if (organism.hasLineage()) {
            organism.getLineage()
                    .forEach(lineage -> {
                        doc.taxLineageIds.add(new Long(lineage.getTaxonId()).intValue());
                        doc.organismTaxon.add(lineage.getScientificName());
                        if (lineage.hasCommonName()) {
                            doc.organismTaxon.add(lineage.getCommonName());
                        }
                    });
        } else {
            log.warn("Unable to find organism lineage for: " + doc.organismTaxId);
        }
        doc.content.addAll(doc.organismName);
        doc.content.addAll(doc.taxLineageIds.stream().map(String::valueOf).collect(Collectors.toList()));
        doc.content.addAll(doc.organismTaxon);
    }

    private List<String> getOrganismNames(TaxonomyEntry organism) {
        List<String> organismNames = new ArrayList<>();
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
}
