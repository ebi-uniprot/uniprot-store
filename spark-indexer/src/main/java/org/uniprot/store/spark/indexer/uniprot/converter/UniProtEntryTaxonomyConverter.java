package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.uniprot.core.util.Utils.*;

import java.util.List;

import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.OrganismHost;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-05
 */
class UniProtEntryTaxonomyConverter {

    void convertOrganism(Organism organism, UniProtDocument document) {
        if (notNull(organism)) {
            int taxonomyId = Math.toIntExact(organism.getTaxonId());
            document.organismTaxId = taxonomyId;
//            document.content.add(String.valueOf(taxonomyId));
        }
    }

    void convertOrganismHosts(List<OrganismHost> hosts, UniProtDocument document) {
        hosts.forEach(
                host -> {
                    int taxonomyId = Math.toIntExact(host.getTaxonId());
                    document.organismHostIds.add(taxonomyId);
//                    document.content.add(String.valueOf(taxonomyId));
                });
    }
}
