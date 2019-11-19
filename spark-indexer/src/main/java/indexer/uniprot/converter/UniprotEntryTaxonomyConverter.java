package indexer.uniprot.converter;

import org.uniprot.core.uniprot.taxonomy.Organism;
import org.uniprot.core.uniprot.taxonomy.OrganismHost;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.List;

/**
 * @author lgonzales
 * @since 2019-09-05
 */
class UniprotEntryTaxonomyConverter {

    void convertOrganism(Organism organism, UniProtDocument document) {
        if (organism != null) {
            int taxonomyId = Math.toIntExact(organism.getTaxonId());
            document.organismTaxId = taxonomyId;
            document.content.add(String.valueOf(taxonomyId));
        }
    }

    void convertOrganismHosts(List<OrganismHost> hosts, UniProtDocument document) {
        hosts.forEach(host -> {
            int taxonomyId = Math.toIntExact(host.getTaxonId());
            document.organismHostIds.add(taxonomyId);
            document.content.add(String.valueOf(taxonomyId));
        });
    }

}
