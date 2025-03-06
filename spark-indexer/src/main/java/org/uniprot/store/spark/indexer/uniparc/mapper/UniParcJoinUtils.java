package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.core.util.Utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class UniParcJoinUtils {

    static Map<Long, TaxonomyEntry> getMappedTaxons(Iterable<TaxonomyEntry> organisms) {
        Map<Long, TaxonomyEntry> result = new HashMap<>();
        if (Utils.notNull(organisms)) {
            result =
                    StreamSupport.stream(organisms.spliterator(), false)
                            .collect(
                                    Collectors.toMap(
                                            Taxonomy::getTaxonId,
                                            java.util.function.Function.identity(),
                                            (tax1, tax2) -> tax1));
        }
        return result;
    }

    static UniParcCrossReference mapTaxonomy(
            UniParcCrossReference xref, Map<Long, TaxonomyEntry> taxonMap) {
        if (Utils.notNull(xref.getOrganism())) {
            TaxonomyEntry taxonomyEntry = taxonMap.get(xref.getOrganism().getTaxonId());
            if (taxonomyEntry != null) {
                UniParcCrossReferenceBuilder builder = UniParcCrossReferenceBuilder.from(xref);
                Organism taxonomy =
                        new OrganismBuilder()
                                .taxonId(taxonomyEntry.getTaxonId())
                                .scientificName(taxonomyEntry.getScientificName())
                                .commonName(taxonomyEntry.getCommonName())
                                .build();
                builder.organism(taxonomy);
                xref = builder.build();
            } else {
                log.warn("Unable to get mapped taxon:" + xref.getOrganism().getTaxonId());
            }
        }
        return xref;
    }
}
