package org.uniprot.store.spark.indexer.uniparc.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.uniprot.core.taxonomy.TaxonomyEntry;

@Getter
@EqualsAndHashCode
public class UniParcTaxonomySequenceSource implements Serializable {

    @Serial
    private static final long serialVersionUID = 1308657310069336971L;
    private final Iterable<TaxonomyEntry> organisms;
    private final Map<String, Set<String>> sequenceSources;

    public UniParcTaxonomySequenceSource(Iterable<TaxonomyEntry> organisms, Map<String, Set<String>> sequenceSources) {
        this.organisms = organisms;
        this.sequenceSources = sequenceSources;
    }

}
