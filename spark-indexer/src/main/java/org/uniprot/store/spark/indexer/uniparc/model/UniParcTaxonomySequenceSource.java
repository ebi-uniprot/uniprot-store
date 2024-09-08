package org.uniprot.store.spark.indexer.uniparc.model;

import org.uniprot.core.taxonomy.TaxonomyEntry;

import java.util.Map;
import java.util.Set;

public record UniParcTaxonomySequenceSource(Iterable<TaxonomyEntry> organisms,
                                            Map<String, Set<String>> sequenceSources) {

}
