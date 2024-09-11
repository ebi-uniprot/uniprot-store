package org.uniprot.store.spark.indexer.uniparc.model;

import java.util.Map;
import java.util.Set;

import org.uniprot.core.taxonomy.TaxonomyEntry;

public record UniParcTaxonomySequenceSource(
        Iterable<TaxonomyEntry> organisms, Map<String, Set<String>> sequenceSources) {}
