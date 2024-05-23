package org.uniprot.store.spark.indexer.main.experimental;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;

import scala.Tuple2;

public class UniParcGroupByTaxonomyId
        implements Function<UniParcEntry, Iterable<Tuple2<String, Tuple2<Long, Long>>>> {

    @Override
    public Iterable<Tuple2<String, Tuple2<Long, Long>>> call(UniParcEntry entry) throws Exception {
        List<UniParcCrossReference> xrefs = entry.getUniParcCrossReferences();
        Map<Long, Long> taxonIdCount = new HashMap<>();
        for (UniParcCrossReference xref : xrefs) {
            if (xref.isActive() && Objects.nonNull(xref.getOrganism())) {
                long taxonId = xref.getOrganism().getTaxonId();
                taxonIdCount.putIfAbsent(taxonId, 0L);
                taxonIdCount.put(taxonId, taxonIdCount.get(taxonId) + 1L);
            }
        }
        return taxonIdCount.entrySet().stream()
                .map(
                        kv ->
                                new Tuple2<>(
                                        entry.getUniParcId().getValue(),
                                        new Tuple2<>(kv.getKey(), kv.getValue())))
                .collect(Collectors.toList());
    }
}
