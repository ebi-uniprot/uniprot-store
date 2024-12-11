package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.uniprot.store.spark.indexer.uniparc.mapper.UniParcJoinUtils.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;

import scala.Tuple2;

public class UniParcEntryTaxonomyJoin
        implements Function<
                Tuple2<String, Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>>>,
                UniParcEntry> {

    private static final long serialVersionUID = -6093868325315670369L;

    @Override
    public UniParcEntry call(
            Tuple2<String, Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>>> tuple)
            throws Exception {
        UniParcEntry result = tuple._2._1;
        Optional<Iterable<TaxonomyEntry>> taxons = tuple._2._2;
        if (taxons.isPresent()) {
            Map<Long, TaxonomyEntry> mappedTaxons = getMappedTaxons(taxons.get());

            UniParcEntryBuilder builder = UniParcEntryBuilder.from(result);
            List<UniParcCrossReference> mappedXRefs =
                    result.getUniParcCrossReferences().stream()
                            .map(xref -> mapTaxonomy(xref, mappedTaxons))
                            .collect(Collectors.toList());
            builder.uniParcCrossReferencesSet(mappedXRefs);
            result = builder.build();
        }
        return result;
    }
}
