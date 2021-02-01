package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/01/2021
 */
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
            Map<Long, TaxonomyEntry> mappedTaxons =
                    StreamSupport.stream(taxons.get().spliterator(), false)
                            .collect(
                                    Collectors.toMap(
                                            Taxonomy::getTaxonId,
                                            java.util.function.Function.identity(),
                                            (tax1, tax2) -> tax1));

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

    private UniParcCrossReference mapTaxonomy(
            UniParcCrossReference xref, Map<Long, TaxonomyEntry> taxonMap) {
        if (Utils.notNull(xref.getTaxonomy())) {
            TaxonomyEntry taxonomyEntry = taxonMap.get(xref.getTaxonomy().getTaxonId());
            if (taxonomyEntry != null) {
                UniParcCrossReferenceBuilder builder = UniParcCrossReferenceBuilder.from(xref);
                Organism taxonomy =
                        new OrganismBuilder()
                                .taxonId(taxonomyEntry.getTaxonId())
                                .scientificName(taxonomyEntry.getScientificName())
                                .commonName(taxonomyEntry.getCommonName())
                                .build();
                builder.taxonomy(taxonomy);
                xref = builder.build();
            } else {
                throw new IndexDataStoreException(
                        "Unable to get mapped taxon:" + xref.getTaxonomy().getTaxonId());
            }
        }
        return xref;
    }
}
