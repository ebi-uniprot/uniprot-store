package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serializable;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

import scala.Tuple2;

/**
 * This class Merge a list of joined TaxonomyEntry lineages to UniParcDocument.
 *
 * @author lgonzales
 * @since 2020-02-20
 */
public class UniParcTaxonomyJoin
        implements Serializable,
                Function<
                        Tuple2<UniParcDocument, Optional<Iterable<TaxonomyEntry>>>,
                        UniParcDocument> {

    private static final long serialVersionUID = 2284525913459775507L;

    @Override
    public UniParcDocument call(Tuple2<UniParcDocument, Optional<Iterable<TaxonomyEntry>>> tuple)
            throws Exception {
        UniParcDocument result = tuple._1;
        if (tuple._2.isPresent()) {
            UniParcDocument.UniParcDocumentBuilder builder = result.toBuilder();
            if (Utils.notNull(tuple._2) && tuple._2.isPresent()) {
                Iterable<TaxonomyEntry> organismList = tuple._2.get();
                organismList.forEach(
                        organism -> {
                            organism.getLineages()
                                    .forEach(
                                            lineage -> {
                                                builder.taxLineageId((int) lineage.getTaxonId());
                                                builder.organismTaxon(lineage.getScientificName());
                                                if (lineage.hasCommonName()) {
                                                    builder.organismTaxon(lineage.getCommonName());
                                                }
                                            });
                        });
            }
            result = builder.build();
        }
        return result;
    }
}
