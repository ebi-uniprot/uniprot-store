package org.uniprot.store.spark.indexer.uniref.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.document.uniref.UniRefDocument;

import scala.Tuple2;

/**
 * Add Joined Taxonomy Lineage ids and names to UniRefDocument
 *
 * @author lgonzales
 * @since 2020-02-08
 */
public class UniRefTaxonomyJoin
        implements Function<Tuple2<UniRefDocument, Optional<TaxonomyEntry>>, UniRefDocument> {
    private static final long serialVersionUID = -3109382698877175332L;

    @Override
    public UniRefDocument call(Tuple2<UniRefDocument, Optional<TaxonomyEntry>> tuple)
            throws Exception {
        UniRefDocument result = tuple._1;
        if (tuple._2.isPresent()) {
            UniRefDocument.UniRefDocumentBuilder builder = result.toBuilder();
            TaxonomyEntry organism = tuple._2.get();
            organism.getLineages()
                    .forEach(
                            lineage -> {
                                builder.taxLineageId(new Long(lineage.getTaxonId()).intValue());
                                builder.organismTaxon(lineage.getScientificName());
                                if (lineage.hasCommonName()) {
                                    builder.organismTaxon(lineage.getCommonName());
                                }
                            });
            result = builder.build();
        }
        return result;
    }
}
