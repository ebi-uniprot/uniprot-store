package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import scala.Tuple2;

public class TaxonomyOtherNamesJoinMapper implements Function<Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>>, TaxonomyEntry> {

    private static final long serialVersionUID = 510880357682827997L;

    @Override
    public TaxonomyEntry call(Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>> tuple) throws Exception {
        TaxonomyEntry result = tuple._1;
        if(tuple._2.isPresent()){
            TaxonomyEntryBuilder builder = TaxonomyEntryBuilder.from(result);
            for(TaxonomyEntry otherNamesEntry : tuple._2.get()){
                String otherName = otherNamesEntry.getOtherNames().get(0);
                if(isOtherName(otherName, result)) {
                    builder.otherNamesAdd(otherName);
                }
            }
            result = builder.build();
        }
        return result;
    }

    private boolean isOtherName(String otherName, TaxonomyEntry entry) {
        return !otherName.isEmpty() &&
                !otherName.equals(entry.getScientificName()) &&
                !otherName.equals(entry.getCommonName()) &&
                !otherName.equals(entry.getMnemonic()) &&
                !entry.getSynonyms().contains(otherName);
    }
}
