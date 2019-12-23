package indexer.taxonomy;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;

import scala.Tuple2;

/**
 * This class is responsible to map a List of TaxonomyLineage into the TaxonomyEntry
 *
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyJoinMapper
        implements Function<Tuple2<TaxonomyEntry, List<TaxonomyLineage>>, TaxonomyEntry>,
                Serializable {

    private static final long serialVersionUID = 7479649182382873120L;

    /**
     * @param tuple is a Tuple of {key=TaxonomyEntry , value= List of TaxonomyLineage}.
     * @return TaxonomyEntry with all TaxonomyLineages.
     */
    @Override
    public TaxonomyEntry call(Tuple2<TaxonomyEntry, List<TaxonomyLineage>> tuple) throws Exception {
        TaxonomyEntry entry = tuple._1;
        List<TaxonomyLineage> lineage = tuple._2;

        TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder().from(entry);
        builder.lineage(lineage);

        return builder.build();
    }
}
