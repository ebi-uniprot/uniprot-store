package indexer.taxonomy;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyJoinMapper implements Function<Tuple2<TaxonomyEntry, List<TaxonomyLineage>>, TaxonomyEntry>, Serializable {

    private static final long serialVersionUID = 7479649182382873120L;

    @Override
    public TaxonomyEntry call(Tuple2<TaxonomyEntry, List<TaxonomyLineage>> tuple) throws Exception {
        TaxonomyEntry entry = tuple._1;
        List<TaxonomyLineage> lineage = tuple._2;

        TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder().from(entry);
        builder.lineage(lineage);

        return builder.build();
    }
}
