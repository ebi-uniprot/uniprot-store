package org.uniprot.store.spark.indexer.literature.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 25/03/2021
 */
public class LiteratureUniProtKBJoin
        implements Function<
                Tuple2<Optional<LiteratureEntry>, Optional<Literature>>, LiteratureEntry> {
    private static final long serialVersionUID = -6400270119440899466L;

    @Override
    public LiteratureEntry call(Tuple2<Optional<LiteratureEntry>, Optional<Literature>> tuple)
            throws Exception {
        LiteratureEntryBuilder builder = new LiteratureEntryBuilder();
        if (tuple._1.isPresent()) {
            builder = LiteratureEntryBuilder.from(tuple._1.get());
        }
        if (tuple._2.isPresent()) {
            builder = builder.citation(tuple._2.get());
        }
        return builder.build();
    }
}
