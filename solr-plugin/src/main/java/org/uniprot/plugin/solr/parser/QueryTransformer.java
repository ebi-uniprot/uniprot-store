package org.uniprot.plugin.solr.parser;

import lombok.Builder;
import lombok.Singular;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.util.List;
import java.util.function.Consumer;

/**
 * Created 18/08/2020
 *
 * @author Edd
 */
@Builder
public class QueryTransformer {
    @Singular private final List<Consumer<Query>> queryTransformers;

    public void transform(Query query) {
        QueryVisitor queryVisitor =
                new QueryVisitor() {
                    @Override
                    public void consumeTerms(Query query, Term... terms) {
                        queryTransformers.forEach(t -> t.accept(query));
                    }
                };

        query.visit(queryVisitor);
    }
}
