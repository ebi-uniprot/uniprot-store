package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniprot.comment.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.suggest.SuggesterUtil;

import scala.Tuple2;

/**
 * This class map from flatFile entry string to an Iterator of ChebiEntry Ids found in all Cofactors
 * comments lines
 *
 * @author lgonzales
 * @since 2020-01-19
 */
public class FlatFileToCofactorChebi implements PairFlatMapFunction<String, String, String> {

    private static final long serialVersionUID = 2466198381636527109L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();

        String catalyticsComments =
                SuggesterUtil.getCommentLinesByType(entryStr, CommentType.COFACTOR);

        if (Utils.notNullNotEmpty(catalyticsComments)) {
            List<Comment> comments = SuggesterUtil.getComments(catalyticsComments);

            comments.stream()
                    .filter(cc -> cc.getCommentType().equals(CommentType.COFACTOR))
                    .forEach(
                            comment -> {
                                CofactorComment cofactorComment = (CofactorComment) comment;
                                if (cofactorComment.hasCofactors()) {
                                    cofactorComment
                                            .getCofactors()
                                            .forEach(
                                                    cofactor -> {
                                                        String id =
                                                                cofactor.getCofactorCrossReference()
                                                                        .getId();
                                                        if (id.startsWith("CHEBI:")) {
                                                            id = id.substring("CHEBI:".length());
                                                        }
                                                        result.add(
                                                                new Tuple2<>(
                                                                        id,
                                                                        cofactor.getCofactorCrossReference()
                                                                                .getId()));
                                                    });
                                }
                            });
        }
        return result.iterator();
    }
}
