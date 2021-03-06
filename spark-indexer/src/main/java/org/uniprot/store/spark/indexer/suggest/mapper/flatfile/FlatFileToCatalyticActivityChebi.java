package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.*;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.CrossReference;
import org.uniprot.core.uniprotkb.comment.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.suggest.SuggesterUtil;

import scala.Tuple2;

/**
 * This class map from flatFile entry string to an Iterator of ChebiEntry Ids found in all Catalytic
 * Activities comments lines
 *
 * @author lgonzales
 * @since 2020-01-17
 */
public class FlatFileToCatalyticActivityChebi
        implements PairFlatMapFunction<String, String, String> {

    private static final long serialVersionUID = 313398807686234741L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();

        String catalyticsComments =
                SuggesterUtil.getCommentLinesByType(entryStr, CommentType.CATALYTIC_ACTIVITY);

        if (Utils.notNullNotEmpty(catalyticsComments)) {
            List<Comment> comments = SuggesterUtil.getComments(catalyticsComments);

            comments.stream()
                    .filter(cc -> cc.getCommentType().equals(CommentType.CATALYTIC_ACTIVITY))
                    .forEach(
                            catalytic -> {
                                CatalyticActivityComment comment =
                                        (CatalyticActivityComment) catalytic;
                                Reaction reaction = comment.getReaction();
                                if (reaction.hasReactionCrossReferences()) {
                                    List<CrossReference<ReactionDatabase>> references =
                                            reaction.getReactionCrossReferences();
                                    references.stream()
                                            .filter(
                                                    ref ->
                                                            ref.getDatabase()
                                                                    == ReactionDatabase.CHEBI)
                                            .forEach(
                                                    val -> {
                                                        String id = val.getId();
                                                        if (val.getId().startsWith("CHEBI:")) {
                                                            id = id.substring("CHEBI:".length());
                                                        }
                                                        result.add(new Tuple2<>(id, val.getId()));
                                                    });
                                }
                            });
        }
        return result.iterator();
    }
}
