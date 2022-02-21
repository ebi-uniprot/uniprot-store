package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.CrossReference;
import org.uniprot.core.uniprotkb.comment.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.suggest.SuggesterUtil;

import scala.Tuple2;

public class FlatFileToCatalyticActivityRheaComp
        implements PairFlatMapFunction<String, String, String> {

    private static final long serialVersionUID = 6342915693294781842L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();

        String catalyticsComments =
                SuggesterUtil.getCommentLinesByType(entryStr, CommentType.CATALYTIC_ACTIVITY);

        if (Utils.notNullNotEmpty(catalyticsComments)) {
            List<Comment> comments = SuggesterUtil.getComments(catalyticsComments);

            comments.stream()
                    .filter(cc -> cc.getCommentType().equals(CommentType.CATALYTIC_ACTIVITY))
                    .map(catalytic -> (CatalyticActivityComment) catalytic)
                    .forEach(catalytic -> addRheaComp(result, catalytic));
        }
        return result.iterator();
    }

    private void addRheaComp(
            List<Tuple2<String, String>> result, CatalyticActivityComment comment) {
        Reaction reaction = comment.getReaction();
        if (reaction.hasReactionCrossReferences()) {
            List<CrossReference<ReactionDatabase>> references =
                    reaction.getReactionCrossReferences();
            references.stream()
                    .filter(ref -> ref.getDatabase() == ReactionDatabase.RHEA)
                    .forEach(
                            val -> {
                                String id = val.getId();
                                if (id.startsWith("RHEA-COMP:")) {
                                    result.add(new Tuple2<>(id, val.getId()));
                                }
                            });
        }
    }
}
