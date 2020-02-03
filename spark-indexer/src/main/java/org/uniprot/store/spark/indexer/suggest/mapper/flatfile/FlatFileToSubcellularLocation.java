package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.*;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniprot.comment.Comment;
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.core.uniprot.comment.SubcellularLocationComment;
import org.uniprot.core.uniprot.comment.SubcellularLocationValue;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.suggest.SuggesterUtil;

import scala.Tuple2;

/**
 * This class map from flatFile entry string to an Iterator of Tuples of Subcellular Location Ids
 * found in Subcellular Location Comments lines
 * @author lgonzales
 * @since 2020-01-16
 */
public class FlatFileToSubcellularLocation implements PairFlatMapFunction<String, String, String> {
    private static final long serialVersionUID = 2243492555899364939L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();
        String subcellularComments =
                SuggesterUtil.getCommentLinesByType(entryStr, CommentType.SUBCELLULAR_LOCATION);

        if (Utils.notNullOrEmpty(subcellularComments)) {
            List<Comment> comments = SuggesterUtil.getComments(subcellularComments);

            comments.stream()
                    .filter(cc -> cc.getCommentType().equals(CommentType.SUBCELLULAR_LOCATION))
                    .forEach(
                            subcell -> {
                                SubcellularLocationComment comment =
                                        (SubcellularLocationComment) subcell;
                                if (comment.hasSubcellularLocations()) {
                                    comment.getSubcellularLocations()
                                            .forEach(
                                                    subcellularLocation -> {
                                                        if (subcellularLocation.hasLocation()) {
                                                            SubcellularLocationValue location =
                                                                    subcellularLocation
                                                                            .getLocation();
                                                            result.add(
                                                                    new Tuple2<>(
                                                                            getId(location),
                                                                            getId(location)));
                                                        }
                                                        if (subcellularLocation.hasOrientation()) {
                                                            SubcellularLocationValue orientation =
                                                                    subcellularLocation
                                                                            .getOrientation();
                                                            result.add(
                                                                    new Tuple2<>(
                                                                            getId(orientation),
                                                                            getId(orientation)));
                                                        }
                                                        if (subcellularLocation.hasTopology()) {
                                                            SubcellularLocationValue topology =
                                                                    subcellularLocation
                                                                            .getTopology();
                                                            result.add(
                                                                    new Tuple2<>(
                                                                            getId(topology),
                                                                            getId(topology)));
                                                        }
                                                    });
                                }
                            });
        }
        return result.iterator();
    }

    private String getId(SubcellularLocationValue location) {
        String[] splitLocation = location.getValue().split(",");
        return splitLocation[splitLocation.length - 1].trim().toLowerCase();
    }
}
