package indexer.subcell;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.SubcellularLocationFileReader;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.core.uniprot.comment.SubcellularLocationComment;
import org.uniprot.core.uniprot.comment.builder.SubcellularLocationBuilder;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-10-13
 */
public class SubcellularLocationMapReader {

    public static JavaPairRDD<String, UniProtEntry> mapSubcellularLocation(JavaPairRDD<String, UniProtEntry> uniRefRDD, ResourceBundle applicationConfig) {
        final Map<String, SubcellularLocationEntry> subcellMap = readSubcellularLocationMap(applicationConfig);
        return (JavaPairRDD<String, UniProtEntry>) uniRefRDD.mapValues(uniprotEntry -> {

            List<SubcellularLocationComment> comments = uniprotEntry.getCommentByType(CommentType.SUBCELLULAR_LOCATION);
            comments.stream().filter(SubcellularLocationComment::hasSubcellularLocations).forEach(comment -> {
                comment.getSubcellularLocations().forEach(subcellularLocation -> {
                    SubcellularLocationBuilder buider = new SubcellularLocationBuilder().from(subcellularLocation);
                    if (subcellularLocation.hasLocation()) {
                        //buider.location(new SubcellularLocationv)
                    }
                    if (subcellularLocation.hasOrientation()) {

                    }
                    if (subcellularLocation.hasTopology()) {

                    }
                });
            });


            return uniprotEntry;
        });
    }

    private static Map<String, SubcellularLocationEntry> readSubcellularLocationMap(ResourceBundle applicationConfig) {
        SubcellularLocationFileReader reader = new SubcellularLocationFileReader();
        List<SubcellularLocationEntry> entries = reader.parse(applicationConfig.getString("subcell.file.path"));
        return entries.stream().collect(Collectors.toMap(SubcellularLocationEntry::getId, sl -> sl));
    }

}
