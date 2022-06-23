package org.uniprot.store.spark.indexer.suggest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.cc.CcLineConverter;
import org.uniprot.core.flatfile.parser.impl.cc.cclineobject.CcLineObject;
import org.uniprot.core.flatfile.parser.impl.ft.FtLineConverter;
import org.uniprot.core.flatfile.parser.impl.ft.FtLineObject;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.feature.UniProtKBFeature;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;

/**
 * Utility class for Suggest index
 *
 * @author lgonzales
 * @since 2020-01-17
 */
public class SuggesterUtil {

    private SuggesterUtil() {}

    public static String getCommentLinesByType(String entryStr, CommentType type) {
        String commentLines =
                Arrays.stream(entryStr.split("\n"))
                        .filter(line -> line.startsWith("CC  "))
                        .filter(
                                line ->
                                        !line.startsWith("CC   ---")
                                                && !line.startsWith("CC   Copyrighted")
                                                && !line.startsWith("CC   Distribute"))
                        .collect(Collectors.joining("\n"));

        return Arrays.stream(commentLines.split("CC   -!- "))
                .filter(cm -> cm.startsWith(type.getDisplayName()))
                .map(cm -> "CC   -!- " + cm)
                .collect(Collectors.joining(""));
    }

    public static List<Comment> getComments(String commentLines) {
        final UniprotKBLineParser<CcLineObject> ccParser =
                new DefaultUniprotKBLineParserFactory().createCcLineParser();
        CcLineObject ccLineObject = ccParser.parse(commentLines + "\n");
        CcLineConverter converter = new CcLineConverter(new HashMap<>(), new HashMap<>());
        return converter.convert(ccLineObject);
    }

    public static String getFeatureLines(String entryStr) {
        return Arrays.stream(entryStr.split("\n"))
                .filter(line -> line.startsWith("FT  "))
                .collect(Collectors.joining("\n"));
    }

    public static List<UniProtKBFeature> getFeaturesByType(
            String entryStr, UniprotKBFeatureType type) {
        String featureLines =
                Arrays.stream(entryStr.split("\n"))
                        .filter(line -> line.startsWith("FT  "))
                        .collect(Collectors.joining("\n"));
        final UniprotKBLineParser<FtLineObject> ftParser =
                new DefaultUniprotKBLineParserFactory().createFtLineParser();

        FtLineObject ftLineObject = ftParser.parse(featureLines + "\n");
        FtLineConverter converter = new FtLineConverter();
        List<UniProtKBFeature> features = converter.convert(ftLineObject);
        return features.stream()
                .filter(feature -> feature.getType() == type)
                .collect(Collectors.toList());
    }
}
