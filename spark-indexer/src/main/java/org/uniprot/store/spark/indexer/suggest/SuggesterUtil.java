package org.uniprot.store.spark.indexer.suggest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.cc.CcLineConverter;
import org.uniprot.core.flatfile.parser.impl.cc.cclineobject.CcLineObject;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.comment.CommentType;

/**
 * Utility class for Suggest index
 *
 * @author lgonzales
 * @since 2020-01-17
 */
public class SuggesterUtil {

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
}
