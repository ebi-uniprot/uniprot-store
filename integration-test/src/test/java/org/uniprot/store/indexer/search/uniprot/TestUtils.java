package org.uniprot.store.indexer.search.uniprot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.search.field.QueryBuilder;

/** Contains utility methods that aid in testing */
final class TestUtils {
    private static SupportingDataMap dataMap =
            new SupportingDataMapImpl("keywlist.txt", "humdisease.txt", null, null);
    private static EntryObjectConverter entryObjectConverter =
            new EntryObjectConverter(dataMap, true);

    private TestUtils() {}

    public static UniProtKBEntry convertToUniProtEntry(UniProtEntryObjectProxy objectProxy) {
        return objectProxy.convertToUniProtEntry(entryObjectConverter);
    }

    public static InputStream getResourceAsStream(String resourcePath) {
        return TestUtils.class.getResourceAsStream(resourcePath);
    }

    public static String convertInputStreamToString(InputStream stream) throws IOException {
        StringBuilder sb = new StringBuilder();

        try (InputStreamReader isr = new InputStreamReader(stream);
                BufferedReader br = new BufferedReader(isr)) {
            String line;

            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * Creates a flatfile like line which contains multiple elements within it. <br>
     * Examples of such lines:
     *
     * <ul>
     *   <li>Keywords KW
     *   <li>Organism classification (OC)
     * </ul>
     *
     * @param lineStart the starting string of the line
     * @param separator the string character that separates the elements
     * @param lineTerminator indicates that all of the elements have been inserted
     * @param elements the elements to populate the line
     * @return the populated line
     */
    public static String createMultiElementFFLine(
            String lineStart, String separator, String lineTerminator, String... elements) {
        StringBuilder line = new StringBuilder(lineStart);

        if (elements.length > 0) {
            for (String element : elements) {
                line.append(element).append(separator).append(" ");
            }

            line.replace(line.length() - 2, line.length(), lineTerminator);
        } else {
            line.append(".");
        }

        return line.toString();
    }

    private static final String COMMENT_DYNAMIC_PREFIX = "cc_";

    public static String query(SearchFieldItem field, String fieldValue) {
        return QueryBuilder.query(field.getFieldName(), fieldValue);
    }

    public static String comments(CommentType commentType, String value) {
        String field =
                COMMENT_DYNAMIC_PREFIX + commentType.name().toLowerCase().replaceAll(" ", "_");
        return QueryBuilder.query(field, value);
    }

    private static final String FEATURE_DYNAMIC_PREFIX = "ft_";
    private static final String FT_LENGTH_DYNAMIC_PREFIX = "ftlen_";

    public static String features(UniprotKBFeatureType featureType, String value) {
        String field =
                FEATURE_DYNAMIC_PREFIX + featureType.getName().toLowerCase().replaceAll(" ", "_");
        return QueryBuilder.query(field, value, true, false);
    }

    public static String featureLength(UniprotKBFeatureType featureType, int start, int end) {
        String field =
                FT_LENGTH_DYNAMIC_PREFIX + featureType.getName().toLowerCase().replaceAll(" ", "_");
        return QueryBuilder.rangeQuery(field, start, end);
    }
}
