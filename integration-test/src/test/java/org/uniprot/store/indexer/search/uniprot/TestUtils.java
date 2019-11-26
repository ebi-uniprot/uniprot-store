package org.uniprot.store.indexer.search.uniprot;

import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.core.uniprot.feature.FeatureType;
import org.uniprot.store.search.domain2.SearchField;
import org.uniprot.store.search.field.QueryBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Contains utility methods that aid in testing
 */
final class TestUtils {
	private static SupportingDataMap dataMap = new SupportingDataMapImpl("keywlist.txt",
			"humdisease.txt", null, null	
			);
    private static EntryObjectConverter entryObjectConverter = new EntryObjectConverter(dataMap, true);

    private TestUtils() {
    }

    public static UniProtEntry convertToUniProtEntry(UniProtEntryObjectProxy objectProxy) {
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
     * Creates a flatfile like line which contains multiple elements within it.
     * <br/>
     * Examples of such lines:
     * <ul>
     * <li>Keywords KW</li>
     * <li>Organism classification (OC)</li>
     * </ul>
     *
     * @param lineStart      the starting string of the line
     * @param separator      the string character that separates the elements
     * @param lineTerminator indicates that all of the elements have been inserted
     * @param elements       the elements to populate the line
     * @return the populated line
     */
    public static String createMultiElementFFLine(String lineStart, String separator, String lineTerminator,
                                                  String... elements) {
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
	private static final String CC_EVIDENCE_DYNAMIC_PREFIX = "ccev_";
	
    public static String query(SearchField field, String fieldValue) {
		return QueryBuilder.query(field.getName(), fieldValue);
	}
    public static String comments(CommentType commentType, String value) {
		String field = COMMENT_DYNAMIC_PREFIX + commentType.name().toLowerCase().replaceAll(" ", "_");
		return QueryBuilder.query(field, value);
	}
    public static String commentEvidence(CommentType commentType, String evidence) {
		String field = CC_EVIDENCE_DYNAMIC_PREFIX + commentType.name().toLowerCase().replaceAll(" ", "_");
		return QueryBuilder.query(field, evidence);
	}
    private static final String FEATURE_DYNAMIC_PREFIX = "ft_";
    private static final String FT_EV_DYNAMIC_PREFIX = "ftev_";
    private static final String FT_LENGTH_DYNAMIC_PREFIX = "ftlen_";
    public static String features(FeatureType featureType, String value) {
    	String field = FEATURE_DYNAMIC_PREFIX +featureType.getName().toLowerCase().replaceAll(" ", "_");
    	return QueryBuilder.query(field, value, true, false);
    }
    public static String featureLength(FeatureType featureType, int start, int end) {
    	  String field = FT_LENGTH_DYNAMIC_PREFIX + featureType.getName().toLowerCase().replaceAll(" ", "_");
    	  return QueryBuilder.rangeQuery(field, start, end);
    }
    public static String featureEvidence(FeatureType featureType, String value) {
  	  String field = FT_EV_DYNAMIC_PREFIX +featureType.getName().toLowerCase().replaceAll(" ", "_");
  	  return QueryBuilder.query(field, value);
  }
}