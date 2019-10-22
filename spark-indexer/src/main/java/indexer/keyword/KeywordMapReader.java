package indexer.keyword;

import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.KeywordFileReader;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-10-13
 */
public class KeywordMapReader {

    public static List<KeywordEntry> readKeywordList(ResourceBundle applicationConfig) {
        return new KeywordFileReader().parse(applicationConfig.getString("keyword.file.path"));
    }

    public static Map<String, KeywordEntry> readKeywordMap(ResourceBundle applicationConfig) {
        List<KeywordEntry> keywordEntries = readKeywordList(applicationConfig);
        return keywordEntries.stream().collect(Collectors.toMap(kw -> kw.getKeyword().getId(), kw -> kw));
    }

}
