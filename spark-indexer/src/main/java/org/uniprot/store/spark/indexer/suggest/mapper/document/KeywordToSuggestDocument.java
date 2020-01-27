package org.uniprot.store.spark.indexer.suggest.mapper.document;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.keyword.Keyword;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-16
 */
public class KeywordToSuggestDocument
        implements Function<Tuple2<String, KeywordEntry>, Iterable<SuggestDocument>> {
    private static final long serialVersionUID = -4656249467275467458L;

    @Override
    public Iterable<SuggestDocument> call(Tuple2<String, KeywordEntry> tuple) throws Exception {
        List<SuggestDocument> result = new ArrayList<>();
        KeywordEntry keyword = tuple._2;
        SuggestDocument keywordDoc =
                SuggestDocument.builder()
                        .id(keyword.getKeyword().getAccession())
                        .value(keyword.getKeyword().getId())
                        .dictionary(SuggestDictionary.KEYWORD.name())
                        .build();
        result.add(keywordDoc);

        Keyword kc = keyword.getCategory();
        if (Utils.notNull(kc)) {
            SuggestDocument categoryDoc =
                    SuggestDocument.builder()
                            .id(kc.getAccession())
                            .value(kc.getId())
                            .dictionary(SuggestDictionary.KEYWORD.name())
                            .build();
            result.add(categoryDoc);
        }
        return result;
    }
}
