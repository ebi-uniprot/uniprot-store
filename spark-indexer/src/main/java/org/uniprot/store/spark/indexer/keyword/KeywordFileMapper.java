package org.uniprot.store.spark.indexer.keyword;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.keyword.KeywordEntry;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

/**
 * This class map a keywlist.txt string lines of an Keyword Entry To a Tuple2{key=keywordId,
 * value={@link KeywordEntry}}
 *
 * @author lgonzales
 * @since 2019-11-16
 */
@Slf4j
public class KeywordFileMapper implements PairFunction<KeywordEntry, String, KeywordEntry> {

    private static final long serialVersionUID = -724929485479520170L;

    /**
     * @param entry KeywordEntry
     * @return a Tuple2{key=keywordId, value={@link KeywordEntry}}
     * @throws Exception
     */
    @Override
    public Tuple2<String, KeywordEntry> call(KeywordEntry entry) throws Exception {
        return new Tuple2<>(entry.getKeyword().getName().toLowerCase(), entry);
    }
}
