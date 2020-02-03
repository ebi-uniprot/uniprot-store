package org.uniprot.store.spark.indexer.keyword;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryImpl;
import org.uniprot.core.cv.keyword.impl.KeywordImpl;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class KeywordFileMapperTest {

    @Test
    void testECFileMapper() throws Exception {
        KeywordEntryImpl entry = new KeywordEntryImpl();
        KeywordImpl keyword = new KeywordImpl("kwId","kwAcc");
        entry.setKeyword(keyword);

        KeywordFileMapper mapper = new KeywordFileMapper();
        Tuple2<String, KeywordEntry> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("kwid", result._1);
        assertEquals(entry, result._2);
    }
}