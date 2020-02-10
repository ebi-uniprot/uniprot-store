package org.uniprot.store.spark.indexer.keyword;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.impl.KeywordImpl;
import org.uniprot.cv.keyword.KeywordEntry;
import org.uniprot.cv.keyword.impl.KeywordEntryImpl;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class KeywordFileMapperTest {

    @Test
    void testECFileMapper() throws Exception {
        KeywordEntryImpl entry = new KeywordEntryImpl();
        KeywordImpl keyword = new KeywordImpl("kwId", "kwAcc");
        entry.setKeyword(keyword);

        KeywordFileMapper mapper = new KeywordFileMapper();
        Tuple2<String, KeywordEntry> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("kwid", result._1);
        assertEquals(entry, result._2);
    }
}
