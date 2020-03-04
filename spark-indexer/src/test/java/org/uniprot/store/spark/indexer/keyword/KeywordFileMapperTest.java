package org.uniprot.store.spark.indexer.keyword;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.KeywordId;
import org.uniprot.core.cv.keyword.builder.KeywordEntryBuilder;
import org.uniprot.core.cv.keyword.builder.KeywordEntryKeywordBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class KeywordFileMapperTest {

    @Test
    void testECFileMapper() throws Exception {
        KeywordId keyword = new KeywordEntryKeywordBuilder().id("kwId").accession("kwAcc").build();
        KeywordEntry entry = new KeywordEntryBuilder().keyword(keyword).build();

        KeywordFileMapper mapper = new KeywordFileMapper();
        Tuple2<String, KeywordEntry> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("kwid", result._1);
        assertEquals(entry, result._2);
    }
}
