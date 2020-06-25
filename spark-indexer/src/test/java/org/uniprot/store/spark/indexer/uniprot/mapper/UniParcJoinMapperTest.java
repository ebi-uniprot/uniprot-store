package org.uniprot.store.spark.indexer.uniprot.mapper;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 25/06/2020
 */
class UniParcJoinMapperTest {

    @Test
    void testValidEntry() throws Exception {
        UniParcJoinMapper mapper = new UniParcJoinMapper();
        UniParcCrossReference valid1 = new UniParcCrossReferenceBuilder()
                .active(true)
                .id("P10000")
                .database(UniParcDatabase.SWISSPROT)
                .build();
        UniParcCrossReference valid2 = new UniParcCrossReferenceBuilder()
                .active(true)
                .id("P20000")
                .database(UniParcDatabase.SWISSPROT_VARSPLIC)
                .build();
        UniParcCrossReference valid3 = new UniParcCrossReferenceBuilder()
                .active(true)
                .id("P30000")
                .database(UniParcDatabase.TREMBL)
                .build();
        UniParcEntry entry = new UniParcEntryBuilder()
                .uniParcId("UP1234567890")
                .uniParcCrossReferencesAdd(valid1)
                .uniParcCrossReferencesAdd(valid2)
                .uniParcCrossReferencesAdd(valid3)
                .build();


        Iterator<Tuple2<String, String>> result = mapper.call(entry);
        assertNotNull(result);
        List<Tuple2<String, String>> resultList= new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertEquals(3, resultList.size());
        Tuple2<String, String> map1 = resultList.get(0);
        assertEquals("P10000", map1._1);
        assertEquals("UP1234567890", map1._2);

        Tuple2<String, String> map2 = resultList.get(1);
        assertEquals("P20000", map2._1);
        assertEquals("UP1234567890", map2._2);

        Tuple2<String, String> map3 = resultList.get(2);
        assertEquals("P30000", map3._1);
        assertEquals("UP1234567890", map3._2);
    }

    @Test
    void testEmptyMap() throws Exception {
        UniParcJoinMapper mapper = new UniParcJoinMapper();
        UniParcCrossReference inactive = new UniParcCrossReferenceBuilder()
                .active(false)
                .id("P10000")
                .database(UniParcDatabase.SWISSPROT)
                .build();
        UniParcCrossReference refSeq = new UniParcCrossReferenceBuilder()
                .active(true)
                .id("refSeqId")
                .database(UniParcDatabase.REFSEQ)
                .build();
        UniParcCrossReference embl = new UniParcCrossReferenceBuilder()
                .active(true)
                .id("P30000")
                .database(UniParcDatabase.EMBL)
                .build();
        UniParcEntry entry = new UniParcEntryBuilder()
                .uniParcId("UP1234567890")
                .uniParcCrossReferencesAdd(inactive)
                .uniParcCrossReferencesAdd(refSeq)
                .uniParcCrossReferencesAdd(embl)
                .build();


        Iterator<Tuple2<String, String>> result = mapper.call(entry);
        assertNotNull(result);
        assertFalse(result.hasNext());
    }
}