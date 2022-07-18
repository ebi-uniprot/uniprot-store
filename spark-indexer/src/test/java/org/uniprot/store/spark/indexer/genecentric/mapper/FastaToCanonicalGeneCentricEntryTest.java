package org.uniprot.store.spark.indexer.genecentric.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.store.spark.indexer.common.exception.IndexHDFSDocumentsException;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
class FastaToCanonicalGeneCentricEntryTest {

    @Test
    void canMapCanonicalEntry() {
        String fastaInput =
                ">sp|P34935|BIP_PIG Endoplasmic reticulum chaperone BiP (Fragment) OS=Sus scrofa OX=9823 GN=HSPA5 PE=2 SV=2\n"
                        + "DEIVLVGGSTRIPKIQQLVKEFFNGKEPSRGINPDEAVAYGAAVQAGVLSGDQDTGDLVL\n"
                        + "LDVCPLTLGIETVGGVMTKLIPRNTVVPTKKSQIFSTASDNQPTVTIKVYEGERPLTKDN\n"
                        + "HLLGTFDLTGIPPAPRGVPQIEVTFEIDVNGILRVTAEDKGTGNKNKITITNDQNRLTPE\n"
                        + "EIERMVNDAEKFAEEDKKLKERIDTRNELESYAYCLKNQIGDKEKLGGKLSSEDKETMEK\n"
                        + "AVEEKIEWLESHQDADIEDFKA";

        FastaToCanonicalGeneCentricEntry mapper = new FastaToCanonicalGeneCentricEntry();
        String proteomeId = "UP000000554";
        Tuple2<LongWritable, Text> tuple = new Tuple2<>(new LongWritable(), new Text(fastaInput));
        Tuple2<String, GeneCentricEntry> result = mapper.parseEntry(proteomeId, tuple);
        assertNotNull(result);
        assertEquals("P34935", result._1);
        GeneCentricEntry entry = result._2;
        assertNotNull(entry);
        assertEquals(proteomeId, entry.getProteomeId());
        Protein canonical = entry.getCanonicalProtein();
        assertNotNull(canonical);
        assertEquals("P34935", canonical.getId());
        assertTrue(entry.getRelatedProteins().isEmpty());
    }

    @Test
    void readErrorInvalidFastaInput() {
        String fastaInput = ">tr|A0A0G2KK10|A0A0G2KK10_DANRE\n" + "AVEEKIEWLESHQDADIEDFKA";

        FastaToCanonicalGeneCentricEntry mapper = new FastaToCanonicalGeneCentricEntry();
        String proteomeId = "UP000000554";
        Tuple2<LongWritable, Text> tuple = new Tuple2<>(new LongWritable(), new Text(fastaInput));
        assertThrows(IndexHDFSDocumentsException.class, () -> mapper.parseEntry(proteomeId, tuple));
    }
}
