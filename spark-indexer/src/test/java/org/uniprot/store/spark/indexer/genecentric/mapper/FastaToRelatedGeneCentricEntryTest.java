package org.uniprot.store.spark.indexer.genecentric.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
class FastaToRelatedGeneCentricEntryTest {

    @Test
    void canMapRelatedEntry() {
        String fastaInput =
                ">sp|P34935|BIP_PIG Isoform of P12345, Endoplasmic reticulum chaperone BiP (Fragment) OS=Sus scrofa OX=9823 GN=HSPA5 PE=2 SV=2\n"
                        + "DEIVLVGGSTRIPKIQQLVKEFFNGKEPSRGINPDEAVAYGAAVQAGVLSGDQDTGDLVL\n"
                        + "LDVCPLTLGIETVGGVMTKLIPRNTVVPTKKSQIFSTASDNQPTVTIKVYEGERPLTKDN\n"
                        + "HLLGTFDLTGIPPAPRGVPQIEVTFEIDVNGILRVTAEDKGTGNKNKITITNDQNRLTPE\n"
                        + "EIERMVNDAEKFAEEDKKLKERIDTRNELESYAYCLKNQIGDKEKLGGKLSSEDKETMEK\n"
                        + "AVEEKIEWLESHQDADIEDFKA";

        FastaToRelatedGeneCentricEntry mapper = new FastaToRelatedGeneCentricEntry();
        String proteomeId = "UP000000554";
        Tuple2<LongWritable, Text> tuple = new Tuple2<>(new LongWritable(), new Text(fastaInput));
        Tuple2<String, GeneCentricEntry> result = mapper.parseEntry(proteomeId, tuple);
        assertNotNull(result);
        assertEquals("P12345", result._1);
        GeneCentricEntry entry = result._2;
        assertNotNull(entry);
        assertEquals(proteomeId, entry.getProteomeId());
        assertNotNull(entry.getCanonicalProtein());
        Protein canonical = entry.getCanonicalProtein();
        assertEquals("P12345", canonical.getId());
        assertNull(canonical.getProteinName());

        assertFalse(entry.getRelatedProteins().isEmpty());
        assertEquals(1, entry.getRelatedProteins().size());
        Protein relatedProtein = entry.getRelatedProteins().get(0);
        assertNotNull(relatedProtein);
        assertEquals("P34935", relatedProtein.getId());
        assertEquals(UniProtKBEntryType.SWISSPROT, relatedProtein.getEntryType());
    }

    @Test
    void canMapRelatedEntryWithInvalidFastaInput() {
        String fastaInput =
                ">sp|P34935|BIP_PIG Endoplasmic reticulum chaperone BiP (Fragment) OS=Sus scrofa OX=9823 GN=HSPA5 PE=2 SV=2\n"
                        + "DEIVLVGGSTRIPKIQQLVKEFFNGKEPSRGINPDEAVAYGAAVQAGVLSGDQDTGDLVL\n"
                        + "LDVCPLTLGIETVGGVMTKLIPRNTVVPTKKSQIFSTASDNQPTVTIKVYEGERPLTKDN\n"
                        + "HLLGTFDLTGIPPAPRGVPQIEVTFEIDVNGILRVTAEDKGTGNKNKITITNDQNRLTPE\n"
                        + "EIERMVNDAEKFAEEDKKLKERIDTRNELESYAYCLKNQIGDKEKLGGKLSSEDKETMEK\n"
                        + "AVEEKIEWLESHQDADIEDFKA";

        FastaToRelatedGeneCentricEntry mapper = new FastaToRelatedGeneCentricEntry();
        String proteomeId = "UP000000554";
        Tuple2<LongWritable, Text> tuple = new Tuple2<>(new LongWritable(), new Text(fastaInput));
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> mapper.parseEntry(proteomeId, tuple));
        assertNotNull(exception);
        String errorMessage =
                "Related protein fasta file must have valid "
                        + "prefix \"Isoform of <Accession>,\", "
                        + "proteomeId: UP000000554, id: P34935, "
                        + "protein name: Endoplasmic reticulum chaperone BiP";
        assertEquals(errorMessage, exception.getMessage());
    }

    @Test
    void readThroughEntryReturnNull() {
        String fastaInput =
                ">tr|A0A0G2KK10|A0A0G2KK10_DANRE ATP6V1G2-DDX39B readthrough, V-type proton ATPase subunit G OS=Danio rerio (Zebrafish) (Brachydanio rerio) OX=7955 GN=ENSDARG00000104558 PE=4 SV=1\n"
                        + "DEIVLVGGSTRIPKIQQLVKEFFNGKEPSRGINPDEAVAYGAAVQAGVLSGDQDTGDLVL\n"
                        + "LDVCPLTLGIETVGGVMTKLIPRNTVVPTKKSQIFSTASDNQPTVTIKVYEGERPLTKDN\n"
                        + "HLLGTFDLTGIPPAPRGVPQIEVTFEIDVNGILRVTAEDKGTGNKNKITITNDQNRLTPE\n"
                        + "EIERMVNDAEKFAEEDKKLKERIDTRNELESYAYCLKNQIGDKEKLGGKLSSEDKETMEK\n"
                        + "AVEEKIEWLESHQDADIEDFKA";

        FastaToRelatedGeneCentricEntry mapper = new FastaToRelatedGeneCentricEntry();
        String proteomeId = "UP000000554";
        Tuple2<LongWritable, Text> tuple = new Tuple2<>(new LongWritable(), new Text(fastaInput));
        Tuple2<String, GeneCentricEntry> result = mapper.parseEntry(proteomeId, tuple);
        assertNotNull(result);
        assertEquals("readthrough", result._1);
        assertNull(result._2);
    }

    @Test
    void readErrorInvalidFastaInput() {
        String fastaInput = ">tr|A0A0G2KK10\n" + "AVEEKIEWLESHQDADIEDFKA";

        FastaToRelatedGeneCentricEntry mapper = new FastaToRelatedGeneCentricEntry();
        String proteomeId = "UP000000554";
        Tuple2<LongWritable, Text> tuple = new Tuple2<>(new LongWritable(), new Text(fastaInput));
        assertThrows(IndexHPSDocumentsException.class, () -> mapper.parseEntry(proteomeId, tuple));
    }
}
