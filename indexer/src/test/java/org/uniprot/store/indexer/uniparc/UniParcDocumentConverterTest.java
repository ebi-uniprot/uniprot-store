package org.uniprot.store.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * @@author sahmad
 *
 * @created 10/08/2020
 */
@ExtendWith(SpringExtension.class)
class UniParcDocumentConverterTest {
    @Mock private TaxonomyRepo taxonomyRepo;

    @Test
    void testConvertEntryToDoc() throws Exception {
        URL res =
                getClass()
                        .getClassLoader()
                        .getResource("uniparc/UPI0000127191_UPI0000127192_UPI0000127193.xml");
        File file = Paths.get(res.toURI()).toFile();
        String absolutePath = file.getAbsolutePath();

        Mockito.when(this.taxonomyRepo.retrieveNodeUsingTaxID(Mockito.anyInt()))
                .thenReturn(Optional.empty());

        UniParcDocumentConverter converter =
                new UniParcDocumentConverter(this.taxonomyRepo, new HashMap<>());
        UniParcXmlEntryReader reader = new UniParcXmlEntryReader(absolutePath);
        Entry entry = reader.read();
        assertNotNull(entry);
        UniParcDocument uniParcDocument = converter.convert(entry);
        assertNotNull(uniParcDocument);
        assertEquals("UPI0000127191", uniParcDocument.getUpi());
        assertEquals(16, uniParcDocument.getDbIds().size());
        MatcherAssert.assertThat(
                uniParcDocument.getDbIds(),
                CoreMatchers.hasItems(
                        "Q02297-8",
                        "ENSP00000498811",
                        "AAA27261",
                        "A0A038DI27",
                        "A0A038DND5",
                        "KFT92747",
                        "KFU20332",
                        "WP_001001975",
                        "YP_005181896",
                        "YP_005233094",
                        "ETC11569",
                        "fig|1218145.3.peg.2041",
                        "fig|99287.1.peg.1951",
                        "fig|99287.12.peg.2148",
                        "A0A038DI37",
                        "107675830"));

        assertEquals(9, uniParcDocument.getDatabases().size());
        assertTrue(uniParcDocument.getDatabases().contains("isoforms"));
        assertTrue(uniParcDocument.getDatabases().contains("Ensembl"));
        assertTrue(uniParcDocument.getDatabases().contains("embl-cds"));
        assertTrue(uniParcDocument.getDatabases().contains("UniProt"));
        assertTrue(uniParcDocument.getDatabases().contains("EMBLWGS"));
        assertTrue(uniParcDocument.getDatabases().contains("RefSeq"));
        assertTrue(uniParcDocument.getDatabases().contains("EnsemblBacteria"));
        assertTrue(uniParcDocument.getDatabases().contains("SEED"));
        assertTrue(uniParcDocument.getDatabases().contains("PATRIC"));

        assertEquals(8, uniParcDocument.getActives().size());
        assertTrue(uniParcDocument.getActives().contains("isoforms"));
        assertTrue(uniParcDocument.getActives().contains("Ensembl"));
        assertTrue(uniParcDocument.getActives().contains("embl-cds"));
        assertTrue(uniParcDocument.getActives().contains("UniProt"));
        assertTrue(uniParcDocument.getActives().contains("EMBLWGS"));
        assertTrue(uniParcDocument.getActives().contains("RefSeq"));
        assertTrue(uniParcDocument.getActives().contains("EnsemblBacteria"));
        assertTrue(uniParcDocument.getActives().contains("SEED"));
        assertFalse(uniParcDocument.getActives().contains("PATRIC"));

        // verify that document has only active proteins accession
        assertEquals(2, uniParcDocument.getUniprotAccessions().size());
        MatcherAssert.assertThat(
                uniParcDocument.getUniprotAccessions(),
                CoreMatchers.hasItems("A0A038DI27", "A0A038DND5"));

        assertEquals(1, uniParcDocument.getGeneNames().size());
        assertTrue(uniParcDocument.getGeneNames().contains("NRG1"));

        assertEquals(1, uniParcDocument.getProteinNames().size());
        assertTrue(
                uniParcDocument
                        .getProteinNames()
                        .contains("Isoform 8 of Pro-neuregulin-1, membrane-bound isoform"));

        assertEquals(1, uniParcDocument.getUpids().size());
        assertTrue(uniParcDocument.getUpids().contains("UP000005640"));

        assertEquals("9A48E062FF6285782553081B4EDEF768", uniParcDocument.getSequenceMd5());
        assertEquals("79E8A4FC5967031C", uniParcDocument.getSequenceChecksum());
        assertEquals(263, uniParcDocument.getSeqLength());

        assertNotNull(uniParcDocument.getFeatureIds());
        assertEquals(4, uniParcDocument.getFeatureIds().size());
        assertTrue(uniParcDocument.getFeatureIds().contains("G3DSA:3.10.100.10"));
        assertTrue(uniParcDocument.getFeatureIds().contains("IPR016186"));
        assertTrue(uniParcDocument.getFeatureIds().contains("PF05966"));
        assertTrue(uniParcDocument.getFeatureIds().contains("IPR009238"));
    }
}
