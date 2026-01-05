package org.uniprot.store.indexer.converters;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * @@author sahmad
 *
 * @created 10/08/2020
 */
@ExtendWith(MockitoExtension.class)
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
                        "AAA27261.1",
                        "A0A038DI27.1",
                        "A0A038DND5.1",
                        "KFT92747.1",
                        "KFU20332.1",
                        "WP_001001975.1",
                        "YP_005181896.1",
                        "YP_005233094.1",
                        "ETC11569",
                        "fig|1218145.3.peg.2041",
                        "fig|99287.1.peg.1951",
                        "fig|99287.12.peg.2148",
                        "A0A038DI37.1",
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

        assertEquals(9, uniParcDocument.getDatabasesFacets().size());
        assertTrue(
                uniParcDocument
                        .getDatabasesFacets()
                        .contains(UniParcDatabase.SWISSPROT_VARSPLIC.getIndex()));
        assertTrue(
                uniParcDocument
                        .getDatabasesFacets()
                        .contains(UniParcDatabase.ENSEMBL_VERTEBRATE.getIndex()));
        assertTrue(uniParcDocument.getDatabasesFacets().contains(UniParcDatabase.EMBL.getIndex()));
        assertTrue(
                uniParcDocument
                        .getDatabasesFacets()
                        .contains(UniParcDatabase.SWISSPROT.getIndex()));
        assertTrue(
                uniParcDocument.getDatabasesFacets().contains(UniParcDatabase.EMBLWGS.getIndex()));
        assertTrue(
                uniParcDocument.getDatabasesFacets().contains(UniParcDatabase.REFSEQ.getIndex()));
        assertTrue(
                uniParcDocument
                        .getDatabasesFacets()
                        .contains(UniParcDatabase.EG_BACTERIA.getIndex()));
        assertTrue(uniParcDocument.getDatabasesFacets().contains(UniParcDatabase.SEED.getIndex()));
        assertTrue(
                uniParcDocument.getDatabasesFacets().contains(UniParcDatabase.PATRIC.getIndex()));

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

        assertEquals(1, uniParcDocument.getProteomes().size());
        assertTrue(uniParcDocument.getProteomes().contains("UP000005640"));

        assertEquals(1, uniParcDocument.getProteomeComponents().size());
        assertTrue(uniParcDocument.getProteomeComponents().contains("UP000005640:Chromosome 8"));

        assertEquals(
                Set.of("79E8A4FC5967031C", "9A48E062FF6285782553081B4EDEF768"),
                uniParcDocument.getSequenceChecksums());
        assertEquals(263, uniParcDocument.getSeqLength());

        assertNotNull(uniParcDocument.getFeatureIds());
        assertEquals(4, uniParcDocument.getFeatureIds().size());
        assertTrue(uniParcDocument.getFeatureIds().contains("G3DSA:3.10.100.10"));
        assertTrue(uniParcDocument.getFeatureIds().contains("IPR016186"));
        assertTrue(uniParcDocument.getFeatureIds().contains("PF05966"));
        assertTrue(uniParcDocument.getFeatureIds().contains("IPR009238"));
    }
}
