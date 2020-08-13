package org.uniprot.store.indexer.uniparc;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
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

        UniParcDocumentConverter converter = new UniParcDocumentConverter(this.taxonomyRepo);
        UniParcXmlEntryReader reader = new UniParcXmlEntryReader(absolutePath);
        Entry entry = reader.read();
        Assertions.assertNotNull(entry);
        UniParcDocument uniParcDocument = converter.convert(entry);
        Assertions.assertNotNull(uniParcDocument);
        Assertions.assertEquals("UPI0000127191", uniParcDocument.getUpi());
        Assertions.assertEquals(13, uniParcDocument.getDbIds().size());
        MatcherAssert.assertThat(
                uniParcDocument.getDbIds(),
                CoreMatchers.hasItems(
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
                        "A0A038DI37"));
        // verify that document has only active proteins accession
        Assertions.assertEquals(2, uniParcDocument.getUniprotAccessions().size());
        MatcherAssert.assertThat(
                uniParcDocument.getUniprotAccessions(),
                CoreMatchers.hasItems(
                        "A0A038DI27",
                        "A0A038DND5"));
    }
}
