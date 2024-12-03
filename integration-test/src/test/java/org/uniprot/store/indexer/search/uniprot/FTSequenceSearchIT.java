package org.uniprot.store.indexer.search.uniprot;

import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;

class FTSequenceSearchIT {

    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q197B1 = "Q197B1";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q12345 = "Q12345";
    private static final String Q6GZN7 = "Q6GZN7";
    private static final String Q6V4H0 = "Q6V4H0";
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   VAR_SEQ         234..249\n"
                        + "FT                   /note=\"Missing (in isoform 3)\"\n"
                        + "FT                   /evidence=\"ECO:0000305\"\n"
                        + "FT                   /id=\"VSP_055167\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   VARIANT         177\n"
                        + "FT                   /note=\"R -> Q (in a colorectal cancer sample; somatic mutation; dbSNP:rs374122292)\"\n"
                        + "FT                   /evidence=\"ECO:0000269|PubMed:16959974\"\n"
                        + "FT                   /id=\"VAR_035897\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   NON_STD         92\n"
                        + "FT                   /note=\"Selenocysteine\"\n"
                        + "FT                   /evidence=\"ECO:0000250\"\n"
                        + "FT   NON_TER         1\n"
                        + "FT                   /evidence=\"ECO:0000303|PubMed:17963685\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   NON_CONS        15..16\n"
                        + "FT                   /evidence=\"ECO:0000305\"\n"
                        + "FT                   /id=\"PRO_0000027671\"\n"
                        + "FT   CONFLICT        758\n"
                        + "FT                   /note=\"P -> A (in Ref. 1; CAA57732)\"\n"
                        + "FT                   /evidence=\"ECO:0000305\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   UNSURE          44\n"
                        + "FT                   /evidence=\"ECO:0000269|Ref.1\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }
}
