package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;

/** Tests if the protein existence search is working correctly */
@Slf4j
class ReferenceSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q6GZX1 = "Q6GZX1";
    private static final String Q6GZX2 = "Q6GZX2";
    private static final String Q6GZX3 = "Q6GZX3";
    private static final String Q6GZX4 = "Q6GZX4";
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        String entryAsString = TestUtils.convertInputStreamToString(resourceAsStream);

        {
            ReferencedUniProtEntryObjectProxy entryProxy =
                    ReferencedUniProtEntryObjectProxy.createEntryFromString(entryAsString);

            // ref ex ///1
            entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX1));
            entryProxy.addReference(
                    "RN   [1]\n"
                            + "RP   NUCLEOTIDE SEQUENCE [LARGE SCALE GENOMIC DNA].\n"
                            + "RC   PLASMID=plas {ECO:0000303|PubMed:22805947};\n"
                            + "RX   PubMed=15165820; DOI=10.1016/j.virol.2004.02.019;\n"
                            + "RA   Tan W.G., Barkman T.J., Gregory Chinchar V., Essani K.;\n"
                            + "RT   \"Comparative genomic analyses of frog virus 3, type species of the\n"
                            + "RT   genus Ranavirus (family Iridoviridae).\";\n"
                            + "RL   Virology 323:70-84(2004).");

            entryProxy.addReference(
                    "RN   [2] {ECO:0000305}\n"
                            + "RP   PROTEIN SEQUENCE OF 1-15 AND 26-36, GLYCOSYLATION, AND ALLERGEN.\n"
                            + "RC   TISSUE=Fruit;\n"
                            + "RA   Bublin M., Radauer C., Lebens A., Knulst A., Scheiner O.,\n"
                            + "RA   Breiteneder H.;\n"
                            + "RT   \"Purification and partial characterisation of a 40 kDa allergen from\n"
                            + "RT   kiwifruit (Actinidia deliciosa) (Presentation at the XXV Congress of\n"
                            + "RT   the European Academy of Allergology and Clinical Immunology, 10-14\n"
                            + "RT   June 2006, Vienna, Austria).\";\n"
                            + "RL   Submitted (NOV-2006) to UniProtKB.");
            searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        }

        // ref ex 2
        {
            ReferencedUniProtEntryObjectProxy entryProxy =
                    ReferencedUniProtEntryObjectProxy.createEntryFromString(entryAsString);

            entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX2));
            entryProxy.addReference(
                    "RN   [1]\n"
                            + "RP   NUCLEOTIDE SEQUENCE [LARGE SCALE GENOMIC DNA].\n"
                            + "RC   STRAIN=LL171 {ECO:0000303|PubMed:22805947};\n"
                            + "RX   PubMed=11448171; DOI=10.1006/viro.2001.0963;\n"
                            + "RA   Jakob N.J., Mueller K., Bahr U., Darai G.;\n"
                            + "RT   \"Analysis of the first complete DNA sequence of an invertebrate\n"
                            + "RT   iridovirus: coding strategy of the genome of Chilo iridescent virus.\";\n"
                            + "RL   Virology 286:182-196(2001).");

            entryProxy.addReference(
                    "RN   [2]\n"
                            + "RP   NUCLEOTIDE SEQUENCE [LARGE SCALE GENOMIC DNA].\n"
                            + "RC   TRANSPOSON=Tn1 {ECO:0000303|PubMed:22805947};\n"
                            + "RX   PubMed=15165820; DOI=10.1016/j.virol.2004.02.019;\n"
                            + "RA   Tan W.G., Barkman T.J., Gregory Chinchar V., Essani K.;\n"
                            + "RT   \"Comparative genomic analyses of frog virus 3, type species of the\n"
                            + "RT   genus Ranavirus (family Iridoviridae).\";\n"
                            + "RL   Virology 323:70-84(2004).");
            entryProxy.addReference(
                    // same as last reference, but with changed authors order + pubmed info
                    "RN   [3]\n"
                            + "RP   FUNCTION, CATALYTIC ACTIVITY, AND SUBCELLULAR LOCATION.\n"
                            + "RX   PubMed=19935662; DOI=10.1038/nchembio.266;\n"
                            + "RA   Romeis T., Werner A.K., Witte C.P.;\n"
                            + "RT   \"Ureide catabolism in Arabidopsis thaliana and Escherichia coli.\";\n"
                            + "RL   Nat. Chem. Biol. 6:19-21(2010).");
            searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        }

        // ref ex 3
        {
            ReferencedUniProtEntryObjectProxy entryProxy =
                    ReferencedUniProtEntryObjectProxy.createEntryFromString(entryAsString);

            entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
            entryProxy.addReference(
                    "RN   [1]\n"
                            + "RP   FUNCTION, CATALYTIC ACTIVITY, AND SUBCELLULAR LOCATION.\n"
                            + "RX   PubMed=19935661; DOI=10.1038/nchembio.265;\n"
                            + "RA   Werner A.K., Romeis T., Witte C.P.;\n"
                            + "RT   \"Ureide catabolism in Arabidopsis thaliana and Escherichia coli.\";\n"
                            + "RL   Nat. Chem. Biol. 6:19-21(2010).");
            entryProxy.addReference(
                    "RN   [2]\n"
                            + "RP   X-RAY CRYSTALLOGRAPHY (2.6 ANGSTROMS) OF 1-261, AND SUBUNIT.\n"
                            + "RG   New York structural genomics research consortium (NYSGRC);\n"
                            + "RT   \"Crystal structure of ylba, hypothetical protein from E.coli.\";\n"
                            + "RL   Submitted (JAN-2005) to the PDB data bank.");
            searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        }

        // ref ex 4
        {
            ReferencedUniProtEntryObjectProxy entryProxy =
                    ReferencedUniProtEntryObjectProxy.createEntryFromString(entryAsString);

            entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
            entryProxy.addReference(
                    "RN   [1]\n"
                            + "RP   FUNCTION, CATALYTIC ACTIVITY, AND SUBCELLULAR LOCATION.\n"
                            + "RX   PubMed=234876; DOI=10.1038/nchembio.262;\n"
                            + "RA   Lionö A.K., Cheetarah T., Tigre C.P.;\n"
                            + "RT   \"Ümlaut titles äre GREAT αβηω!\";\n"
                            + "RL   Nat. Chem. Biol. 6:19-21(2010).");

            entryProxy.addReference(
                    "RN   [2]\n"
                            + "RP   X-RAY CRYSTALLOGRAPHY (2.6 ANGSTROMS) OF 1-261, AND SUBUNIT.\n"
                            + "RG   New York structural genomics research consortium (NYSGRC);\n"
                            + "RT   \"Crystal structure of ylba, hypothetical protein from E.coli.\";\n"
                            + "RL   Submitted (JAN-2005) to the PDB data bank.");
            searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        }

        searchEngine.printIndexContents();
    }

    @Test
    void refTitleFindSpecialCharacterFullTitle() {
        String query = title("Ümlaut titles äre GREAT αβηω!");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX4));
    }

    @Test
    void refTitleFindReplaceAUmlautWithAPartialTitle() {
        String query = title("are great");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX4));
    }

    @Test
    void refTitleFindUmlautPartialTitle() {
        String query = title("Ümlaut");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX4));
    }

    @Test
    void refTitleFindNoUmlautPartialTitle() {
        String query = title("Umlaut");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX4));
    }

    @Test
    void refTitleFindAlphaPartialTitle() {
        String query = title("αβηω");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX4));
    }

    @Test
    void refTitleFindNothingWithWrongSpecialCharactersInTitle() {
        String query = title("αβηωThisbitIsWrong");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void refFindFullTitle() {
        String query = title("Crystal structure of ylba, hypothetical protein from E.coli");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX3, Q6GZX4));
    }

    @Test
    void refFindLongFullTitle() {
        String query =
                title(
                        "Comparative genomic analyses of frog virus 3, type species of the genus Ranavirus (family Iridoviridae).");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX1, Q6GZX2));
    }

    @Test
    void refAuthorsSetFind1Entries() {
        Set<String> authorSet =
                new HashSet<String>() {
                    {
                        add("Knulst A.");
                        add("Radauer C.");
                    }
                };
        String query = authors(authorSet);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX1));
    }

    @Test
    void refAuthorsSurnameSetFind1Entries() {
        Set<String> authorSet =
                new HashSet<String>() {
                    {
                        add("Knulst");
                        add("Radauer");
                    }
                };
        String query = authors(authorSet);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX1));
    }

    @Test
    void refAuthorsSetFind3Entries() {
        Set<String> authorSet =
                new HashSet<String>() {
                    {
                        add("Breiteneder H.");
                        add("Gregory Chinchar V.");
                    }
                };
        String query = authors(authorSet);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX1));
    }

    @Test
    void refAuthorsSetFind0Entries() {
        Set<String> authorSet =
                new HashSet<String>() {
                    {
                        add("ThisIsNotEvenAName H.");
                        add("NorIsThisANameWowCowabunga C.P.");
                    }
                };
        String query = authors(authorSet);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void refAuthoringGroupAbbrev() {
        String query = authorGroup("NYSGRC");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX3, Q6GZX4));
    }

    @Test
    void refAuthoringGroupNoAbbrev() {
        String query = authorGroup("New York structural genomics research consortium");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX3, Q6GZX4));
    }

    @Test
    void refAuthoringGroupFull() {
        String query = authorGroup("New York structural genomics research consortium (NYSGRC);");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX3, Q6GZX4));
    }

    @Test
    void refPubMedTwoEntries() {
        String query = pubmed("15165820");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(Q6GZX1, Q6GZX2));
    }

    @Test
    void refPubMedNoEntries() {
        String query = pubmed("1516580");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void refStrainsOneEntry() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("strain"),
                        "LL171");
        log.debug(query.toString());
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItem(Q6GZX2));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX1)));
    }

    @Test
    void refTissueOneEntry() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("tissue"),
                        "Fruit");
        log.debug(query.toString());
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItem(Q6GZX1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX2)));
    }

    @Test
    void refPlasmidOneEntry() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("plasmid"),
                        "plas");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItem(Q6GZX1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX2)));
    }

    @Test
    void refTransposonOneEntry() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("transposon"),
                        "tn1");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItem(Q6GZX2));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX1)));
    }

    @Test
    void refRPOneEntry() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("scope"),
                        "GLYCOSYLATION");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        log.debug(retrievedAccessions.toString());
        assertThat(retrievedAccessions, hasItem(Q6GZX1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX2)));
    }

    @Test
    void refRPThreeEntries() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("scope"),
                        "SUBCELLULAR");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        log.debug(retrievedAccessions.toString());
        assertThat(retrievedAccessions, hasItems(Q6GZX2, Q6GZX3, Q6GZX4));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX1)));
    }

    @Test
    void refPublished() {
        LocalDate start = LocalDate.of(2010, 1, 1);
        LocalDate end = LocalDate.of(2010, 12, 31);

        String query =
                QueryBuilder.rangeQuery(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("lit_pubdate")
                                .getFieldName(),
                        start,
                        end);

        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX2, Q6GZX3, Q6GZX4));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX1)));
    }
    /**
     * Class that allows to add Reference entry objects directly into the UniProtEntryObjectProxy
     */
    private static class ReferencedUniProtEntryObjectProxy extends UniProtEntryObjectProxy {
        protected ReferencedUniProtEntryObjectProxy() {
            super();
        }

        public static ReferencedUniProtEntryObjectProxy createEntryFromString(String entryText) {
            ReferencedUniProtEntryObjectProxy uniProtEntryObject =
                    new ReferencedUniProtEntryObjectProxy();
            uniProtEntryObject.entryObject = uniProtEntryObject.entryParser.parse(entryText);

            return uniProtEntryObject;
        }

        void addReference(String referenceBlock) {
            entryObject.ref.add(buildReferenceEntryObject(referenceBlock));
        }

        private EntryObject.ReferenceObject buildReferenceEntryObject(String rawReference) {
            // ugly method, yes, but how else can we easily build reference objects?

            String[] lines = rawReference.split("\n");
            DefaultUniprotKBLineParserFactory parserFactory =
                    new DefaultUniprotKBLineParserFactory();

            Map<LineType, StringBuilder> refMap = new HashMap<>();
            for (String line : lines) {
                LineType lineType =
                        LineType.valueOf(
                                line.substring(0, 2)); // get the line type from the text, e.g., RX

                if (!refMap.containsKey(lineType)) {
                    refMap.put(lineType, new StringBuilder());
                }
                refMap.get(lineType).append(line).append("\n");
            }

            EntryObject.ReferenceObject ro = new EntryObject.ReferenceObject();
            for (LineType lineType : refMap.keySet()) {
                switch (lineType) {
                    case RN:
                        ro.rn =
                                parserFactory
                                        .createRnLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    case RP:
                        ro.rp =
                                parserFactory
                                        .createRpLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    case RC:
                        ro.rc =
                                parserFactory
                                        .createRcLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    case RX:
                        ro.rx =
                                parserFactory
                                        .createRxLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    case RG:
                        ro.rg =
                                parserFactory
                                        .createRgLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    case RA:
                        ro.ra =
                                parserFactory
                                        .createRaLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    case RT:
                        ro.rt =
                                parserFactory
                                        .createRtLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    case RL:
                        ro.rl =
                                parserFactory
                                        .createRlLineParser()
                                        .parse(refMap.get(lineType).toString());
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Couldn't parse the reference: " + rawReference);
                }
            }

            return ro;
        }
    }

    private String title(String value) {
        return query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("lit_title"), value);
    }

    private String author(String value) {
        return query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("lit_author"), value);
    }

    private String authorGroup(String value) {
        return query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("lit_organisation"),
                value);
    }

    BinaryOperator<String> andQuery = (q1, q2) -> QueryBuilder.and(q1, q2);

    private String authors(Collection<String> values) {
        Optional<String> query =
                values.stream().map(this::author).collect(Collectors.reducing(andQuery));
        return query.get();
    }

    private String pubmed(String value) {
        return query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("lit_pubmed"), value);
    }
}
