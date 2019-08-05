package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

/**
 * Tests if the protein names of an entry are indexed correctly
 */
public class DESearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";

    private static final String ACCESSION1 = "Q197F4";
    private static final String DE_LINE1 =
            "DE   RecName: Full=Aspartate aminotransferase, mitochondrial;\n" +
                    "DE            Short=mAspAT;\n" +
                    "DE   AltName: Full=Fatty acid-binding protein;\n" +
                    "DE            Short=FABP;\n" +
                    "DE   AltName: Full=Glutamate oxaloacetate transaminase 2;\n" +
                    "DE   AltName: Full=Kynurenine aminotransferase IV;\n" +
                    "DE   AltName: Allergen=Act d 3;\n" +
                    "DE   AltName: Biotech=bio technical context;\n" +
                    "DE   AltName: CD_antigen=CD1b;\n" +
                    "DE   AltName: INN=substrate;\n" +
                    "DE   SubName: Full=AT10348p;\n" +
                    "DE   Includes:\n" +
                    "DE     RecName: Full=Aspartate carbamoyltransferase;\n" +
                    "DE              Short=AspCar;\n" +
                    "DE     AltName: Full=Dihydroorotase;\n" +
                    "DE   Contains:\n" +
                    "DE     RecName: Full=Megakaryocyte-potentiating factor;\n" +
                    "DE              Short=MPF;\n" +
                    "DE   Contains:\n" +
                    "DE     RecName: Full=Adipolin gCTRP12;\n" +
                    "DE     AltName: Full=Adipolin cleaved form;\n";
    private static final String ACCESSION2 = "Q197F5";
    private static final String DE_LINE2 = "DE   RecName: Full=KCIP-1;\n";
    private static final String ACCESSION3 = "Q197F6";
    private static final String DE_LINE3 = "DE   RecName: Full=14-3-3;\n";
    private static final String ACCESSION4 = "Q197F7";
    private static final String DE_LINE4 = "DE   RecName: " +
            "Full=HLA class I histocompatibility antigen, A-1 alpha chain (MHC class I antigen A*1);\n";
    private static final String ACCESSION5 = "Q197F8";
    private static final String DE_LINE5 = "DE   RecName: Full=PP2A B subunit isoform B'-delta;\n";
    private static final String ACCESSION6 = "Q197F9";
    private static final String DE_LINE6 = "DE   RecName: Full=Q06JG5 CLAVATA3/ESR (CLE)-related protein 16D10;\n";
    private static final String ACCESSION7 = "Q197G0";
    private static final String DE_LINE7 =
            "DE   RecName: Full=Carene synthase, chloroplastic;\n" +
                    "DE            Short=(+)-car-3-ene synthase;\n";
    private static final String ACCESSION8 = "Q197G1";
    private static final String DE_LINE8 = "DE   RecName: Full=Cardiotoxin 7'';\n";
    private static final String ACCESSION9 = "Q197G2";
    private static final String DE_LINE9 = "DE   RecName: Full=Collectin-11;\n" +
            "DE   AltName: Full=Collectin kidney protein 1;\n" +
            "DE            Short=CL-K1;";
    @ClassRule
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();
    private static int accessionId = 0;

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE1);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE2);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE3);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 4
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION4));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE4);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 5
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION5));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE5);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 6
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION6));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE6);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 7
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION7));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE7);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 8
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION8));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE8);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 9
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION9));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE9);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    public void nonExistentProteinNameMatchesNoDocuments() {
        String query = proteinName("Unknown");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void fullRecommendedNameInMainInEntry1HitsEntry1() {
        String query = proteinName("Aspartate aminotransferase, mitochondrial");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void partialFullRecommendedNameInMainInEntry1HitsEntry1() {
        String query = proteinName("Aspartate aminotransferase");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void shortRecommendedNameInMainInEntry1HitsEntry1() {
        String query = proteinName("mAspAT");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void fullAlternativeNameInMainInEntry1HitsEntry1() {
        String query = proteinName("Glutamate oxaloacetate transaminase 2");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void partialFullAlternativeNameInMainInEntry1HitsEntry1() {
        String query = proteinName("Glutamate oxaloacetate");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void shortAlternativeNameInMainInEntry1HitsEntry1() {
        String query = proteinName("FABP");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void cdAntigenInMainInEntry1HitsEntry1() {
        String query = proteinName("CD1b");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void allergenInMainInEntry1HitsEntry1() {
        String query = proteinName("Act d 3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void INNInMainInEntry1HitsEntry1() {
        String query = proteinName("substrate");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void biotechInMainInEntry1HitsEntry1() {
        String query = proteinName("bio technical context");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void fullRecommendedNameInContainsInEntry1HitsEntry1() {
        String query = proteinName("Megakaryocyte-potentiating factor");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void shortRecommendedNameInContainsInEntry1HitsEntry1() {
        String query = proteinName("MPF");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void fullAlternativeNameInContainsInEntry1HitsEntry1() {
        String query = proteinName("Adipolin cleaved form");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void fullRecommendedNameInIncludesInEntry1HitsEntry1() {
        String query = proteinName("Aspartate carbamoyltransferase");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void shortRecommendedNameInIncludesInEntry1HitsEntry1() {
        String query = proteinName("AspCar");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void fullAlternativeNameInIncludesInEntry1HitsEntry1() {
        String query = proteinName("Dihydroorotase");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void searchForHyphenatedNameUsingExactMatch() {
        String query = proteinName("KCIP-1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    public void noMatchForHyphenatedNameUsingExtraNonExistentCharacter() {
        String query = proteinName("KCIPE-1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void searchForHyphenatedNameUsingTermsWithSpaces() {
        String query = proteinName("KCIP 1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    public void searchForHyphenatedNameUsingSingleTerm() {
        String query = proteinName("KCIP");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    public void noMatchForHyphenatedNameUsingSubstringOfLetterPart() {
        String query = proteinName("KCI");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void searchForHyphenatedAndNumberedNameUsingExactMatch() {
        String query = proteinName("14-3-3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void searchForHyphenatedAndNumberedNameUsingPartialNameWithHyphen() {
        String query = proteinName("14-3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void searchForHyphenatedAndNumberedNameUsingPartialNameWithoutHyphen() {
        String query = proteinName("14 3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void noMatchForHyphenatedAndNumberedNameUsingPartialNameAndNonMatchingNumber() {
        String query = proteinName("14 1");
        System.out.println(query);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void searchForNameWithParenthesisAndAsteriskUsingExactMatch() {
        String query =
                proteinName("HLA class I histocompatibility antigen, A-1 alpha chain (MHC class I antigen A\\*1)");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    public void searchForNameWithParenthesisAndAsteriskUsingSubPartWithAsterisk() {
        String query = proteinName("A*1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    public void searchForNameWithParenthesisAndAsteriskUsingSubPartWithParenthesis() {
        String query = proteinName("(MHC class I antigen A\\*1)");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    public void searchForNameWithSingleQuoteUsingExactMatch() {
        String query = proteinName("PP2A B subunit isoform B'-delta");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION5));
    }

    @Test
    public void searchForNameWithSingleQuoteUsingPartialWithoutQuote() {
        String query = proteinName("B-delta");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION5));
    }

    @Test
    public void searchForNameWithForwardSlashUsingExactMatch() {
        String query = proteinName("Q06JG5 CLAVATA3/ESR (CLE)-related protein 16D10");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION6));
    }

    @Test
    public void searchForNameWithForwardSlashUsingPartialWordLeftOfForwardSlash() {
        String query = proteinName("CLAVATA3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION6));
    }

    @Test
    public void searchForNameWithChemicalSymbolUsingExactMatch() {
        String query = proteinName("(+)-car-3-ene synthase");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION7));
    }

    @Test
    public void searchForNameWithChemicalSymbolUsingSubstringOfChemicalSymbol() {
        String query = proteinName("3-ene synthase");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION7));
    }

    @Test
    public void searchForNameWithTwoSingleQuoatesUsingExactMatch() {
        String query = proteinName("Cardiotoxin 7''");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION8));
    }

    @Test
    public void searchForNameWithTwoSingleQuotesUsingFirstWord() {
        String query = proteinName("Cardiotoxin");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION8));
    }

    @Test
    public void queryForClsProteinNameDoesNotHitEntryThatHasOnlyCLInTheName() {
        String query = proteinName("cls");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, not(contains(ACCESSION9)));
    }

    @Test
    public void canFindRecNameWhenQueryOmitsSeparators() {
        String accession = newAccession();

        index(accession, "7-cyano-7-deazaguanine tRNA-ribosyltransferase");
        String query = proteinName("cyano 7 deazaguanine");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    public void canFindRecNameWithNestedBraces() {
        String accession = newAccession();

        String deRecName = "Methylenetetrahydrofolate-tRNA(uracil(54)-C(5))-methyltransferase(FADH(2)-oxidizig)";
        index(accession, deRecName);
        String query = proteinName(deRecName);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    public void canFindRecNameContainingComma() {
        String accession = newAccession();

        String deRecName = "Endo-1,5-beta-xylanase";
        index(accession, deRecName);
        String query = proteinName(deRecName);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    public void canFindRecNameContainingSlashes() {
        String accession = newAccession();

        String deRecName = "Cys-tRNA(Pro)/cys-tRNA(Cys) deacylae";
        index(accession, deRecName);
        String query = proteinName(deRecName);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    public void canFindRecNameContainingSquareBrackets() {
        String accession = newAccession();

        String deRecName = "Ornithine-acyl[acyl carrier protein] N-acyltransferase";
        index(accession, deRecName);
        String query = proteinName(deRecName);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    private void index(String accession, String recName) {
    	try {
        String deLine = "DE   RecName: Full=" + recName + ";";

        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, accession));
        entryProxy.updateEntryObject(LineType.DE, deLine);

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
    	}catch(Exception e) {
    		
    	}
    }

    private String newAccession() {
        return "P" + String.format("%05d", accessionId++);
    }
    
    private String proteinName(String value) {
    	return query(UniProtField.Search.name, value);
    }
}