package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.core.uniprotkb.comment.CommentType;

class CCTextOnlySearchIT {
    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q6GZX3 = "Q6GZX3";
    private static final String Q6GZY3 = "Q6GZY3";
    private static final String Q197B6 = "Q197B6";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q196W5 = "Q196W5";
    private static final String Q6GZN7 = "Q6GZN7";
    private static final String Q6V4H0 = "Q6V4H0";
    private static final String P48347 = "P48347";
    private static final String Q12345 = "Q12345";

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
                LineType.CC, "CC   -!- FUNCTION: Transcription activation. {ECO:0000305}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- FUNCTION: Transmembrane serine/threonine kinase forming with the\n"
                        + "CC       TGF-beta type I serine/threonine kinase receptor, TGFBR1, the non-\n"
                        + "CC       cytokine dimer results in the phosphorylation and the activation\n"
                        + "CC       of TGFRB1 by the constitutively active TGFBR2. Activated TGFBR1\n"
                        + "CC       phosphorylates SMAD2 which dissociates from the receptor and\n"
                        + "CC       interacts with SMAD4. The SMAD2-SMAD4 complex is subsequently\n"
                        + "CC       translocated to the nucleus where it modulates the transcription\n"
                        + "CC       of the TGF-beta-regulated genes. This constitutes the canonical\n"
                        + "CC       SMAD-dependent TGF-beta signaling cascade. Also involved in non-\n"
                        + "CC       canonical, SMAD-independent TGF-beta signaling pathways.\n"
                        + "CC       {ECO:0000256|PIRNR:PIRNR037393}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZY3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- ACTIVITY REGULATION: Activated by binding of S100B which releases\n"
                        + "CC       autoinhibitory N-lobe interactions, enabling ATP to bind and the\n"
                        + "CC       autophosphorylation of Ser-281. Thr-444 then undergoes calcium-\n"
                        + "CC       dependent phosphorylation by STK24/MST3. Interactions between\n"
                        + "CC       phosphorylated Thr-444 and the N-lobe promote additional\n"
                        + "CC       structural changes that complete the activation of the kinase.\n"
                        + "CC       Autoinhibition is also released by the binding of MOB1/MOBKL1A and\n"
                        + "CC       MOB2/HCCA2 to the N-terminal of STK38.\n"
                        + "CC       {ECO:0000269|PubMed:12493777, ECO:0000269|PubMed:14661952,\n"
                        + "CC       ECO:0000269|PubMed:15067004, ECO:0000269|PubMed:15197186}.\n"
                        + "CC   -!- SUBUNIT: Homodimeric S100B binds two molecules of STK38\n"
                        + "CC       (PubMed:14661952). Interacts with MOB1 and MOB2 (PubMed:15067004,\n"
                        + "CC       PubMed:15197186). Interacts with MAP3K1 and MAP3K2 (via the kinase\n"
                        + "CC       catalytic domain) (PubMed:17906693). Forms a tripartite complex\n"
                        + "CC       with MOBKL1B and STK3/MST2 (PubMed:18362890). Interacts with\n"
                        + "CC       MICAL1; leading to inhibit the protein kinase activity by\n"
                        + "CC       antagonizing activation by MST1/STK4 (By similarity).\n"
                        + "CC       {ECO:0000250|UniProtKB:Q91VJ4, ECO:0000269|PubMed:14661952,\n"
                        + "CC       ECO:0000269|PubMed:15067004, ECO:0000269|PubMed:15197186,\n"
                        + "CC       ECO:0000269|PubMed:17906693, ECO:0000269|PubMed:18362890}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SIMILARITY: Belongs to the protein kinase superfamily. Ser/Thr\n"
                        + "CC       protein kinase family. {ECO:0000255|PROSITE-ProRule:PRU00159}.\n"
                        + "CC   -!- SIMILARITY: Contains 1 protein kinase domain.\n"
                        + "CC       {ECO:0000255|PROSITE-ProRule:PRU00159}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q196W5));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- DOMAIN: The conserved cysteine present in the cysteine-switch\n"
                        + "CC       motif binds the catalytic zinc ion, thus inhibiting the enzyme.\n"
                        + "CC       The dissociation of the cysteine from the zinc ion upon the\n"
                        + "CC       activation-peptide release activates the enzyme.\n"
                        + "CC   -!- SIMILARITY: Belongs to the peptidase M10A family. {ECO:0000305}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- CATALYTIC ACTIVITY:\n"
                        + "CC       Reaction=O2 + 2 R'C(R)SH = H2O2 + R'C(R)S-S(R)CR';\n"
                        + "CC         Xref=Rhea:RHEA:17357, ChEBI:CHEBI:15379, ChEBI:CHEBI:16240,\n"
                        + "CC         ChEBI:CHEBI:16520, ChEBI:CHEBI:17412; EC=1.8.3.2;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- TISSUE SPECIFICITY: Highly expressed in the large and small\n"
                        + "CC       intestine, stomach and testis. High levels also present in the\n"
                        + "CC       brain, in particular the neurocortex, basal forebrain,\n"
                        + "CC       hippocampus, the amygdala, cerebellum and brainstem.\n"
                        + "CC       {ECO:0000269|PubMed:15037617, ECO:0000269|PubMed:15308672}.\n"
                        + "CC   -!- SIMILARITY: Belongs to the protein kinase superfamily. AGC Ser/Thr\n"
                        + "CC       protein kinase family. {ECO:0000305}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, P48347));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- PTM: Phosphorylated on tyrosine. Tyr-250 may be important for\n"
                        + "CC       interaction with kinases. Phosphorylated by PTK6 at Tyr-250\n"
                        + "CC       modulates PTK6-mediated STAT3 activation. Tyr-22 and Tyr-322\n"
                        + "CC       appears to be phosphorylated by SRC. {ECO:0000269|PubMed:12540842,\n"
                        + "CC       ECO:0000269|PubMed:19393627}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- TISSUE SPECIFICITY: Expressed in roots, stems leaves and flowers,\n"
                        + "CC       but not in seeds (PubMed:17092320). In short days, observed in\n"
                        + "CC       cotyledons and roots but absent from rosette leaves\n"
                        + "CC       (PubMed:21736589). {ECO:0000269|PubMed:17092320,\n"
                        + "CC       ECO:0000269|PubMed:21736589}.\n"
                        + "CC   -!- DEVELOPMENTAL STAGE: Accumulates and reach a peak shortly before\n"
                        + "CC       full senescence, at the interface between the green and yellow\n"
                        + "CC       regions of senescent leaves, and then declines (PubMed:9617813,\n"
                        + "CC       PubMed:21736589). In flowers, restricted to the pollen and very\n"
                        + "CC       weak expression in petal veins. In dark-treated seedlings,\n"
                        + "CC       strongly expressed throughout the root tissues, including root\n"
                        + "CC       hairs, except in primary and lateral root tips (PubMed:21736589).\n"
                        + "CC       {ECO:0000269|PubMed:21736589, ECO:0000269|PubMed:9617813}.\n"
                        + "CC   -!- INDUCTION: In short-day conditions, follows a diurnal pattern of\n"
                        + "CC       regulation, with transcription repression in the light and\n"
                        + "CC       activation in the dark (PubMed:17092320, PubMed:9617813,\n"
                        + "CC       PubMed:21736589). Induced by oxidants (e.g. hydrogen peroxide\n"
                        + "CC       H(2)O(2), menadione and paraquat) (PubMed:17092320,\n"
                        + "CC       PubMed:21736589). Accumulates in response to abscisic acid (ABA)\n"
                        + "CC       and dehydration (PubMed:17092320, PubMed:9617813,\n"
                        + "CC       PubMed:21736589). Induced by ethylene, more strongly in the\n"
                        + "CC       younger leaves than in the older ones (PubMed:9617813). Up-\n"
                        + "CC       regulated 12 h postinfestation (hpi) in green peach aphid (GPA;\n"
                        + "CC       Myzus persicae Sulzer) infested leaves (PubMed:16299172).\n"
                        + "CC       Triggered by cold, wounding and salt (PubMed:18808718,\n"
                        + "CC       PubMed:21736589). Strongly induced by the necrotrophic fungal\n"
                        + "CC       pathogen Botrytis cinerea (PubMed:21736589).\n"
                        + "CC       {ECO:0000269|PubMed:16299172, ECO:0000269|PubMed:17092320,\n"
                        + "CC       ECO:0000269|PubMed:18808718, ECO:0000269|PubMed:21736589,\n"
                        + "CC       ECO:0000269|PubMed:9617813}.\n"
                        + "CC   -!- DISRUPTION PHENOTYPE: Early flowering and senescence, as well as\n"
                        + "CC       reduced shoot biomass. Short primary root with reduced lateral\n"
                        + "CC       root formation and short root hairs. Enhanced sensitivity to the\n"
                        + "CC       fungal nectroph, Botrytis cinerea and to the virulent bacterial\n"
                        + "CC       pathogen Pseudomonas syringae pv. tomato, but normal resistance to\n"
                        + "CC       an avirulent P.syringae strain. {ECO:0000269|PubMed:21736589}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void shouldFindTwoFunctionEntry() {
        String query = comments(CommentType.FUNCTION, "*");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX3, Q6GZX4));
    }

    @Test
    void shouldFindTwoFunctionEntryQuery() {
        String query = comments(CommentType.FUNCTION, "activation");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX3, Q6GZX4));
    }

    @Test
    void shouldFindNoneFunctionEntryQueryWithEvidence() {
        String query = comments(CommentType.FUNCTION, "inactivation");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);

        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindTwoSimilarityEntryQuery() {
        String query = comments(CommentType.SIMILARITY, "family");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B6, Q196W5));
    }

    @Test
    void shouldFindOneEnzymeRegEntryQuery() {
        String query = comments(CommentType.ACTIVITY_REGULATION, "phosphorylation");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZY3));
    }

    @Test
    void shouldFindNoneEnzymeRegEntryQueryEvidence() {
        String query = comments(CommentType.ACTIVITY_REGULATION, "notphosphorylation");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindOneDomainEntryQuery() {
        String query = comments(CommentType.DOMAIN, "cysteine");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q196W5));
    }

    @Test
    void shouldFindOneSubunitEntryQuery() {
        String query = comments(CommentType.SUBUNIT, "molecules");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZY3));
    }

    @Test
    void shouldFindNoneSubunitEntryQueryEvidence() {
        String query = comments(CommentType.SUBUNIT, "nomolecules");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindOneTissueSpeEntryQuery() {
        String query = comments(CommentType.TISSUE_SPECIFICITY, "expressed");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345, Q6V4H0));
    }

    @Test
    void shouldFindNoneTissueSpeEntryQueryEvidence() {
        String query = comments(CommentType.TISSUE_SPECIFICITY, "noexpressed");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindOneDevStageEntryQuery() {
        String query = comments(CommentType.DEVELOPMENTAL_STAGE, "senescence");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q12345));
    }

    @Test
    void shouldFindNoneDevStageEntryQueryEvidence() {
        String query = comments(CommentType.DEVELOPMENTAL_STAGE, "nosenescence");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindOneInductionEntryQuery() {
        String query = comments(CommentType.INDUCTION, "abscisic");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q12345));
    }

    @Test
    void shouldFindNoneInductionEntryQueryEvidence() {
        String query = comments(CommentType.INDUCTION, "noabscisic");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindOneDisruptionEntryQuery() {
        String query = comments(CommentType.DISRUPTION_PHENOTYPE, "biomass");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q12345));
    }

    @Test
    void shouldFindNoneDisruptionEntryQueryEvidence() {
        String query = comments(CommentType.DISRUPTION_PHENOTYPE, "nobiomass");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindOnePtmEntryQuery() {
        String query = comments(CommentType.PTM, "tyrosine");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(P48347));
    }

    @Test
    void shouldFindNonePtmEntryQueryEvidence() {
        String query = comments(CommentType.PTM, "notyrosine");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void shouldFindOneCataEntryQuery() {
        String query = comments(CommentType.CATALYTIC_ACTIVITY, "O2");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
    }

    @Test
    void shouldFindOneCataEntryFromPartialChecmicalFormula() {
        String query = comments(CommentType.CATALYTIC_ACTIVITY, "H2O2 + R'C(R)S-S(R)CR'");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
    }

    @Test
    void shouldFindOneCataEntryFromSmallPartOfChecmicalFormula() {
        String query = comments(CommentType.CATALYTIC_ACTIVITY, "(R)S");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
    }

    @Test
    void shouldFindNonCataEntryQueryEvidence() {
        String query = comments(CommentType.CATALYTIC_ACTIVITY, "nonoO2");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }
}
