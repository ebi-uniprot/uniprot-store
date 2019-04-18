package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.uniprot.domain.builder.SequenceBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntryType;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtAccessionBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtEntryBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtIdBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.comment.Comment;
import uk.ac.ebi.uniprot.flatfile.parser.SupportingDataMap;
import uk.ac.ebi.uniprot.flatfile.parser.impl.DefaultUniProtParser;
import uk.ac.ebi.uniprot.flatfile.parser.impl.SupportingDataMapImpl;
import uk.ac.ebi.uniprot.flatfile.parser.impl.cc.CcLineTransformer;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTerm;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomicNode;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.ac.ebi.uniprot.indexer.uniprotkb.processor.UniProtEntryConverter.*;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
class UniProtConverterTest {
    private static final String INDEXING_DOC_CONVERSION_FAILED_ENTRIES_LOG = "indexing-doc-conversion-failed-entries.error";
    private static final String CC_ALTERNATIVE_PRODUCTS_FIELD = "cc_alternative_products";
    private static final String CCEV_ALTERNATIVE_PRODUCTS_FIELD = "ccev_alternative_products";
    private static final String CC_COFACTOR_FIELD = "cc_cofactor";
    private static final String CCEV_COFACTOR_FIELD = "ccev_cofactor";
    private static final String CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD = "cc_biophysicochemical_properties";
    private static final String CCEV_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD = "ccev_biophysicochemical_properties";
    private static final String CC_SEQUENCE_CAUTION_FIELD = "cc_sequence_caution";
    private static final String CCEV_SEQUENCE_CAUTION_FIELD = "ccev_sequence_caution";
    private static final String CC_SUBCELLULAR_LOCATION_FIELD = "cc_subcellular_location";
    private static final String CCEV_SUBCELLULAR_LOCATION_FIELD = "ccev_subcellular_location";
    private static final String CC_SIMILARITY_FIELD = "cc_similarity";
    private static final String CCEV_SIMILARITY_FIELD = "ccev_similarity";
    private static final String FT_CONFLICT_FIELD = "ft_conflict";
    private static final String FTEV_CONFLICT_FIELD = "ftev_conflict";
    private static final String FTLEN_CHAIN_FIELD = "ftlen_chain";
    private DateFormat dateFormat;
    private UniProtEntryConverter converter;

    private TaxonomyRepo repoMock;
    private GoRelationRepo goRelationRepoMock;

    @BeforeEach
    void setUp() {
        repoMock = mock(TaxonomyRepo.class);
        goRelationRepoMock = mock(GoRelationRepo.class);
        converter = new UniProtEntryConverter(repoMock, goRelationRepoMock, mock(KeywordRepo.class), mock(PathwayRepo.class));
        dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
    }

    // TODO: 18/04/19 fix this test
    @Test
    void testConvertFullA0PHU1Entry() throws Exception {
        when(repoMock.retrieveNodeUsingTaxID(anyInt()))
                .thenReturn(getTaxonomyNode(172543, "Cichlasoma festae", null, null, null));
        when(goRelationRepoMock.getIsA("GO:0016021")).thenReturn(getMockParentGoTerm());
        when(goRelationRepoMock.getPartOf("GO:0016021")).thenReturn(getMockPartOfGoTerm());
        String file = "A0PHU1.txl";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);
        assertEquals("A0PHU1", doc.accession);
        assertEquals("A0PHU1_9CICH", doc.id);
        assertFalse(doc.reviewed);
        assertEquals(1, doc.proteinNames.size());
        assertEquals("Cytochrome b", doc.proteinNames.get(0));
        assertEquals("Cytochrome b", doc.proteinsNamesSort);
        assertEquals(0, doc.ecNumbers.size());

        assertEquals("09-JAN-2007", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("07-JAN-2015", dateFormat.format(doc.lastModified).toUpperCase());

        assertEquals(16, doc.keywords.size());
        assertEquals("KW-0249", doc.keywords.get(0));
        assertEquals("Electron transport", doc.keywords.get(1));

        assertEquals(1, doc.organismName.size());
        assertEquals("Cichlasoma festae", doc.organismName.get(0));
        assertEquals("Cichlasoma festae", doc.organismSort);
        assertEquals(172543, doc.organismTaxId);
        assertNull(doc.popularOrganism);
        assertEquals("Cichlasoma festae", doc.otherOrganism);

        assertEquals(1, doc.organismTaxon.size());
        assertEquals("Cichlasoma festae", doc.organismTaxon.get(0));

        assertEquals(1, doc.taxLineageIds.size());
        assertEquals(172543L, doc.taxLineageIds.get(0).longValue());

        assertEquals(1, doc.organelles.size());
        assertEquals("Mitochondrion", doc.organelles.get(0));

        assertEquals(1, doc.organismHostIds.size());
        assertEquals(9539, doc.organismHostIds.get(0).intValue());

        assertEquals(1, doc.organismHostNames.size());
        assertEquals("Cichlasoma festae", doc.organismHostNames.get(0));

        assertEquals(55, doc.xrefs.size());
        assertTrue(doc.xrefs.contains("embl-AAY21541.1"));
        assertTrue(doc.xrefs.contains("embl-AAY21541"));
        assertTrue(doc.xrefs.contains("AAY21541.1"));
        assertTrue(doc.xrefs.contains("AAY21541"));

        assertEquals(10, doc.databases.size());
        assertTrue(doc.databases.contains("go"));
        assertTrue(doc.databases.contains("interpro"));


        assertEquals(1, doc.referenceTitles.size());
        assertTrue(doc.referenceTitles.get(0).startsWith("Phylogeny and biogeography of 91 species of heroine"));

        assertEquals(2, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.get(0).startsWith("Concheiro Perez G.A., Bermingham E."));

        assertEquals(1, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("17045493"));

        assertEquals(2, doc.referenceDates.size());
        assertEquals("01-DEC-2004", dateFormat.format(doc.referenceDates.get(0)).toUpperCase());

        assertEquals(1, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Mol. Phylogenet. Evol."));

        assertEquals(3, doc.commentMap.keySet().size());
        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the cytochrome b family. {ECO:0000256|RuleBase:RU000300}."));

        assertEquals(3, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_SIMILARITY_FIELD));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("ECO_0000256"));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("automatic"));

        assertEquals("HOMOLOGY", doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertEquals(3, doc.commentMap.keySet().size());
        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the cytochrome b family. {ECO:0000256|RuleBase:RU000300}."));

        assertEquals(2, doc.cofactorChebi.size());
        assertTrue(doc.cofactorChebi.contains("heme"));

        assertEquals(1, doc.cofactorNote.size());
        assertTrue(doc.cofactorNote.contains("Binds 2 heme groups non-covalently."));

        assertEquals(2, doc.cofactorChebiEv.size());
        assertTrue(doc.cofactorChebiEv.contains("ECO_0000256"));

        assertEquals(2, doc.cofactorNoteEv.size());
        assertTrue(doc.cofactorNoteEv.contains("ECO_0000256"));

        assertEquals(1, doc.familyInfo.size());
        assertTrue(doc.familyInfo.contains("cytochrome b family"));

        assertEquals(42276, doc.seqMass);
        assertEquals(378, doc.seqLength);

        assertEquals(1, doc.scopes.size());
        assertTrue(doc.scopes.contains("NUCLEOTIDE SEQUENCE"));

        assertEquals(22, doc.goes.size());
        assertTrue(doc.goes.contains("Go term 5"));

//        assertEquals(14, doc.defaultGo.size());
//        assertTrue(doc.defaultGo.contains("mitochondrion"));

        assertEquals(2, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(5, doc.score);
        assertNotNull(doc.avro_binary);

        assertFalse(doc.isIsoform);
    }

    @Test
    void testConvertFullQ9EPI6Entry() throws Exception {
        when(repoMock.retrieveNodeUsingTaxID(anyInt()))
                .thenReturn(getTaxonomyNode(10116, "Rattus norvegicus", "Rat", null, null));

        String file = "Q9EPI6.sp";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6", doc.accession);
        assertEquals(5, doc.secacc.size());
        assertEquals("Q7TSC6", doc.secacc.get(1));
        assertEquals("NSMF_RAT", doc.id);
        assertTrue(doc.reviewed);

        assertEquals(5, doc.proteinNames.size());
        assertTrue(doc.proteinNames.contains("NMDA receptor synaptonuclear signaling and neuronal migration factor"));
        assertTrue(doc.proteinNames.contains("Juxtasynaptic attractor of caldendrin on dendritic boutons protein"));
        assertTrue(doc.proteinNames.contains("Jacob protein"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic luteinizing hormone-releasing hormone factor"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic LHRH factor"));
        assertEquals("NMDA receptor synaptonuclear s", doc.proteinsNamesSort);

        assertEquals(0, doc.ecNumbers.size());
        assertEquals(0, doc.ecNumbersExact.size());

        assertEquals("29-OCT-2014", dateFormat.format(doc.lastModified).toUpperCase());
        assertEquals("19-JUL-2005", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("01-MAR-2001", dateFormat.format(doc.sequenceUpdated).toUpperCase());

        assertEquals(30, doc.keywords.size());
        assertEquals("KW-0025", doc.keywords.get(0));
        assertEquals("Alternative splicing", doc.keywords.get(1));

        assertEquals(3, doc.geneNames.size());
        assertEquals("Nsmf", doc.geneNames.get(0));
        assertEquals("Nsmf Jac Nelf", doc.geneNamesSort);
        assertEquals(3, doc.geneNamesExact.size());

        assertEquals(2, doc.organismName.size());
        assertEquals("Rat", doc.organismName.get(1));
        assertEquals("Rattus norvegicus Rat", doc.organismSort);
        assertEquals(10116, doc.organismTaxId);
        assertEquals("Rat", doc.popularOrganism);
        assertNull(doc.otherOrganism);
        assertEquals(2, doc.organismTaxon.size());
        assertEquals(1, doc.taxLineageIds.size());
        assertEquals(10116L, doc.taxLineageIds.get(0).longValue());

        assertEquals(0, doc.organelles.size());
        assertEquals(0, doc.organismHostNames.size());
        assertEquals(0, doc.organismHostIds.size());

        assertEquals(159, doc.xrefs.size());
        assertTrue(doc.xrefs.contains("refseq-NM_001270626.1"));
        assertTrue(doc.xrefs.contains("refseq-NM_001270626"));
        assertTrue(doc.xrefs.contains("NM_001270626.1"));
        assertTrue(doc.xrefs.contains("NM_001270626"));

        assertEquals(23, doc.databases.size());
        assertTrue(doc.databases.contains("refseq"));
        assertTrue(doc.databases.contains("ensembl"));

        assertEquals(7, doc.referenceTitles.size());
        assertTrue(doc.referenceTitles.contains("Characterization of the novel brain-specific protein Jacob."));

        assertEquals(6, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.contains("Kramer P.R., Wray S.;"));

        assertEquals(5, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("15489334"));

        assertEquals(1, doc.referenceOrganizations.size());
        assertTrue(doc.referenceOrganizations.contains("The MGC Project Team;"));

        assertEquals(7, doc.referenceDates.size());
        assertTrue(doc.referenceDates.contains(new Date(965084400000L)));

        assertEquals(5, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Genome Res."));

        assertEquals(9, doc.commentMap.keySet().size());
        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the NSMF family. {ECO:0000305}."));

        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the NSMF family. {ECO:0000305}."));

        assertEquals(9, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_SIMILARITY_FIELD));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("ECO_0000305"));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("manual"));

        assertEquals(8, doc.featuresMap.size());
        assertTrue(doc.featuresMap.containsKey(FT_CONFLICT_FIELD));
        assertTrue(doc.featuresMap.get(FT_CONFLICT_FIELD).
                contains("CONFLICT 174 174 K -> Q (in Ref. 3; AAH87719). {ECO:0000305}."));

        assertEquals(8, doc.featureEvidenceMap.size());
        assertTrue(doc.featureEvidenceMap.containsKey(FTEV_CONFLICT_FIELD));
        assertTrue(doc.featureEvidenceMap.get(FTEV_CONFLICT_FIELD).contains("ECO_0000305"));
        assertTrue(doc.featureEvidenceMap.get(FTEV_CONFLICT_FIELD).contains("manual"));

        assertEquals(8, doc.featureLengthMap.size());
        assertTrue(doc.featureLengthMap.containsKey(FTLEN_CHAIN_FIELD));
        assertTrue(doc.featureLengthMap.get(FTLEN_CHAIN_FIELD).contains(531));

        assertEquals("PROTEIN_LEVEL", doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertEquals(13, doc.subcellLocationTerm.size());
        assertTrue(doc.subcellLocationTerm.contains("Nucleus envelope"));
        assertEquals(0, doc.subcellLocationTermEv.size());
        assertEquals(1, doc.subcellLocationNote.size());
        assertEquals(2, doc.subcellLocationNoteEv.size());
        assertTrue(doc.subcellLocationNoteEv.contains("ECO_0000250"));
        assertTrue(doc.subcellLocationNoteEv.contains("manual"));

        assertEquals(2, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative splicing"));
        assertEquals(2, doc.apAs.size());
        assertTrue(doc.apAs.contains("Additional isoforms seem to exist."));

        assertEquals(5, doc.interactors.size());
        assertTrue(doc.interactors.contains("P27361"));

        assertEquals(1, doc.familyInfo.size());
        assertTrue(doc.familyInfo.contains("NSMF family"));

        assertEquals(60282, doc.seqMass);
        assertEquals(532, doc.seqLength);

        assertEquals(2, doc.rcTissue.size());
        assertTrue(doc.rcTissue.contains("Hippocampus"));

        assertEquals(2, doc.rcStrain.size());
        assertTrue(doc.rcStrain.contains("Wistar"));

        assertEquals(11, doc.scopes.size());
        assertTrue(doc.scopes.contains("SUBCELLULAR LOCATION"));

        assertEquals(50, doc.goes.size());
        assertTrue(doc.goes.contains("0030863"));

//        assertEquals(50, doc.defaultGo.size());
//        assertTrue(doc.defaultGo.contains("membrane"));

        assertEquals(4, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(5, doc.score);
        assertNotNull(doc.avro_binary);

        assertFalse(doc.isIsoform);
    }

    @Test
    void testConvertIsoformEntry() throws Exception {
        when(repoMock.retrieveNodeUsingTaxID(anyInt()))
                .thenReturn(getTaxonomyNode(10116, "Rattus norvegicus", "Rat", null, null));

        String file = "Q9EPI6-2.sp";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6-2", doc.accession);
        assertEquals(1, doc.secacc.size());
        assertEquals("Q9EPI6", doc.secacc.get(0));
        assertEquals("NSMF-2_RAT", doc.id);
        assertTrue(doc.isIsoform);
        assertTrue(doc.reviewed);

        assertEquals(5, doc.proteinNames.size());
        assertTrue(doc.proteinNames
                           .contains("Isoform 2 of NMDA receptor synaptonuclear signaling and neuronal migration factor"));
        assertTrue(doc.proteinNames.contains("Juxtasynaptic attractor of caldendrin on dendritic boutons protein"));
        assertTrue(doc.proteinNames.contains("Jacob protein"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic luteinizing hormone-releasing hormone factor"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic LHRH factor"));
        assertEquals("Isoform 2 of NMDA receptor syn", doc.proteinsNamesSort);

        assertEquals(0, doc.ecNumbers.size());
        assertEquals(0, doc.ecNumbersExact.size());

        assertEquals("20-JUN-2018", dateFormat.format(doc.lastModified).toUpperCase());
        assertEquals("19-JUL-2005", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("01-MAR-2001", dateFormat.format(doc.sequenceUpdated).toUpperCase());

        assertEquals(30, doc.keywords.size());
        assertEquals("KW-0025", doc.keywords.get(0));
        assertEquals("Alternative splicing", doc.keywords.get(1));

        assertEquals(3, doc.geneNames.size());
        assertEquals("Nsmf", doc.geneNames.get(0));
        assertEquals("Nsmf Jac Nelf", doc.geneNamesSort);
        assertEquals(3, doc.geneNamesExact.size());

        assertEquals(2, doc.organismName.size());
        assertEquals("Rat", doc.organismName.get(1));
        assertEquals("Rattus norvegicus Rat", doc.organismSort);
        assertEquals(10116, doc.organismTaxId);
        assertEquals("Rat", doc.popularOrganism);
        assertNull(doc.otherOrganism);
        assertEquals(2, doc.organismTaxon.size());
        assertEquals(1, doc.taxLineageIds.size());
        assertEquals(10116L, doc.taxLineageIds.get(0).longValue());

        assertEquals(0, doc.organelles.size());
        assertEquals(0, doc.organismHostNames.size());
        assertEquals(0, doc.organismHostIds.size());

        assertEquals(56, doc.xrefs.size());
        assertTrue(doc.xrefs.contains("embl-CAC20867.1"));
        assertTrue(doc.xrefs.contains("embl-CAC20867"));
        assertTrue(doc.xrefs.contains("CAC20867.1"));
        assertTrue(doc.xrefs.contains("CAC20867"));

        assertEquals(2, doc.databases.size());
        assertTrue(doc.databases.contains("go"));
        assertTrue(doc.databases.contains("embl"));

        assertEquals(8, doc.referenceTitles.size());
        assertTrue(doc.referenceTitles.contains("Characterization of the novel brain-specific protein Jacob."));

        assertEquals(7, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.contains("Kramer P.R., Wray S.;"));

        assertEquals(6, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("15489334"));

        assertEquals(1, doc.referenceOrganizations.size());
        assertTrue(doc.referenceOrganizations.contains("The MGC Project Team;"));

        assertEquals(8, doc.referenceDates.size());
        assertTrue(doc.referenceDates.contains(new Date(965084400000L)));

        assertEquals(6, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Genome Res."));

        assertEquals(1, doc.commentMap.keySet().size());
        assertEquals(1, doc.commentMap.size());
        assertTrue(doc.commentMap.containsKey(CC_ALTERNATIVE_PRODUCTS_FIELD));

        assertEquals(1, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey("ccev_alternative_products"));

        assertEquals(0, doc.featuresMap.size());
        assertEquals(0, doc.featureEvidenceMap.size());
        assertEquals(0, doc.featureLengthMap.size());

        assertEquals("PROTEIN_LEVEL", doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertEquals(0, doc.subcellLocationTerm.size());
        assertEquals(0, doc.subcellLocationTermEv.size());
        assertEquals(0, doc.subcellLocationNote.size());
        assertEquals(0, doc.subcellLocationNoteEv.size());


        assertEquals(1, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative splicing"));
        assertEquals(1, doc.apAs.size());
        assertTrue(doc.apAs.contains("Alternative splicing"));

        assertEquals(0, doc.interactors.size());

        assertEquals(0, doc.familyInfo.size());

        assertEquals(57478, doc.seqMass);
        assertEquals(509, doc.seqLength);

        assertEquals(2, doc.rcTissue.size());
        assertTrue(doc.rcTissue.contains("Hippocampus"));

        assertEquals(2, doc.rcStrain.size());
        assertTrue(doc.rcStrain.contains("Wistar"));

        assertEquals(13, doc.scopes.size());
        assertTrue(doc.scopes.contains("SUBCELLULAR LOCATION"));

        assertEquals(50, doc.goes.size());
        assertTrue(doc.goes.contains("0030863"));

//        assertEquals(50, doc.defaultGo.size());
//        assertTrue(doc.defaultGo.contains("membrane"));

        assertEquals(4, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(5, doc.score);
        assertNotNull(doc.avro_binary);

    }

    @Test
    void testConvertIsoformCanonical() throws Exception {
        when(repoMock.retrieveNodeUsingTaxID(anyInt())).thenReturn(Optional.<TaxonomicNode>empty());

        String file = "Q9EPI6-1.sp";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6-1", doc.accession);
        assertNull(doc.isIsoform);

        assertTrue(converter.isCanonicalIsoform(entry));

        assertNull(doc.id);
        assertNull(doc.firstCreated);
        assertNull(doc.lastModified);
    }

    @Test
    void givenTooLongFieldValue_whenTruncatedSortFieldValueGenerated_thenEnsureItHasCorrectSize() {
        char[] sortValueArr = new char[SORT_FIELD_MAX_LENGTH + 10];
        Arrays.fill(sortValueArr, 'X');
        String sortValue = new String(sortValueArr);

        String truncatedSortValue = truncatedSortValue(sortValue);
        assertEquals(SORT_FIELD_MAX_LENGTH, truncatedSortValue.length());
    }

    @Test
    void givenSmallFieldValue_whenTruncatedSortFieldValueGenerated_thenEnsureItHasCorrectSize() {
        String sortValue = "hello world";

        String truncatedSortValue = truncatedSortValue(sortValue);

        assertEquals(sortValue.length(), truncatedSortValue.length());
    }

    @Test
    void givenSmallAvroDefaultField_whenTruncated_thenDefaultIsSet() {
        String fakeBase64String = "hello world";

        String defaultBinaryValue = getDefaultBinaryValue(fakeBase64String);

        assertEquals(fakeBase64String, defaultBinaryValue);
    }

    @Test
    void givenLargeAvroDefaultField_whenTruncated_thenDefaultIsNotSet() {
        char[] fakeSortValueArr = new char[MAX_STORED_FIELD_LENGTH + 10];
        Arrays.fill(fakeSortValueArr, 'X');
        String fakeBase64String = new String(fakeSortValueArr);

        String defaultBinaryValue = getDefaultBinaryValue(fakeBase64String);

        assertNull(defaultBinaryValue);
    }

    @Test
    void testAlternativeProductsCommentConvertProperlyToDocument() {
        String alternativeLine = "CC   -!- ALTERNATIVE PRODUCTS:\n" +
                "CC       Event=Alternative promoter usage, Alternative initiation; Named isoforms=3;\n" +
                "CC       Name=Genome polyprotein;\n" +
                "CC         IsoId=Q672I1-1; Sequence=Displayed;\n" +
                "CC         Note=Produced from the genomic RNA.;\n" +
                "CC       Name=Subgenomic capsid protein; Synonyms=VP1;\n" +
                "CC         IsoId=Q672I1-2; Sequence=VSP_034391;\n" +
                "CC         Note=Produced from the subgenomic RNA by alternative promoter\n" +
                "CC         usage.;\n" +
                "CC       Name=Uncharacterized protein VP3;\n" +
                "CC         IsoId=Q672I0-1; Sequence=External;\n" +
                "CC         Note=Produced by alternative initiation from the subgenomic\n" +
                "CC         RNA.;";

        String alternativeProductsLine = "ALTERNATIVE PRODUCTS:\n" +
                "Event=Alternative promoter usage, Alternative initiation; Named isoforms=3;\n" +
                "Name=Genome polyprotein;\n" +
                "IsoId=Q672I1-1; Sequence=Displayed;\n" +
                "Note=Produced from the genomic RNA.;\n" +
                "Name=Subgenomic capsid protein; Synonyms=VP1;\n" +
                "IsoId=Q672I1-2; Sequence=VSP_034391;\n" +
                "Note=Produced from the subgenomic RNA by alternative promoter usage.;\n" +
                "Name=Uncharacterized protein VP3;\n" +
                "IsoId=Q672I0-1; Sequence=External;\n" +
                "Note=Produced by alternative initiation from the subgenomic RNA.;";
        UniProtEntry entry = createUniProtEntryFromCommentLine(alternativeLine);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertTrue(doc.commentMap.containsKey(CC_ALTERNATIVE_PRODUCTS_FIELD));
        assertTrue(doc.commentMap.get(CC_ALTERNATIVE_PRODUCTS_FIELD).
                contains(alternativeProductsLine));

        assertEquals(1, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_ALTERNATIVE_PRODUCTS_FIELD));
        assertEquals(0, doc.commentEvMap.get(CCEV_ALTERNATIVE_PRODUCTS_FIELD).size());

        assertEquals(2, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative promoter usage"));
        assertTrue(doc.ap.contains("Alternative initiation"));
        assertEquals(0, doc.apEv.size());
        assertEquals(2, doc.apApu.size());
        assertTrue(doc.apApu.contains("Alternative promoter usage"));
        assertTrue(doc.apApu.contains("Alternative initiation"));
        assertEquals(0, doc.apApuEv.size());
    }

    @Test
    void testCofactorCommentConvertProperlyToDocument() {
        String cofactorLine = "CC   -!- COFACTOR: RNA-directed RNA polymerase:\n" +
                "CC       Name=Mg(2+); Xref=ChEBI:CHEBI:18420;\n" +
                "CC         Evidence={ECO:0000250|UniProtKB:P03313};\n" +
                "CC       Note=Requires the presence of 3CDpro or 3CPro.\n" +
                "CC       {ECO:0000250|UniProtKB:P03313};";
        String cofactorLineValue = "COFACTOR: RNA-directed RNA polymerase:\n" +
                "Name=Mg(2+); Xref=ChEBI:CHEBI:18420; Evidence={ECO:0000250|UniProtKB:P03313};\n" +
                "Note=Requires the presence of 3CDpro or 3CPro. {ECO:0000250|UniProtKB:P03313};";

        UniProtEntry entry = createUniProtEntryFromCommentLine(cofactorLine);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals(1, doc.commentMap.keySet().size());

        assertTrue(doc.commentMap.containsKey(CC_COFACTOR_FIELD));
        assertTrue(doc.commentMap.get(CC_COFACTOR_FIELD).contains(cofactorLineValue));
        assertEquals(1, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_COFACTOR_FIELD));
        assertEquals(0, doc.commentEvMap.get(CCEV_COFACTOR_FIELD).size());

        assertEquals(2, doc.cofactorChebi.size());
        assertTrue(doc.cofactorChebi.contains("Mg(2+)"));
        assertEquals(2, doc.cofactorChebiEv.size());
        assertTrue(doc.cofactorChebiEv.contains("manual"));

        assertEquals(1, doc.cofactorNote.size());
        assertTrue(doc.cofactorNote.contains("Requires the presence of 3CDpro or 3CPro."));
        assertEquals(2, doc.cofactorNoteEv.size());
        assertTrue(doc.cofactorNoteEv.contains("ECO_0000250"));
    }

    @Test
    void testBPCPCommentConvertProperlyToDocument() {
        String bpcpLine = "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n" +
                "CC       pH dependence:\n" +
                "CC         Optimum pH is 5.0 for protease activity.\n" +
                "CC         {ECO:0000269|PubMed:16603535};\n" +
                "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n" +
                "CC       Absorption:\n" +
                "CC         Abs(max)=550 nm {ECO:0000269|PubMed:10510276};\n" +
                "CC       Kinetic parameters:\n" +
                "CC         KM=9 uM for AMP (at pH 5.5 and 25 degrees Celsius)\n" +
                "CC         {ECO:0000269|PubMed:10510276};\n" +
                "CC         KM=9 uM for pyrophosphate (at pH 5.5 and 25 degrees Celsius)\n" +
                "CC         {ECO:0000269|PubMed:10510276};\n" +
                "CC         KM=30 uM for beta-glycerophosphate (at pH 5.5 and 25 degrees\n" +
                "CC         Celsius) {ECO:0000269|PubMed:10510276};\n" +
                "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n" +
                "CC       Kinetic parameters:\n" +
                "CC         KM=27 mM for L-proline (at 25 degrees Celsius)\n" +
                "CC         {ECO:0000269|PubMed:17344208};\n" +
                "CC         KM=4 mM for 3,4-dehydro-L-proline (at 25 degrees Celsius)\n" +
                "CC         {ECO:0000269|PubMed:17344208};\n" +
                "CC         Vmax=20.5 umol/min/mg enzyme for L-proline (at 25 degrees\n" +
                "CC         Celsius) {ECO:0000269|PubMed:17344208};\n" +
                "CC         Vmax=119 umol/min/mg enzyme for 3,4-dehydro-L-proline (at 25\n" +
                "CC         degrees Celsius) {ECO:0000269|PubMed:17344208};\n" +
                "CC         Note=kcat is 13 s(-1) for L-proline. kcat is 75 s(-1) for 3,4-\n" +
                "CC         dehydro-L-proline. {ECO:0000269|PubMed:17344208};\n" +
                "CC       Redox potential:\n" +
                "CC         E(0) is -75 mV. {ECO:0000269|PubMed:17344208};\n" +
                "CC       Temperature dependence:\n" +
                "CC         Highly thermostable. Exhibits over 85% or 60% of activity after\n" +
                "CC         a 1 hour or 3 hours incubation at 90 degrees Celsius,\n" +
                "CC         respectively. The half-life is estimated to be 257 minutes.\n" +
                "CC         {ECO:0000269|PubMed:17344208};";

        String phdependenceLineValue = "BIOPHYSICOCHEMICAL PROPERTIES:\n" +
                "pH dependence:\n" +
                "Optimum pH is 5.0 for protease activity. {ECO:0000269|PubMed:16603535};";

        UniProtEntry entry = createUniProtEntryFromCommentLine(bpcpLine);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);
        assertEquals(3, doc.commentMap.get(CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD).size());

        assertTrue(doc.commentMap.containsKey(CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD));
        assertTrue(doc.commentMap.get(CC_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD).contains(phdependenceLineValue));

        assertEquals(1, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD));
        assertEquals(0, doc.commentEvMap.get(CCEV_BIOPHYSICOCHEMICAL_PROPERTIES_FIELD).size());

        assertEquals(12, doc.bpcp.size());
        assertTrue(doc.bpcp.contains("550"));
        assertEquals(3, doc.bpcpEv.size());
        assertTrue(doc.bpcpEv.contains("ECO_0000269"));

        assertEquals(1, doc.bpcpAbsorption.size());
        assertEquals(3, doc.bpcpAbsorptionEv.size());
        assertTrue(doc.bpcpAbsorptionEv.contains("experimental"));

        assertEquals(8, doc.bpcpKinetics.size());
        assertEquals(3, doc.bpcpKineticsEv.size());
        assertTrue(doc.bpcpKineticsEv.contains("manual"));

        assertEquals(1, doc.bpcpPhDependence.size());
        assertEquals(3, doc.bpcpPhDependenceEv.size());
        assertTrue(doc.bpcpPhDependenceEv.contains("ECO_0000269"));

        assertEquals(1, doc.bpcpRedoxPotential.size());
        assertEquals(3, doc.bpcpRedoxPotentialEv.size());
        assertTrue(doc.bpcpRedoxPotentialEv.contains("experimental"));

        assertEquals(1, doc.bpcpTempDependence.size());
        assertEquals(3, doc.bpcpTempDependenceEv.size());
        assertTrue(doc.bpcpTempDependenceEv.contains("manual"));

    }

    @Test
    void testSequenceCautionCommentConvertProperlyToDocument() throws Exception {
        String sequenceCautionLine = "CC   -!- SEQUENCE CAUTION:\n" +
                "CC       Sequence=CAB59730.1; Type=Frameshift; Positions=76, 138; Evidence={ECO:0000305};\n" +
                "CC   -!- SEQUENCE CAUTION:\n" +
                "CC       Sequence=AAA42785.1; Type=Erroneous gene model prediction; Evidence={ECO:0000305};\n" +
                "CC   -!- SEQUENCE CAUTION:\n" +
                "CC       Sequence=AAA03332.1; Type=Erroneous initiation; Evidence={ECO:0000305};\n" +
                "CC   -!- SEQUENCE CAUTION:\n" +
                "CC       Sequence=AAB25832.2; Type=Erroneous translation; Note=Wrong choice of frame.; Evidence={ECO:0000305};\n" +
                "CC   -!- SEQUENCE CAUTION:\n" +
                "CC       Sequence=BAB43866.1; Type=Miscellaneous discrepancy; Note=Chimeric cDNA. It is a chimera between Dox-A3 and PPO2.; Evidence={ECO:0000305};\n" +
                "CC   -!- SEQUENCE CAUTION:\n" +
                "CC       Sequence=CAH10679.1; Type=Erroneous termination; Positions=431; Note=Translated as Trp.; Evidence={ECO:0000305};";

        String sequenceCautionLineValue = "SEQUENCE CAUTION:\n" +
                "Sequence=CAB59730.1; Type=Frameshift; Positions=76, 138; Evidence={ECO:0000305};";
        UniProtEntry entry = createUniProtEntryFromCommentLine(sequenceCautionLine);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);
        assertEquals(6, doc.commentMap.get(CC_SEQUENCE_CAUTION_FIELD).size());

        assertTrue(doc.commentMap.containsKey(CC_SEQUENCE_CAUTION_FIELD));
        assertTrue(doc.commentMap.get(CC_SEQUENCE_CAUTION_FIELD).contains(sequenceCautionLineValue));
        assertEquals(1, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_SEQUENCE_CAUTION_FIELD));
        assertEquals(0, doc.commentEvMap.get(CCEV_SEQUENCE_CAUTION_FIELD).size());


        assertEquals(6, doc.seqCaution.size());
        assertTrue(doc.seqCaution.contains("Translated as Trp."));

        assertEquals(2, doc.seqCautionEv.size());
        assertTrue(doc.seqCautionEv.contains("ECO_0000305"));

        assertEquals(1, doc.seqCautionErInit.size());
        assertTrue(doc.seqCautionErInit.contains("Erroneous initiation"));

        assertEquals(1, doc.seqCautionErPred.size());
        assertTrue(doc.seqCautionErPred.contains("Erroneous gene model prediction"));

        assertEquals(1, doc.seqCautionErTerm.size());
        assertTrue(doc.seqCautionErTerm.contains("Translated as Trp."));

        assertEquals(1, doc.seqCautionErTran.size());
        assertTrue(doc.seqCautionErTran.contains("Wrong choice of frame."));

        assertEquals(1, doc.seqCautionFrameshift.size());
        assertTrue(doc.seqCautionFrameshift.contains("Frameshift"));

        assertEquals(1, doc.seqCautionMisc.size());
        assertTrue(doc.seqCautionMisc.contains("Chimeric cDNA. It is a chimera between Dox-A3 and PPO2."));

        assertEquals(2, doc.seqCautionMiscEv.size());
        assertTrue(doc.seqCautionMiscEv.contains("manual"));
    }

    @Test
    void testSubcellularLocationCommentConvertProperlyToDocument() throws Exception {
        String subcellularLocationLine = "CC   -!- SUBCELLULAR LOCATION: Capsid protein: Virion. Host cytoplasm.\n" +
                "CC   -!- SUBCELLULAR LOCATION: Small envelope protein M: Virion membrane\n" +
                "CC       {ECO:0000250|UniProtKB:P03314}; Multi-pass membrane protein\n" +
                "CC       {ECO:0000250|UniProtKB:P03314}. Host endoplasmic reticulum\n" +
                "CC       membrane {ECO:0000250|UniProtKB:P03314}; Multi-pass membrane\n" +
                "CC       protein {ECO:0000255}. Note=ER membrane retention is mediated by\n" +
                "CC       the transmembrane domains. {ECO:0000250|UniProtKB:P03314}.";

        String subcellularLocationLineValue = "SUBCELLULAR LOCATION: Capsid protein: Virion. Host cytoplasm.";
        UniProtEntry entry = createUniProtEntryFromCommentLine(subcellularLocationLine);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);
        assertEquals(2, doc.commentMap.get(CC_SUBCELLULAR_LOCATION_FIELD).size());

        assertTrue(doc.commentMap.containsKey(CC_SUBCELLULAR_LOCATION_FIELD));
        assertTrue(doc.commentMap.get(CC_SUBCELLULAR_LOCATION_FIELD).contains(subcellularLocationLineValue));
        assertEquals(1, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_SUBCELLULAR_LOCATION_FIELD));
        assertEquals(0, doc.commentEvMap.get(CCEV_SUBCELLULAR_LOCATION_FIELD).size());


        assertEquals(5, doc.subcellLocationTerm.size());
        assertTrue(doc.subcellLocationTerm.contains("Host cytoplasm"));

        assertEquals(3, doc.subcellLocationTermEv.size());
        assertTrue(doc.subcellLocationTermEv.contains("ECO_0000255"));

        assertEquals(1, doc.subcellLocationNote.size());
        assertTrue(doc.subcellLocationNote.contains("ER membrane retention is mediated by the transmembrane domains"));

        assertEquals(2, doc.subcellLocationNoteEv.size());
        assertTrue(doc.subcellLocationNoteEv.contains("ECO_0000250"));
    }

//    @Test
//    void onConversionErrorWriteFailedEntryToFile() throws Exception {
//        // GIVEN --------------------------------
//        String logFileNameForErrors = INDEXING_DOC_CONVERSION_FAILED_ENTRIES_LOG;
//        Path logFileForErrors = Paths.get(logFileNameForErrors);
//        // truncate any previous log file used to store document conversion errors ...
//        // so that we can check for new content later
//        if (Files.exists(logFileForErrors)) {
//            PrintWriter fileWriter = new PrintWriter(logFileNameForErrors);
//            fileWriter.print("");
//            fileWriter.close();
//        }
//
//        String accession = "Q9EPI6";
//        String file = accession + ".sp";
//        UniProtEntry entry = parse(file);
//        assertNotNull(entry);
//
//        UniProtEntry entry = mock(UniProtEntry.class);
//        doThrow(NullPointerException.class).when(entry).setDocument(any());
//        when(entry.getEntry()).thenReturn(entry);
//
//        // WHEN --------------------------------
//        // ensure an exception is thrown when being processed
//        converter.convert(entry);
//
//        // wait for the file to be written
//        Thread.sleep(500);
//
//        // THEN --------------------------------
//        // ensure this entry is written to the error log
//        assertTrue(Files.exists(logFileForErrors));
//
//        // sanity check: ensure the error log contains the correct accession
//        Stream<String> lines = Files.lines(logFileForErrors);
//        List<String> acLinesForAccession = lines.filter(l -> l.startsWith("AC   " + accession))
//                .collect(Collectors.toList());
//        assertThat(acLinesForAccession, hasSize(1));
//    }

    private UniProtEntry parse(String file) throws Exception {
        InputStream is = UniProtEntryProcessor.class.getClassLoader().getResourceAsStream("uniprotkb/" + file);
        assertNotNull(is);
        SupportingDataMap supportingDataMap = new SupportingDataMapImpl("uniprotkb/keywlist.txt",
                                                                        "uniprotkb/humdisease.txt",
                                                                        "uniprotkb/PMID.GO.dr_ext.txt",
                                                                        "uniprotkb/subcell.txt");
        DefaultUniProtParser parser = new DefaultUniProtParser(supportingDataMap, false);
        return parser.parse(IOUtils.toString(is, Charset.defaultCharset()));
    }

    private UniProtDocument convertEntry(UniProtEntry entry) {
        return converter.convert(entry);
    }

    private UniProtEntry createUniProtEntryFromCommentLine(String commentLine) {
        List<Comment> comments = new CcLineTransformer("", "").transformNoHeader(commentLine);
        return new UniProtEntryBuilder()
                .primaryAccession(new UniProtAccessionBuilder("P12345").build())
                .uniProtId(new UniProtIdBuilder("P12345_ID").build())
                .active()
                .entryType(UniProtEntryType.TREMBL)
                .comments(comments)
                .sequence(new SequenceBuilder("AAAA").build())
                .build();
    }


    private List<GoTerm> getMockParentGoTerm() {
        return asList(
                new GoTermFileReader.GoTermImpl("GO:123", "Go term 3"),
                new GoTermFileReader.GoTermImpl("GO:124", "Go term 4")
        );

    }

    private List<GoTerm> getMockPartOfGoTerm() {
        return asList(
                new GoTermFileReader.GoTermImpl("GO:125", "Go term 5"),
                new GoTermFileReader.GoTermImpl("GO:126", "Go term 6")
        );
    }

    private Optional<TaxonomicNode> getTaxonomyNode(int id, String scientificName, String commonName, String synonym, String mnemonic) {
        return Optional.of(new TaxonomicNode() {
            @Override
            public int id() {
                return id;
            }

            @Override
            public String scientificName() {
                return scientificName;
            }

            @Override
            public String commonName() {
                return commonName;
            }

            @Override
            public String synonymName() {
                return synonym;
            }

            @Override
            public String mnemonic() {
                return mnemonic;
            }

            @Override
            public TaxonomicNode parent() {
                return null;
            }

            @Override
            public boolean hasParent() {
                return false;
            }
        });
    }
}