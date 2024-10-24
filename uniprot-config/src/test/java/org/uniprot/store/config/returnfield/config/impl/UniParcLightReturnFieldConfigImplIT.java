package org.uniprot.store.config.returnfield.config.impl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.parser.tsv.uniparc.UniParcEntryLightValueMapper;
import org.uniprot.core.parser.tsv.uniparc.UniParcEntryValueMapper;
import org.uniprot.core.uniparc.*;
import org.uniprot.core.uniparc.impl.*;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class UniParcLightReturnFieldConfigImplIT {

    private static UniParcEntryLight entry;
    private static ReturnFieldConfig returnFieldConfig;
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void setUp() {
        returnFieldConfig = ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIPARC);
        searchFieldConfig = SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.UNIPARC);
        entry = createUniParcEntryLight();
    }

    @ParameterizedTest(
            name = "Sort field [{0}] configured in return field exists in search fields?")
    @MethodSource("provideReturnSortFields")
    void validReturnFieldSortFieldDefined(String returnFieldsSortField) {
        assertThat(searchFieldConfig.correspondingSortFieldExists(returnFieldsSortField), is(true));
    }

    @ParameterizedTest(name = "Sort field [{0}] configured in search exists in return fields?")
    @MethodSource("provideSearchSortFields")
    void validSearchFieldSortFieldDefined(String searchFieldsSortField) {
        boolean found =
                returnFieldConfig.getReturnFields().stream()
                        .map(ReturnField::getSortField)
                        .filter(Objects::nonNull)
                        .map(searchFieldConfig::getCorrespondingSortField)
                        .map(SearchFieldItem::getFieldName)
                        .anyMatch(
                                sortFieldName ->
                                        sortFieldName.equalsIgnoreCase(searchFieldsSortField));
        assertTrue(found);
    }

    @ParameterizedTest(name = "Return TSV column [{0}] for return field exists?")
    @MethodSource("provideReturnFieldNames")
    void validReturnFieldWithMappedEntryDefined(String returnFieldName) {

        UniParcEntryLightValueMapper entityValueMapper = new UniParcEntryLightValueMapper();
        Map<String, String> mappedField =
                entityValueMapper.mapEntity(entry, Collections.singletonList(returnFieldName));
        System.out.println(returnFieldName + " : " + mappedField.get(returnFieldName));
        assertNotNull(mappedField.get(returnFieldName));
        assertFalse(mappedField.get(returnFieldName).isEmpty());
    }

    @Test
    void testInternalReturnFields() {
        List<String> expectedInternalNames = List.of("fullSequence", "fullsequencefeatures");
        List<ReturnField> internal =
                returnFieldConfig.getReturnFields().stream()
                        .filter(rf -> Objects.isNull(rf.getParentId()))
                        .collect(Collectors.toList());
        assertNotNull(internal);
        assertEquals(2, internal.size());
        List<String> internalNames =
                internal.stream().map(ReturnField::getId).collect(Collectors.toList());

        assertEquals(expectedInternalNames, internalNames);
    }

    private static Stream<Arguments> provideSearchSortFields() {
        return searchFieldConfig.getSortFieldItems().stream()
                .map(SearchFieldItem::getFieldName)
                .map(Arguments::of);
    }

    private static Stream<Arguments> provideReturnSortFields() {
        return returnFieldConfig.getReturnFields().stream()
                .filter(field -> Utils.notNullNotEmpty(field.getSortField()))
                .map(ReturnField::getSortField)
                .map(Arguments::of);
    }

    private static Stream<Arguments> provideReturnFieldNames() {
        return returnFieldConfig.getReturnFields().stream()
                .filter(rf -> Objects.nonNull(rf.getChildNumber()))
                .map(ReturnField::getName)
                .map(Arguments::of);
    }

    private static UniParcEntryLight createUniParcEntryLight() {
        String uniParcId = getName("UPI", 2);
        StringBuilder seq = new StringBuilder("MLMPKRTKYR");
        IntStream.range(0, 2).forEach(j -> seq.append("A"));
        Sequence sequence = new SequenceBuilder(seq.toString()).build();
        List<SequenceFeature> seqFeatures = new ArrayList<>();
        Arrays.stream(SignatureDbType.values())
                .forEach(signatureType -> seqFeatures.add(getSeqFeature(2, signatureType)));
        List<CommonOrganism> commonTaxons = getCommonTaxons();
        Organism organism1 = new OrganismBuilder().taxonId(9606).scientificName("Homo sapiens").build();
        Organism organism2 = new OrganismBuilder().taxonId(10090).scientificName("MOUSE").build();
        return new UniParcEntryLightBuilder()
                .uniParcId(uniParcId)
                .geneNamesSet(new LinkedHashSet<>(List.of("gene1","gene2")))
                .commonTaxonsSet(commonTaxons)
                .crossReferenceCount(3)
                .uniProtKBAccessionsAdd(getName("P123", 2))
                .sequence(sequence)
                .proteomesSet(new LinkedHashSet<>(List.of(new ProteomeBuilder().id("UP000005640").component("C1").build(), new ProteomeBuilder().id("UP000002494").component("C2").build())))
                .proteinNamesSet(new LinkedHashSet<>(List.of("protein1", "protein2")))
                .organismsSet( new LinkedHashSet<>(List.of(organism1, organism2)))
                .sequenceFeaturesSet(seqFeatures)
                .oldestCrossRefCreated(LocalDate.now())
                .mostRecentCrossRefUpdated(LocalDate.now())
                .build();
    }

    private static List<CommonOrganism> getCommonTaxons() {
        return List.of(
                new CommonOrganismBuilder()
                        .topLevel("cellular organisms")
                        .commonTaxon("Bacteria")
                        .build(),
                new CommonOrganismBuilder()
                        .topLevel("other entries")
                        .commonTaxon("plasmids")
                        .build());
    }

    private static SequenceFeature getSeqFeature(int i, SignatureDbType signatureDbType) {
        List<SequenceFeatureLocation> locations =
                Arrays.asList(
                        new SequenceFeatureLocationBuilder().range(12, 23).alignment("55M").build(),
                        new SequenceFeatureLocationBuilder().range(45, 89).build());
        InterProGroup domain =
                new InterProGroupBuilder()
                        .name(getName("Inter Pro Name", i))
                        .id(getName("IP0000", i))
                        .build();
        return new SequenceFeatureBuilder()
                .interproGroup(domain)
                .signatureDbType(signatureDbType)
                .signatureDbId(getName("SIG0000", i))
                .locationsSet(locations)
                .build();
    }

    private static String getName(String prefix, int i) {
        return String.format(prefix + "%02d", i);
    }
}
