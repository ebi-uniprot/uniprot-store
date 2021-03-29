package org.uniprot.store.config.returnfield.config.impl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.json.parser.uniparc.UniParcEntryTest;
import org.uniprot.core.parser.tsv.uniparc.UniParcEntryCrossRefValueMapper;
import org.uniprot.core.parser.tsv.uniparc.UniParcEntryValueMapper;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;
import org.uniprot.store.config.returnfield.model.ReturnField;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author sahmad
 * @created 23/03/2021
 */
public class UniParcCrossRefReturnFieldConfigImplIT {

    private static UniParcEntry entry;
    private static ReturnFieldConfig returnFieldConfig;

    @BeforeAll
    static void setUp() {
        returnFieldConfig = ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIPARC_CROSSREF);
        entry = UniParcEntryTest.getCompleteUniParcEntry();
    }

    @ParameterizedTest(name = "Return TSV column [{0}] for return field exists?")
    @MethodSource("provideReturnFieldNames")
    void validReturnFieldWithMappedEntryDefined(String returnFieldName) {
        UniParcEntryCrossRefValueMapper entityValueMapper = new UniParcEntryCrossRefValueMapper();
        Map<String, String> mappedField =
                entityValueMapper.mapEntity(entry.getUniParcCrossReferences().get(0), Collections.singletonList(returnFieldName));
        System.out.println(returnFieldName + " : " + mappedField.get(returnFieldName));
        assertNotNull(mappedField.get(returnFieldName));
        assertFalse(mappedField.get(returnFieldName).isEmpty());
    }

    private static Stream<Arguments> provideReturnFieldNames() {
        return returnFieldConfig.getReturnFields().stream()
                .map(ReturnField::getName)
                .map(Arguments::of);
    }
}
