package org.uniprot.store.config.returnfield.config.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.json.parser.uniprot.UniProtKBEntryIT;
import org.uniprot.core.parser.tsv.uniprot.UniProtKBEntryValueMapper;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;
import org.uniprot.store.config.returnfield.model.ReturnField;

/**
 * @author lgonzales
 * @since 2020-03-24
 */
class ReturnFieldConfigWithTsvMapperIT {

    private static UniProtKBEntry entry;

    @BeforeAll
    static void setUp() {
        entry = UniProtKBEntryIT.getCompleteColumnsUniProtEntry();
    }

    @ParameterizedTest(name = "Return field [{0}] for return field exists?")
    @MethodSource("provideReturnFieldNames")
    void validReturnFieldWithMappedEntryDefined(String returnFieldName) {
        UniProtKBEntryValueMapper entityValueMapper = new UniProtKBEntryValueMapper();
        Map<String, String> mappedField =
                entityValueMapper.mapEntity(entry, Collections.singletonList(returnFieldName));
        System.out.println(returnFieldName + " : " + mappedField.get(returnFieldName));
        assertNotNull(mappedField.get(returnFieldName));
        assertFalse(mappedField.get(returnFieldName).isEmpty());
    }

    private static Stream<Arguments> provideReturnFieldNames() {
        ReturnFieldConfig returnFieldConfig =
                ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIPROTKB);
        return returnFieldConfig.getReturnFields().stream()
                .map(ReturnField::getName)
                .map(Arguments::of);
    }
}
