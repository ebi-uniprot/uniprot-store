package org.uniprot.store.search.field;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.domain.FieldGroup;

/**
 * @author jluo
 * @date: 1 May 2019
 */
public class ProteomeResultFieldsTest {
    @Test
    void test() {
        List<FieldGroup> result = ProteomeResultFields.INSTANCE.getResultFieldGroups();
        assertEquals(2, result.size());
    }
}
