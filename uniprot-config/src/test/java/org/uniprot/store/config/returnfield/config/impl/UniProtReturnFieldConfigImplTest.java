package org.uniprot.store.config.returnfield.config.impl;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Created 16/03/20
 *
 * @author Edd
 */
class UniProtReturnFieldConfigImplTest {
    @Test
    void ensureThereAreNoDynamicFields() {
        UniProtReturnFieldConfigImpl config =
                new UniProtReturnFieldConfigImpl("test-return-fields-no-dynamic-fields.json");
        assertThat(config.dynamicallyLoadFields(), is(emptyList()));
    }
}
