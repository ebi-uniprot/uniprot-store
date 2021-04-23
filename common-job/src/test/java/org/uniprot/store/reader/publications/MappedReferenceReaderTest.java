package org.uniprot.store.reader.publications;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.ComputationallyMappedReference;

/**
 * @author sahmad
 * @created 21/01/2021
 */
class MappedReferenceReaderTest {
    public static final String FILE_PATH = "src/test/resources/computational_pir_map.txt";

    @Test
    void testGetNext() throws IOException {
        ComputationallyMappedReferenceConverter converter =
                new ComputationallyMappedReferenceConverter();
        MappedReferenceReader<ComputationallyMappedReference> reader =
                new MappedReferenceReader<>(converter, FILE_PATH);
        List<ComputationallyMappedReference> refs;
        while ((refs = reader.readNext()) != null) {
            if (refs.size() > 1) {
                assertThat(refs.size(), is(2));
                assertThat(refs.get(0).getCitationId(), is("11203701"));
                assertThat(refs.get(1).getCitationId(), is("11203701"));
                assertThat(refs.get(0).getUniProtKBAccession().getValue(), is("P21802"));
                assertThat(refs.get(1).getUniProtKBAccession().getValue(), is("P21802"));
            } else {
                assertThat(refs.size(), is(1));
            }
        }
    }
}
