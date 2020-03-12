package org.uniprot.store.indexer.uniprot.mockers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.uniprot.store.indexer.uniprot.mockers.UniProtEntryMocker.Type.SP;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtkbEntry;

/**
 * Created 19/09/18
 *
 * @author Edd
 */
public class UniProtkbEntryMockerTest {
    @Test
    public void canCreateSP() {
        UniProtkbEntry uniProtkbEntry = UniProtEntryMocker.create(SP);
        assertThat(uniProtkbEntry, is(notNullValue()));
    }

    @Test
    public void canCreateEntryWithAccession() {
        String accession = "P12345";
        UniProtkbEntry uniProtkbEntry = UniProtEntryMocker.create(accession);
        assertThat(uniProtkbEntry, is(notNullValue()));
        assertThat(uniProtkbEntry.getPrimaryAccession().getValue(), is(accession));
    }

    @Test
    public void canCreateEntries() {
        Collection<UniProtkbEntry> entries = UniProtEntryMocker.createEntries();
        assertThat(entries, hasSize(greaterThan(0)));
    }
}
