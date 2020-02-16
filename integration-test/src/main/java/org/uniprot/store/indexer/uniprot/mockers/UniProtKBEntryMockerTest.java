package org.uniprot.store.indexer.uniprot.mockers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.uniprot.store.indexer.uniprot.mockers.UniProtEntryMocker.Type.SP;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;

/**
 * Created 19/09/18
 *
 * @author Edd
 */
public class UniProtKBEntryMockerTest {
    @Test
    public void canCreateSP() {
        UniProtKBEntry uniProtkbEntry = UniProtEntryMocker.create(SP);
        assertThat(uniProtkbEntry, is(notNullValue()));
    }

    @Test
    public void canCreateEntryWithAccession() {
        String accession = "P12345";
        UniProtKBEntry uniProtkbEntry = UniProtEntryMocker.create(accession);
        assertThat(uniProtkbEntry, is(notNullValue()));
        assertThat(uniProtkbEntry.getPrimaryAccession().getValue(), is(accession));
    }

    @Test
    public void canCreateEntries() {
        Collection<UniProtKBEntry> entries = UniProtEntryMocker.createEntries();
        assertThat(entries, hasSize(greaterThan(0)));
    }

    @Test
    public void canCloneEntries() {
        List<UniProtEntry> entries = UniProtEntryMocker.cloneEntries(SP, 10);
        assertThat(entries, hasSize(10));
    }
}
