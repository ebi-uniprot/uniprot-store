package org.uniprot.store.datastore.member.uniref;

import static org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore.getVoldemortKey;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Slf4j
public class UniRefMemberRetryWriter
        extends ItemRetryWriter<RepresentativeMember, RepresentativeMember> {

    public UniRefMemberRetryWriter(
            Store<RepresentativeMember> store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
    }

    @Override
    protected String extractItemId(RepresentativeMember item) {
        return getVoldemortKey(item);
    }

    @Override
    protected String entryToString(RepresentativeMember entry) {
        return getVoldemortKey(entry);
    }

    @Override
    public RepresentativeMember itemToEntry(RepresentativeMember item) {
        return item;
    }
}
