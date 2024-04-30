package org.uniprot.store.datastore.member.uniref;

import static java.lang.Math.toIntExact;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.job.common.listener.LogRateListener;

import lombok.extern.slf4j.Slf4j;

/**
 * @@author sahmad
 *
 * @created 29/07/2020
 */
@Slf4j
public class UniRef90And50MemberLogRateListener
        extends LogRateListener<List<RepresentativeMember>> {

    public UniRef90And50MemberLogRateListener(int writeRateDocumentInterval) {
        super(writeRateDocumentInterval);
    }

    @Override
    public void afterWrite(List<? extends List<RepresentativeMember>> lists) {
        int totalCount = toIntExact(lists.stream().flatMap(Collection::stream).count());
        getDeltaWriteCount().addAndGet(totalCount);

        if (getDeltaWriteCount().get() >= getWriteRateDocumentInterval()) {
            log.info(computeWriteRateStats(Instant.now()).toString());
            resetDelta();
        }
    }
}
