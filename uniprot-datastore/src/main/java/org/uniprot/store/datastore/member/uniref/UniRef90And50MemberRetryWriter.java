package org.uniprot.store.datastore.member.uniref;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

import com.google.common.base.Strings;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Slf4j
public class UniRef90And50MemberRetryWriter
        extends ItemRetryWriter<List<RepresentativeMember>, List<RepresentativeMember>> {

    public UniRef90And50MemberRetryWriter(
            Store<List<RepresentativeMember>> store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
    }

    @Override
    protected String extractItemId(List<RepresentativeMember> items) {
        return items.stream()
                .map(VoldemortRemoteUniRefMemberStore::getVoldemortKey)
                .collect(Collectors.joining(","));
    }

    @Override
    protected String entryToString(List<RepresentativeMember> entries) {
        return entries.stream()
                .map(VoldemortRemoteUniRefMemberStore::getVoldemortKey)
                .collect(Collectors.joining(","));
    }

    @Override
    public List<RepresentativeMember> itemToEntry(List<RepresentativeMember> items) {
        return items;
    }

    @Override
    protected void writeEntriesToStore(List<? extends List<RepresentativeMember>> items) {
        List<List<RepresentativeMember>> convertedItems =
                items.stream().map(this::itemToEntry).collect(Collectors.toList());
        getStore().save(convertedItems);
        int totalCount = toIntExact(convertedItems.stream().flatMap(Collection::stream).count());
        getWrittenEntriesCount().addAndGet(totalCount);
        recordItemsWereProcessed(totalCount);
    }

    @Override
    protected void logFailedEntriesToFile(
            List<? extends List<RepresentativeMember>> items, Throwable throwable) {
        List<String> accessions = new ArrayList<>();
        if (!Strings.isNullOrEmpty(getHeader())) STORE_FAILED_LOGGER.error(getHeader());
        for (List<RepresentativeMember> item : items) {
            String entryFF = entryToString(item);
            accessions.add(extractItemId(item));
            STORE_FAILED_LOGGER.error(entryFF);
        }
        if (!Strings.isNullOrEmpty(getFooter())) STORE_FAILED_LOGGER.error(getFooter());
        log.error(ERROR_WRITING_ENTRIES_TO_STORE + accessions, throwable);
        int totalCount = toIntExact(items.stream().flatMap(Collection::stream).count());
        getFailedWritingEntriesCount().addAndGet(totalCount);
        recordItemsWereProcessed(totalCount);
    }
}
