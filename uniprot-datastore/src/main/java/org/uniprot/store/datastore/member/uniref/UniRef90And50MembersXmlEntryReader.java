package org.uniprot.store.datastore.member.uniref;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.uniprot.core.util.Utils;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.store.job.common.reader.XmlItemReader;
import org.uniprot.store.job.common.util.CommonConstants;

/**
 * @author sahmad
 * @since 23/07/2020
 */
public class UniRef90And50MembersXmlEntryReader implements ItemReader<List<MemberType>> {

    public static final String UNIREF_ROOT_ELEMENT = "entry";
    private final OnZeroCountSleeper sleeper;
    private List<MemberType> members;
    private final XmlItemReader<Entry> entryReader;
    private final int batchSize;
    private AtomicInteger readEntriesCount;

    public UniRef90And50MembersXmlEntryReader(String filepath, int batchSize) {
        this.entryReader = new XmlItemReader<>(filepath, Entry.class, UNIREF_ROOT_ELEMENT);
        this.batchSize = batchSize;
        this.members = new ArrayList<>();
        this.sleeper = new OnZeroCountSleeper();
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        stepExecution
                .getJobExecution()
                .getExecutionContext()
                .put(CommonConstants.ENTRIES_TO_WRITE_COUNTER, sleeper);
        this.readEntriesCount = new AtomicInteger(0);
        executionContext.put(CommonConstants.READ_ENTRIES_COUNT_KEY, this.readEntriesCount);
    }

    @Override
    public List<MemberType> read() {
        boolean isLastBatch =
                (!members.isEmpty()
                        && members.size() < this.batchSize
                        && !entryReader.getEntryIterator().hasNext());
        if ((members.size() >= this.batchSize) || isLastBatch) {
            List<MemberType> currentBatch;
            if (!isLastBatch) {
                this.sleeper.add(batchSize);
                this.readEntriesCount.addAndGet(batchSize);
                currentBatch = new ArrayList<>(members.subList(0, batchSize));
                members.subList(0, batchSize).clear();
            } else {
                this.sleeper.add(members.size());
                this.readEntriesCount.addAndGet(members.size());
                currentBatch = new ArrayList<>(members);
                members.clear();
            }
            return currentBatch;
        } else if (entryReader.getEntryIterator().hasNext()) {
            Entry entry = entryReader.getEntryIterator().next();
            members = getAllMembers(entry);
            return read();
        } else {
            return null;
        }
    }

    private List<MemberType> getAllMembers(Entry entry) {
        List<MemberType> currentMembers = new ArrayList<>();
        currentMembers.add(entry.getRepresentativeMember());
        if (Utils.notNullNotEmpty(entry.getMember())) {
            entry.getMember().stream().forEach(currentMembers::add);
        }
        return currentMembers;
    }
}
