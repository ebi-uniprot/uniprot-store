package org.uniprot.store.datastore.member.uniref;

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

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author sahmad
 * @since 23/07/2020
 */
public class UniRefMemberXmlEntryReader implements ItemReader<MemberType> {

    public static final String UNIREF_ROOT_ELEMENT = "entry";
    private final OnZeroCountSleeper sleeper;
    private Queue<MemberType> queue;
    private final XmlItemReader<Entry> entryReader;

    public UniRefMemberXmlEntryReader(String filepath) {
        entryReader = new XmlItemReader<>(filepath, Entry.class, UNIREF_ROOT_ELEMENT);
        queue = new LinkedList<>();
        this.sleeper = new OnZeroCountSleeper();
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(CommonConstants.ENTRIES_TO_WRITE_COUNTER, sleeper);
    }

    @Override
    public MemberType read() {
        if (!queue.isEmpty()) {
            sleeper.increment();
            return queue.remove();
        } else if (entryReader.getEntryIterator().hasNext()) {
            Entry entry = entryReader.getEntryIterator().next();
            queue = getAllMembers(entry);
            return read();
        } else {
            return null;
        }
    }

    private Queue<MemberType> getAllMembers(Entry entry) {
        Queue<MemberType> members = new LinkedList<>();
        members.add(entry.getRepresentativeMember());
        if (Utils.notNullNotEmpty(entry.getMember())) {
            entry.getMember().stream().forEach(members::add);
        }
        return members;
    }
}
