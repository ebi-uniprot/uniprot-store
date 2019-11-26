package org.uniprot.store.indexer.literature.reader;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.builder.LiteratureEntryBuilder;

/** @author lgonzales */
public class LiteratureMappingItemReader implements ItemReader<LiteratureEntry>, ItemStream {

    private FlatFileItemReader<LiteratureEntry> delegate;

    private LiteratureEntry nextEntry;

    @Override
    public LiteratureEntry read() throws Exception, UnexpectedInputException, ParseException {
        LiteratureEntry entry;
        if (nextEntry != null) {
            entry = nextEntry;
        } else {
            entry = delegate.read();
        }
        if (entry != null) {
            LiteratureEntryBuilder itemBuilder = new LiteratureEntryBuilder().from(entry);
            while ((nextEntry = this.delegate.read()) != null) {
                if (entry.getPubmedId().equals(nextEntry.getPubmedId())) {
                    itemBuilder.addLiteratureMappedReference(
                            nextEntry.getLiteratureMappedReferences().get(0));
                } else {
                    return itemBuilder.build();
                }
            }
            return itemBuilder.build();
        }
        return null;
    }

    public void setDelegate(FlatFileItemReader<LiteratureEntry> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() throws ItemStreamException {
        this.delegate.close();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.delegate.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        this.delegate.update(executionContext);
    }
}
