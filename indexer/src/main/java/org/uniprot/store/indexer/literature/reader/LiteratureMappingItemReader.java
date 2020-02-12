package org.uniprot.store.indexer.literature.reader;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.uniprot.core.DBCrossReference;
import org.uniprot.core.citation.CitationXrefType;
import org.uniprot.core.literature.LiteratureStoreEntry;
import org.uniprot.core.literature.builder.LiteratureStoreEntryBuilder;

/** @author lgonzales */
public class LiteratureMappingItemReader implements ItemReader<LiteratureStoreEntry>, ItemStream {

    private FlatFileItemReader<LiteratureStoreEntry> delegate;

    private LiteratureStoreEntry nextEntry;

    @Override
    public LiteratureStoreEntry read() throws Exception, UnexpectedInputException, ParseException {
        LiteratureStoreEntry entry;
        if (nextEntry != null) {
            entry = nextEntry;
        } else {
            entry = delegate.read();
        }
        if (entry != null) {
            String entryPubmedId =
                    entry.getLiteratureEntry()
                            .getCitation()
                            .getCitationXrefsByType(CitationXrefType.PUBMED)
                            .map(DBCrossReference::getId)
                            .orElse("");
            LiteratureStoreEntryBuilder itemBuilder = LiteratureStoreEntryBuilder.from(entry);
            while ((nextEntry = this.delegate.read()) != null) {
                String nextPubmedId =
                        nextEntry
                                .getLiteratureEntry()
                                .getCitation()
                                .getCitationXrefsByType(CitationXrefType.PUBMED)
                                .map(DBCrossReference::getId)
                                .orElse("");
                if (entryPubmedId.equals(nextPubmedId)) {
                    itemBuilder.literatureMappedReferencesAdd(
                            nextEntry.getLiteratureMappedReferences().get(0));
                } else {
                    return itemBuilder.build();
                }
            }
            return itemBuilder.build();
        }
        return null;
    }

    public void setDelegate(FlatFileItemReader<LiteratureStoreEntry> delegate) {
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
