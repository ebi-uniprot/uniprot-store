package org.uniprot.store.indexer.literature.reader;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;

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
            String entryPubmedId =
                    entry.getCitation()
                            .getCitationCrossReferenceByType(CitationDatabase.PUBMED)
                            .map(CrossReference::getId)
                            .orElse("");
            while ((nextEntry = this.delegate.read()) != null) {
                String nextPubmedId =
                        nextEntry
                                .getCitation()
                                .getCitationCrossReferenceByType(CitationDatabase.PUBMED)
                                .map(CrossReference::getId)
                                .orElse("");
                if (entryPubmedId.equals(
                        nextPubmedId)) { // update computational and/or community mappedProteinCount
                    LiteratureStatisticsBuilder statisticsBuilder =
                            LiteratureStatisticsBuilder.from(entry.getStatistics());
                    statisticsBuilder.computationallyMappedProteinCount(
                            entry.getStatistics().getComputationallyMappedProteinCount()
                                    + nextEntry
                                            .getStatistics()
                                            .getComputationallyMappedProteinCount());
                    statisticsBuilder.communityMappedProteinCount(
                            entry.getStatistics().getCommunityMappedProteinCount()
                                    + nextEntry.getStatistics().getCommunityMappedProteinCount());
                    LiteratureEntryBuilder entryBuilder = LiteratureEntryBuilder.from(entry);
                    entryBuilder.statistics(statisticsBuilder.build());
                    entry = entryBuilder.build();
                } else {
                    return entry;
                }
            }
            return entry;
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
