package org.uniprot.store.job.common.processor;

import lombok.extern.slf4j.Slf4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.store.job.common.model.EntryDocumentPair;
import org.uniprot.store.search.document.Document;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;

/**
 * This class represents an {@link ItemProcessor} of {@link EntryDocumentPair}s, where conversion is
 * delegated to a {@link DocumentConverter} instance. If processing these pairs causes a {@link
 * DocumentConversionException} (thrown by the converter), the entry is written to a log file, and
 * the id of the entry is written to the standard log.
 *
 * <p>Created 18/04/19
 *
 * @author Edd
 */
@Slf4j
public abstract class EntryDocumentPairProcessor<
                E, D extends Document, P extends EntryDocumentPair<E, D>>
        implements ItemProcessor<P, P> {
    private static final Logger INDEXING_FAILED_LOGGER =
            LoggerFactory.getLogger("indexing-doc-conversion-failed-entries");
    private final DocumentConverter<E, D> converter;

    public EntryDocumentPairProcessor(DocumentConverter<E, D> converter) {
        this.converter = converter;
    }

    @Override
    public P process(P entryDocumentPair) throws Exception {
        E entry = entryDocumentPair.getEntry();
        try {
            entryDocumentPair.setDocument(converter.convert(entry));
            return entryDocumentPair;
        } catch (DocumentConversionException e) {
            writeFailedEntryToFile(entry);
            log.error("Error converting entry: " + extractEntryId(entry), e);
            return null; // => the item should not be written
            // https://docs.spring.io/spring-batch/trunk/reference/html/domain.html#domainItemProcessor
        }
    }

    public abstract String extractEntryId(E entry);

    public abstract String entryToString(E entry);

    private void writeFailedEntryToFile(E entry) {
        String entryFF = entryToString(entry);
        INDEXING_FAILED_LOGGER.error(entryFF);
    }
}
