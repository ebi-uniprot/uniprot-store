package org.uniprot.store.indexer.common.config;

import java.util.Map;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;

/**
 * In our Use Case we need to read Multiple Fasta Files from a Folder, and to support it, spring
 * batch has ({@link org.springframework.batch.item.file.MultiResourceItemReader}).
 *
 * <p>Also, in Order to iterate over fasta files we will need to use a {@link PeekableItemReader},
 * because the fasta separator is placed at the beginning of the fastaInput String.
 *
 * <p>MultiResourceItemReader requires a ({@link ResourceAwareItemReaderItemStream}) delegate, and
 * the current PeekableItemReader implementation does not support it.
 *
 * <p>This is why we had to implement this PeekableResourceAwareItemReader, that has a delegate that
 * support ResourceAwareItemReaderItemStream. For that, we copied SingleItemPeekableItemReader
 * implementation and changed its delegate to FlatFileItemReader. We also added the required methods
 * for ResourceAwareItemReaderItemStream interface.
 *
 * @author lgonzales
 * @since 03/11/2020
 */
public class PeekableResourceAwareItemReader<T>
        implements ItemStreamReader<T>,
                PeekableItemReader<T>,
                ResourceAwareItemReaderItemStream<T> {

    private FlatFileItemReader<T> delegate;

    private String fileName;

    private T next;

    private ExecutionContext executionContext = new ExecutionContext();

    /**
     * The item reader to use as a delegate. Items are read from the delegate and passed to the
     * caller in {@link #read()}.
     *
     * @param delegate the delegate to set
     */
    public void setDelegate(FlatFileItemReader<T> delegate) {
        this.delegate = delegate;
    }

    /**
     * Get the next item from the delegate (whether or not it has already been peeked at).
     *
     * @see ItemReader#read()
     */
    @Nullable
    @Override
    public T read() throws Exception {
        if (next != null) {
            T item = next;
            next = null;
            return item;
        }
        return delegate.read();
    }

    /**
     * Peek at the next item, ensuring that if the delegate is an {@link ItemStream} the state is
     * stored for the next call to {@link #update(ExecutionContext)}.
     *
     * @return the next item (or null if there is none).
     * @see PeekableItemReader#peek()
     */
    @Nullable
    @Override
    public T peek() throws Exception {
        if (next == null) {
            updateDelegate(executionContext);
            next = delegate.read();
        }
        return next;
    }

    /**
     * If the delegate is an {@link ItemStream}, just pass the call on, otherwise reset the peek
     * cache.
     *
     * @throws ItemStreamException if there is a problem
     * @see ItemStream#close()
     */
    @Override
    public void close() {
        next = null;
        if (delegate instanceof ItemStream) {
            delegate.close();
        }
        executionContext = new ExecutionContext();
    }

    /**
     * If the delegate is an {@link ItemStream}, just pass the call on, otherwise reset the peek
     * cache.
     *
     * @param executionContext the current context
     * @throws ItemStreamException if there is a problem
     * @see ItemStream#open(ExecutionContext)
     */
    @Override
    public void open(ExecutionContext executionContext) {
        next = null;
        if (delegate instanceof ItemStream) {
            delegate.open(executionContext);
        }
        executionContext = new ExecutionContext();
    }

    /**
     * If there is a cached peek, then retrieve the execution context state from that point. If
     * there is no peek cached, then call directly to the delegate.
     *
     * @param executionContext the current context
     * @throws ItemStreamException if there is a problem
     * @see ItemStream#update(ExecutionContext)
     */
    @Override
    public void update(ExecutionContext executionContext) {
        if (next != null) {
            // Get the last state from the delegate instead of using
            // current value.
            for (Map.Entry<String, Object> entry : this.executionContext.entrySet()) {
                executionContext.put(entry.getKey(), entry.getValue());
            }
            return;
        }
        updateDelegate(executionContext);
    }

    private void updateDelegate(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream) {
            delegate.update(executionContext);
        }
    }

    @Override
    public void setResource(Resource resource) {
        delegate.setResource(resource);
        fileName = resource.getFilename();
    }

    public String getResourceFileName() {
        return fileName;
    }
}
