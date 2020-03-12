package org.uniprot.store.datastore.voldemort.client;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.store.datastore.voldemort.VoldemortEntryStoreBuilder;
import org.uniprot.store.datastore.voldemort.client.impl.DefaultClientFactory;

/**
 * Created 13/10/2016
 *
 * @author wudong
 */
public class ClientStressTest {

    private static final Logger logger = LoggerFactory.getLogger(ClientStressTest.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        String url = args[0];
        String accessionFile = args[1];
        int numberOfThread = Integer.parseInt(args[2]);

        logger.info(String.format("testing %s with number of thread %d", url, numberOfThread));

        AtomicLong counter = new AtomicLong();
        AtomicLong number_entry_counter = new AtomicLong();

        DefaultClientFactory defaultClientFactory = new DefaultClientFactory(url);

        UniProtClient uniProtClient = defaultClientFactory.createUniProtClient();

        VoldemortEntryStoreBuilder.LimitedQueue<Runnable> queue =
                new VoldemortEntryStoreBuilder.LimitedQueue<>(10000);

        // new blocking queue.
        // int coreNumber = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService =
                new ThreadPoolExecutor(numberOfThread, 32, 30, TimeUnit.SECONDS, queue);

        Path path = FileSystems.getDefault().getPath(accessionFile);

        Files.lines(path)
                .forEach(
                        (acc) -> {
                            number_entry_counter.incrementAndGet();
                            Runnable runnable =
                                    new Runnable() {
                                        @Override
                                        public void run() {
                                            Optional<UniProtkbEntry> entry = this.getEntry(acc);
                                            if (entry.isPresent()) {
                                                long l = counter.incrementAndGet();
                                                if (l % 10000 == 0) {
                                                    logger.info("get entries {}", l);
                                                }
                                            }
                                        }

                                        private Optional<UniProtkbEntry> getEntry(String acc) {
                                            if (acc != null && acc.length() > 2) {
                                                try {
                                                    return uniProtClient.getEntry(acc);
                                                } catch (Exception e) {
                                                    logger.error("error while get entry" + acc, e);
                                                    return Optional.empty();
                                                }
                                            } else {
                                                logger.error("error accession input: " + acc);
                                                return Optional.empty();
                                            }
                                        }
                                    };

                            executorService.submit(runnable);
                        });

        Thread.sleep(1000);
        executorService.shutdown();
        defaultClientFactory.close();
        logger.info(
                String.format("input acc: %d, got %d", number_entry_counter.get(), counter.get()));
    }

    public static class LimitedQueue<E> extends LinkedBlockingQueue<E> {
        public LimitedQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        public boolean offer(E e) {
            // turn offer() and add() into a blocking calls (unless interrupted)
            try {
                put(e);
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
