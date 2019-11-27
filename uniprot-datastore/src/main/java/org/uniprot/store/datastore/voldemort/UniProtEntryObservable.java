package org.uniprot.store.datastore.voldemort;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.EntryBufferedReader2;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprot.UniProtEntry;

import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;

/**
 * Created 26/04/2016
 *
 * @author wudong
 */
public class UniProtEntryObservable {

    private static final Logger logger = LoggerFactory.getLogger(UniProtEntryObservable.class);

    private static ThreadLocal<UniprotLineParser<EntryObject>> threadLocal =
            new ThreadLocal<UniprotLineParser<EntryObject>>();
    private static EntryObjectConverter converter =
            new EntryObjectConverter(new SupportingDataMapImpl("", "", "", ""), true);

    private static final MetricRegistry metrics = new MetricRegistry();

    static {
        Slf4jReporter reporter =
                Slf4jReporter.forRegistry(metrics)
                        .outputTo(logger)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build();
        reporter.start(1, TimeUnit.MINUTES);
    }

    public static Observable<UniProtEntry> fromFile(String filePath) throws FileNotFoundException {

        EntryBufferedReader2 entryBufferReader2 = new EntryBufferedReader2(filePath);
        EntryStringEmitter entryStringEmitter = new EntryStringEmitter(entryBufferReader2);

        Observable<String> stringObservable = Observable.create(entryStringEmitter);
        return stringObservable
                .observeOn(Schedulers.computation())
                .map(
                        s -> {
                            try {
                                EntryObject parse = getUniprotEntryParser().parse(s);
                                return converter.convert(parse);
                            } catch (Exception e) {
                                logger.warn("Parsing entry exception:", e);
                                logger.warn("Entry:\n{}", s);
                                return null;
                            }
                        })
                .filter(entry -> entry != null);
    }

    // the parser is not thread safe.
    private static UniprotLineParser<EntryObject> getUniprotEntryParser() {
        if (threadLocal.get() == null) {
            DefaultUniprotLineParserFactory defaultUniprotLineParserFactory =
                    new DefaultUniprotLineParserFactory();
            UniprotLineParser<EntryObject> entryParser =
                    defaultUniprotLineParserFactory.createEntryParser();
            threadLocal.set(entryParser);
        }
        return threadLocal.get();
    }

    public static class EntryStringEmitter implements Observable.OnSubscribe<String> {
        private final EntryBufferedReader2 entryReader;

        public EntryStringEmitter(EntryBufferedReader2 entryReader) {
            this.entryReader = entryReader;
        }

        @Override
        public void call(Subscriber<? super String> subscriber) {

            logger.info("Entry String Emitter is called.");

            if (!subscriber.isUnsubscribed()) {
                subscriber.onStart();
            }

            while (true) {
                Timer.Context time = metrics.timer("entry-observer-time").time();
                try {

                    String next = entryReader.next();

                    if (next != null) {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onNext(next);
                        }
                    } else {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onCompleted();
                        }
                        logger.info("Entry String Emitter Completed.");
                        break;
                    }
                } catch (Exception e) {
                    if (!subscriber.isUnsubscribed()) {
                        subscriber.onError(e);
                    }
                    logger.info("Entry String Emitter Errored.");
                    break;
                } finally {
                    time.stop();
                }
            }

            logger.info("Entry String Emitter Finished.");
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        Observable<UniProtEntry> uniProtEntryObservable = UniProtEntryObservable.fromFile(args[0]);
        AtomicLong counter = new AtomicLong(0);
        uniProtEntryObservable.subscribe(
                i -> {
                    counter.incrementAndGet();
                });
        logger.info("total count: " + counter.get());
    }
}
