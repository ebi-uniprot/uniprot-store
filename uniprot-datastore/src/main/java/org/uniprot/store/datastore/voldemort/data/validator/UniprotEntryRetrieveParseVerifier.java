package org.uniprot.store.datastore.voldemort.data.validator;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Slf4jReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.flatfile.parser.impl.EntryBufferedReader2;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.UniProtEntryType;
import org.uniprot.store.datastore.voldemort.MetricsUtil;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.VoldemortEntryStoreBuilder;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortUniprotStoreBuilderCommandParameters;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class is responsible to test if we are able to parse back to uniprot entry for all entries saved in Voldemort
 * It generate a statistic file with numbers.
 * It also generate another file with all accessions that failed to parse, I am printing in the log the exception that
 * happened during the parse.
 *
 * Created By lgonzales
 */
public class UniprotEntryRetrieveParseVerifier {

    private static final Logger logger = LoggerFactory.getLogger(UniprotEntryRetrieveParseVerifier.class);
    private final VoldemortClient<UniProtEntry> remoteStore;
    protected final Slf4jReporter reporter;

    private final Counter counter_parsed_sw;
    private final Counter counter_parsed_tr;
    private final Counter counter_parsed_isoform;
    private final Counter counter_parsed_fail;
    private final Counter counter_entry_total;
    private final Counter counter_not_found;

    UniprotEntryRetrieveParseVerifier(VoldemortClient<UniProtEntry> store){
        this.remoteStore = store;

        Slf4jReporter reporter = Slf4jReporter.forRegistry(MetricsUtil.getMetricRegistryInstance()).build();
        reporter.start(10, TimeUnit.MINUTES);
        this.reporter = reporter;

        this.counter_parsed_sw = MetricsUtil.getMetricRegistryInstance().counter("uniprot-entry-parse-success-swissprot");
        this.counter_parsed_tr = MetricsUtil.getMetricRegistryInstance().counter("uniprot-entry-parse-success-trembl");
        this.counter_parsed_isoform = MetricsUtil.getMetricRegistryInstance().counter("uniprot-entry-parse-success-isoform");
        this.counter_parsed_fail = MetricsUtil.getMetricRegistryInstance().counter("uniprot-entry-parse-fail");
        this.counter_entry_total = MetricsUtil.getMetricRegistryInstance().counter("uniprot-entry-total");
        this.counter_not_found = MetricsUtil.getMetricRegistryInstance().counter("uniprot-entry-not-found");
    }

    void executeVerification(String uniprotFFPath) throws IOException{
        PrintWriter parsingFailed = new PrintWriter(new FileOutputStream("uniprot.parse.fail.txt", true));

        VoldemortEntryStoreBuilder.LimitedQueue<Runnable> queue = new VoldemortEntryStoreBuilder.LimitedQueue<>(50000);

        //new blocking queue.
        int coreNumber = Runtime.getRuntime().availableProcessors();
        int numThread = Math.max(8, coreNumber);
        numThread = Math.min(numThread, 32);
        ExecutorService executorService = new ThreadPoolExecutor(numThread, 32, 30, TimeUnit.SECONDS, queue);

        EntryBufferedReader2 entryBufferReader2 = new EntryBufferedReader2(uniprotFFPath);

        do {
            String next = null;
            try {
                next = entryBufferReader2.next();
            } catch (Exception e) {
                logger.warn("Error splitting entry string");
                e.printStackTrace();
            }

            if (next == null) {
                break;
            } else {
                final String accession = getAccession(next);
                executorService.submit(new UniprotEntryRetrieveParseVerifier.DataVerificationJob(accession, parsingFailed));
            }
        } while (true);

        logger.info("Done strings splitting.");

        executorService.shutdown();
        logger.info("Shutting down executor");

        try {
            executorService.awaitTermination(15, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.warn("Executer waiting interrupted.", e);
        }

        parsingFailed.close();

        logger.info("Done entry storing.");
    }

    private String getAccession(String nextStr) {
        String[] lines = nextStr.split("\\n");
        String accessionLine = lines[1];
        if (accessionLine != null && accessionLine.startsWith("AC")) {
            return accessionLine.substring(2, accessionLine.indexOf(";")).trim();
        } else{
            logger.warn("Could not parse accession line to get extended Go Evicence {}",accessionLine);
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        VoldemortUniprotStoreBuilderCommandParameters commandParameters =
                VoldemortUniprotStoreBuilderCommandParameters.fromCommand(args);
        if (commandParameters == null) {
            return;
        }

        if (commandParameters.isHelp()) {
            commandParameters.showUsage();
            return;
        }

        String uniprotFFPath = commandParameters.getImportFilePath();
        String voldemortUrl = commandParameters.getVoldemortUrl();
        String statsFilePath = commandParameters.getStatsFilePath();

        logger.info("Loading from file: {}", uniprotFFPath);
        logger.info("voldemort server: {}", voldemortUrl);
        VoldemortClient<UniProtEntry> store = new VoldemortRemoteUniProtKBEntryStore("avro-uniprot", voldemortUrl);
        UniprotEntryRetrieveParseVerifier dataVerification = new UniprotEntryRetrieveParseVerifier(store);
        dataVerification.executeVerification(uniprotFFPath);

        dataVerification.generateStatsFile(statsFilePath);
        logger.info("Done, The End!!!");
    }


    public class DataVerificationJob implements Runnable {

        private final String accession;
        private final PrintWriter parsingFailed;

        DataVerificationJob(String accession, PrintWriter parsingFailed) {
            this.accession = accession;
            this.parsingFailed = parsingFailed;
        }

        @Override
        public void run() {
            counter_entry_total.inc();
            try {
                Optional<UniProtEntry> voldemortResult = remoteStore.getEntry(this.accession);
                if (voldemortResult.isPresent()) {
                    UniProtEntry uniprotEntry = voldemortResult.get();
                    if (uniprotEntry.getEntryType() == UniProtEntryType.TREMBL) {
                        counter_parsed_tr.inc();
                    } else if (uniprotEntry.getEntryType() == UniProtEntryType.SWISSPROT) {
                        if (uniprotEntry.getPrimaryAccession().getValue().contains("-")) {
                            counter_parsed_isoform.inc();
                        } else {
                            counter_parsed_sw.inc();
                        }
                    }
                } else {
                    logger.warn("IMPORTANT: UNABLE TO FIND ACCESSION IN VOLDEMORT: " + this.accession);
                    counter_not_found.inc();
                }
            }catch (Exception e){
                logger.error("Error parsing Entry for accession: " + this.accession, e);
                counter_parsed_fail.inc();
                parsingFailed.write(this.accession);
            }
        }
    }

    static public class LimitedQueue<E> extends LinkedBlockingQueue<E> {
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

    private void generateStatsFile(String outputFileName) {
        SortedMap<String, Counter> counters = MetricsUtil.getMetricRegistryInstance().getCounters();
        Map<String, String> stats = counters.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue().getCount())));

        Properties properties = new Properties();
        properties.putAll(stats);

        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFileName)) {
            properties.store(fileOutputStream, null);
        } catch (IOException e) {
            logger.warn("Error while writing InfoObject file on path: " + outputFileName, e);
        }
    }

    Map<String, Counter> getExecutionStatistics(){
        return MetricsUtil.getMetricRegistryInstance().getCounters();
    }
}
