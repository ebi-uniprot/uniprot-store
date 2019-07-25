package uk.ac.ebi.uniprot.datastore.voldemort.uniprot;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.uniprot.datastore.voldemort.MetricsUtil;
import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortClient;
import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortEntryStoreBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntryType;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtEntryBuilder;
import uk.ac.ebi.uniprot.flatfile.parser.SupportingDataMap;
import uk.ac.ebi.uniprot.flatfile.parser.UniprotLineParser;
import uk.ac.ebi.uniprot.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import uk.ac.ebi.uniprot.flatfile.parser.impl.EntryBufferedReader2;
import uk.ac.ebi.uniprot.flatfile.parser.impl.SupportingDataMapImpl;
import uk.ac.ebi.uniprot.flatfile.parser.impl.entry.EntryObject;
import uk.ac.ebi.uniprot.flatfile.parser.impl.entry.EntryObjectConverter;
import uk.ebi.uniprot.scorer.uniprotkb.UniProtEntryScored;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class contains methods that are used to build Voldemort Uniprot data.
 *
 * Created 05/10/2017
 *
 * @author lgonzales
 */
public class VoldemortUniprotEntryStoreBuilder extends VoldemortEntryStoreBuilder<UniProtEntry> {

    private static final Logger logger = LoggerFactory.getLogger(VoldemortEntryStoreBuilder.class);

    private static ThreadLocal<UniprotLineParser<EntryObject>> threadLocal = new ThreadLocal<UniprotLineParser<EntryObject>>();
    private static ThreadLocal<EntryObjectConverter> converterThreadLocal = new ThreadLocal<EntryObjectConverter>();
    private static SupportingDataMap supportingDataMap;
    private final Counter counter_store_sw;
    private final Counter counter_store_tr;
    private final Counter counter_store_isoform;

    public VoldemortUniprotEntryStoreBuilder(VoldemortClient<UniProtEntry> remoteStore, String metricsPrefix) throws IOException {
        super(remoteStore, metricsPrefix);
        this.counter_store_sw = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-store-success-swissprot");
        this.counter_store_tr = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-store-success-trembl");
        this.counter_store_isoform = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-store-success-isoform");
    }

    public static void setFilePaths(String diseaseFilePath,String keywordFilePath,String goFilePath,String subccellularLocationPath){
        SupportingDataMap supportingDataMap = new SupportingDataMapImpl(keywordFilePath,diseaseFilePath,goFilePath,subccellularLocationPath);
        VoldemortUniprotEntryStoreBuilder.supportingDataMap = supportingDataMap;
    }

    //the parser is not thread safe.
    private static UniprotLineParser<EntryObject> getUniprotEntryParser() {
        if (threadLocal.get() == null) {
            DefaultUniprotLineParserFactory defaultUniprotLineParserFactory = new DefaultUniprotLineParserFactory();
            UniprotLineParser<EntryObject> entryParser = defaultUniprotLineParserFactory.createEntryParser();
            threadLocal.set(entryParser);
        }
        return threadLocal.get();
    }

    private static EntryObjectConverter getEntryObjectConverter() {
        if (converterThreadLocal.get() == null) {
            converterThreadLocal.set(new EntryObjectConverter(supportingDataMap,false));
        }
        return converterThreadLocal.get();
    }

    public void build(String ff, String parseFailFile, String storingFailFile, boolean retry) throws IOException {

        logger.info("Loading from file: {}", ff);

        PrintWriter parsingFailed = new PrintWriter(new FileOutputStream(parseFailFile, true));
        PrintWriter storingFailed = new PrintWriter(new FileOutputStream(storingFailFile, true));

        VoldemortEntryStoreBuilder.LimitedQueue<Runnable> queue = new VoldemortEntryStoreBuilder.LimitedQueue<>(50000);

        //new blocking queue.
        int coreNumber = Runtime.getRuntime().availableProcessors();
        logger.info("Core Number: {}", coreNumber);
        int numThread = Math.max(8, coreNumber);
        numThread = Math.min(numThread, 32);
        ExecutorService
                executorService = new ThreadPoolExecutor(numThread, 32, 30, TimeUnit.SECONDS, queue);

        EntryBufferedReader2 entryBufferReader2 = new EntryBufferedReader2(ff);

        do {
            String next = null;
            Timer.Context time = this.string_splitter_time.time();
            try {
                next = entryBufferReader2.next();
            } catch (Exception e) {
                logger.warn("Error splitting entry string");
                e.printStackTrace();
            } finally {
                time.stop();
            }

            if (next == null) {
                break;
            } else {
                if (!retry) {
                    //only count for the first splitter, i.e., not the retry one.
                    counter_total.inc();
                }
                executorService.submit(new VoldemortUniprotEntryStoreBuilder.StoringJob(next, parsingFailed, storingFailed));
            }
        } while (true);

        reporter.report();
        logger.info("Done strings plitting.");

        executorService.shutdown();
        logger.info("Shutting down executor");

        try {
            executorService.awaitTermination(15, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.warn("Executer waiting interrupted.", e);
        }

        parsingFailed.close();
        storingFailed.close();

        logger.info("Done entry storing.");
        reporter.report();
    }

    public class StoringJob implements Runnable {

        private final String entryStr;
        private final PrintWriter parsingFailed;
        private final PrintWriter storingFailed;

        public StoringJob(String entryStr, PrintWriter parsingFailed, PrintWriter storingFailed) {
            this.entryStr = entryStr;
            this.parsingFailed = parsingFailed;
            this.storingFailed = storingFailed;
        }

        private void parseFailed(String entryStr, Timer.Context timer1) {
            synchronized (parsingFailed) {
                parsingFailed.print(entryStr);
                parsingFailed.flush();
            }
            counter_parse_fail.inc();
            hasParseFailure.set(true);
            timer1.stop();
        }

        public void run() {
            String acc = null;
            EntryObject parse = null;
            UniProtEntry entryObject = null;
            Timer.Context time1 = entry_parse_convert_time.time();
            Timer.Context time2 = entry_store_time.time();

            try {
                parse = getUniprotEntryParser().parse(entryStr);
                acc = parse.ac.primaryAcc;
            } catch (Exception e) {
                parseFailed(entryStr, time1);
                logger.warn("Error on FF parsing: {}",  entryStr, e);
            }

            if (parse != null) {
                try {
                    entryObject = getEntryObjectConverter().convert(parse);
                    counter_parse_success.inc();
/*                    if(goExtendedEvidences != null){
                        addExtendedEvidencesToEntryObject(convert);
                    }*/
                } catch (Exception e) {
                    parseFailed(entryStr, time1);
                    logger.warn("Error on converting entry from FF Entry Object: " + acc);
                } finally {
                    time1.stop();
                }
            }

            if (entryObject != null) {
                try {
                    UniProtEntryScored entryScored = new UniProtEntryScored(entryObject);
                    double score = entryScored.score();
                    UniProtEntryBuilder.ActiveEntryBuilder builder = new UniProtEntryBuilder().from(entryObject);
                    builder.annotationScore(score);
                    entryObject = builder.build();

                    remoteStore.saveEntry(entryObject);
                    counter_store_success.inc();

                    if(entryObject.getEntryType() == UniProtEntryType.SWISSPROT){
                        if (entryObject.getPrimaryAccession().getValue().contains("-")) {
                            counter_store_isoform.inc();
                        }else{
                            counter_store_sw.inc();
                        }
                    }else if(entryObject.getEntryType() == UniProtEntryType.TREMBL){
                        counter_store_tr.inc();
                    }

                } catch (Exception e) {
                    synchronized (storingFailed) {
                        storingFailed.print(entryStr);
                        storingFailed.flush();
                    }
                    hasStoreFailure.set(true);
                    counter_store_fail.inc();
                    logger.warn("Error on storing acc: " + acc +" with error message: "+ e.getMessage());
                } finally {
                    time2.stop();
                }
            } else {
                time2.stop();
            }
        }
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
        String goTermFilePath = commandParameters.getGoExtendedPMIDPath();
        String keywordFilePath = commandParameters.getKeywordFilePath();
        String diseaseFilePath = commandParameters.getDiseaseFilePath();
        String voldemortUrl = commandParameters.getVoldemortUrl();
        String releaseNumber = commandParameters.getReleaseNumber();
        String statsFilePath = commandParameters.getStatsFilePath();
        String subcellularLocationFilePath = commandParameters.getSubcellularLocationFilePath();

        logger.info("Loading from file: {}", uniprotFFPath);
        logger.info("voldemort server: {}", voldemortUrl);
        VoldemortRemoteUniProtKBEntryStore voldemortRemoteEntryStore = new VoldemortRemoteUniProtKBEntryStore("avro-uniprot", voldemortUrl);

        VoldemortUniprotEntryStoreBuilder.setFilePaths(diseaseFilePath,keywordFilePath,goTermFilePath,subcellularLocationFilePath);
        VoldemortUniprotEntryStoreBuilder voldemortEntryStoreBuiler = new VoldemortUniprotEntryStoreBuilder(voldemortRemoteEntryStore, "uniprot");

        logger.info("Loading Entries from file: {}", uniprotFFPath);
        voldemortEntryStoreBuiler.retryBuild(uniprotFFPath);

        logger.info("Building release {} stats file: {} ", releaseNumber, statsFilePath);
        voldemortEntryStoreBuiler.generateStatsFile(statsFilePath, releaseNumber);
        logger.info("All finished.");
    }
}
