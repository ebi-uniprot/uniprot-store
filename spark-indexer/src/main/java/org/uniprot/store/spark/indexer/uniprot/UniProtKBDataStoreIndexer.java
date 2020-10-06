package org.uniprot.store.spark.indexer.uniprot;

import java.util.Iterator;
import java.util.Locale;
import java.util.ResourceBundle;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidence;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidenceMapper;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidencesRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtKBAnnotationScoreMapper;
import org.uniprot.store.spark.indexer.uniprot.writer.UniProtKBDataStoreWriter;

import static org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer.OS.*;
import static org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer.Arch.*;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniProtKBDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public UniProtKBDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {

        try{
            log.info("OS_NAME: {}", determineOS());
            log.info("ARCH_NAME: {}", determineArch());
        }catch ( Exception e){
            log.error("Exception from indexInDataStore:", e);
        }

        try { // Try system lib path first
            System.loadLibrary("brotli");
        } catch (UnsatisfiedLinkError linkError) {
            log.error("UnsatisfiedLinkError from indexInDataStore:", linkError);
        }

        String mappedLibrary = System.mapLibraryName("brotli");
        log.info("System.mapLibraryName: {}",mappedLibrary);

        System.getenv().forEach((key, value) -> log.info("ENV_VAR_KEY: {} ENV_VAR_VALUE: {}", key ,value));

        log.info("Checking brotli. isBrotliAvailable: {}", BrotliLoader.isBrotliAvailable());

        ResourceBundle config = parameter.getApplicationConfig();
        GOEvidencesRDDReader goEvidencesReader = new GOEvidencesRDDReader(parameter);
        UniProtKBRDDTupleReader uniprotkbReader = new UniProtKBRDDTupleReader(parameter, false);

        JavaPairRDD<String, UniProtKBEntry> uniprotRDD = uniprotkbReader.load();
        JavaPairRDD<String, Iterable<GOEvidence>> goEvidenceRDD = goEvidencesReader.load();

        String numberOfConnections = config.getString("store.uniprot.numberOfConnections");
        String storeName = config.getString("store.uniprot.storeName");
        String connectionURL = config.getString("store.uniprot.host");
        uniprotRDD
                .mapValues(new UniProtKBAnnotationScoreMapper())
                .leftOuterJoin(goEvidenceRDD)
                .mapValues(new GOEvidenceMapper())
                .values()
                .foreachPartition(getWriter(numberOfConnections, storeName, connectionURL));

        log.info("Completed UniProtKb Data Store index. and brotli {}",BrotliLoader.isBrotliAvailable());
    }

    VoidFunction<Iterator<UniProtKBEntry>> getWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        return new UniProtKBDataStoreWriter(numberOfConnections, storeName, connectionURL);
    }

    private static String determineOS() {
        String osName = System.getProperty("os.name").toLowerCase(Locale.US);
        if (LINUX.matches(osName)) return LINUX.name;
        if (WIN32.matches(osName)) return WIN32.name;
        if (OSX.matches(osName)) return OSX.name;
        return null;
    }

    private static String determineArch() {
        String osArch = System.getProperty("os.arch").toLowerCase(Locale.US);
        if (X86_AMD64.matches(osArch)) return X86_AMD64.name;
        if (X86.matches(osArch)) return X86.name;
        if (ARM32_VFP_HFLT.matches(osArch)) return ARM32_VFP_HFLT.name;
        return null;
    }

    enum OS {
        WIN32("win32", "win32", "windows"),
        LINUX("linux", "linux", "unix"),
        OSX("darwin", "darwin", "mac os x", "mac", "osx");

        final String name;
        final String[] aliases;

        OS(String name, String... aliases) {
            this.name = name;
            this.aliases = aliases;
        }

        boolean matches(String aName) {
            for (String alias : aliases) {
                if (aName.contains(alias)) return true;
            }
            return false;
        }

    }

    enum Arch {
        ARM32_VFP_HFLT("arm32-vfp-hflt", "arm32-vfp-hflt", "arm"),
        X86("x86", "x86", "i386", "i486", "i586", "i686", "pentium"),
        X86_AMD64("x86-amd64", "x86-amd64", "x86_64", "amd64", "em64t", "universal");

        final String name;
        final String[] aliases;

        Arch(String name, String... aliases) {
            this.name = name;
            this.aliases = aliases;
        }

        boolean matches(String aName) {
            for (String alias : aliases) {
                if (aName.contains(alias)) return true;
            }
            return false;
        }

    }
}
