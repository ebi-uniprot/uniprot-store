package org.uniprot.store.spark.indexer.uniprot;

import java.io.*;
import java.nio.file.*;
import java.util.Iterator;
import java.util.Locale;
import java.util.ResourceBundle;


import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.tukaani.xz.simple.X86;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidence;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidenceMapper;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidencesRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtKBAnnotationScoreMapper;
import org.uniprot.store.spark.indexer.uniprot.writer.UniProtKBDataStoreWriter;

import static org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer.Arch.*;
import static org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer.OS.*;

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

        BrotliLoader.isBrotliAvailable();
        log.info("Checking brotli. Line just after isBrotliAvailable() method , if any exception above, it means it is not working :-(");
        System.getenv().forEach((key, value) -> log.info("ENV_VAR: NAME="+key + ": VALUE=" + value));

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

    private static class BrotliLoader {

        /**
         * Base name of the Brotli library as compiled by CMake. This constant should NOT be changed.
         *
         * This is morphed according to OS by using System.mapLibraryName(). So for example:
         * Windows: brotli.dll
         * Linux:   libbrotli.so
         * Mac:     libbrotli.dylib
         */
        private static final String LIBNAME = "brotli";

        /**
         * Name of directory we create in the system temp folder when unpacking and loading the native library
         *
         * Must be at least 3 characters long, and should be unique to prevent clashing with existing folders in temp
         */
        private static final String DIR_PREFIX = "jvmbrotli";

        /**
         * Have we already loaded the native library? Used to avoid multiple load attempts in the same JVM instance
         */
        private static boolean libLoaded = false;

        /**
         * No sense trying to load again if we failed the first time
         */
        private static boolean loadAttempted = false;

        public static boolean isBrotliAvailable() {
            if(loadAttempted) {
                return libLoaded;
            }
            else {
                try {
                    loadBrotli();
                } catch (RuntimeException e) {
                    e.printStackTrace();
                } finally {
                    return libLoaded;
                }
            }
        }

        @Deprecated // Use isBrotliAvailable() instead. Will be made private in version 1.0.0.
        public static void loadBrotli() {
            if(loadAttempted) return;
            try { // Try system lib path first
                System.loadLibrary(LIBNAME);
                libLoaded = true;
            } catch (UnsatisfiedLinkError linkError) { // If system load fails, attempt to unpack from jar and then load
                try {
                    String nativeLibName = System.mapLibraryName(LIBNAME);
                    String libPath = "/lib/" + determineOsArchName() + "/" + nativeLibName;
                    NativeUtils.loadLibraryFromJar(DIR_PREFIX, libPath);
                    libLoaded = true;
                } catch (Throwable ioException) {
                    log.error("NativeUtils.loadLibraryFromJar fail", ioException);
                    try{
                        System.load("/usr/bin/"+LIBNAME);
                        libLoaded = true;
                        log.info("THERE WE GO!!!!! LOOKS LIKE IT WORKED!!!");
                    }catch (UnsatisfiedLinkError e){
                        log.error("/usr/local/bin/"+LIBNAME + "load failed as well", e);
                    }
                    throw new RuntimeException(ioException);
                }
            } finally {
                loadAttempted = true;
            }
        }

        private static String determineOsArchName() {
            String os = determineOS();
            String arch = determineArch();
            if(os == null) throw new RuntimeException("Unsupported operating system");
            if(arch == null) throw new RuntimeException("Unsupported architecture");
            return os + "-" + arch;
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

    private static class NativeUtils {

        /**
         * The minimum length a prefix for a file has to have according to {@link File#createTempFile(String, String)}}.
         */
        private static final int MIN_PREFIX_LENGTH = 3;

        /**
         * Private constructor - this class will never be instanced
         */
        private NativeUtils() { }

        public static void loadLibraryFromJar(String dirPrefix, String path) throws IOException {
            log.info("loadLibraryFromJar( {}, {} )", dirPrefix, path);
            if(dirPrefix.length() < MIN_PREFIX_LENGTH) {
                throw new IllegalArgumentException("The temp dir prefix has to be at least 3 characters long.");
            }

            if (null == path || !path.startsWith("/")) {
                throw new IllegalArgumentException("The path has to be absolute (start with '/').");
            }

            // Obtain filename from path
            String[] parts = path.split("/");
            String filename = (parts.length > 1) ? parts[parts.length - 1] : null;

            // Check if the filename is okay
            if (filename == null || filename.length() < MIN_PREFIX_LENGTH) {
                throw new IllegalArgumentException("The filename has to be at least 3 characters long.");
            }

            File temporaryDir = createTempDirectory(dirPrefix);
            temporaryDir.deleteOnExit();

            File temp = new File(temporaryDir, filename);
            log.info("TEMP PATH: {}", temp.getAbsolutePath());
            log.info("getResourceAsStream PATH: {}", path);
            try (InputStream is = NativeUtils.class.getResourceAsStream(path)) {
                Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                temp.delete();
                throw e;
            } catch (NullPointerException e) {
                temp.delete();
                throw new FileNotFoundException("File " + path + " was not found inside JAR.");
            }

            try {
                System.load(temp.getAbsolutePath());
                log.info("AFTER LOAD: {}", temp.getAbsolutePath());
            }finally {
                if (isPosixCompliant()) {
                    // Assume POSIX compliant file system, can be deleted after loading
                    temp.delete();
                } else {
                    // Assume non-POSIX, and don't delete until last file descriptor closed
                    temp.deleteOnExit();
                }
            }

            // create lock file
            final File lock = new File( temp.getAbsolutePath() + ".lock");
            lock.createNewFile();
            lock.deleteOnExit();

            cleanUnusedCopies(dirPrefix, filename);
        }



        private static boolean isPosixCompliant() {
            try {
                return FileSystems.getDefault()
                        .supportedFileAttributeViews()
                        .contains("posix");
            } catch (FileSystemNotFoundException
                    | ProviderNotFoundException
                    | SecurityException e) {
                return false;
            }
        }

        private static File createTempDirectory(String prefix) throws IOException {
            String tempDir = System.getProperty("java.io.tmpdir");
            File generatedDir = new File(tempDir, prefix + System.nanoTime());

            if (!generatedDir.mkdir())
                throw new IOException("Failed to create temp directory " + generatedDir.getName());

            return generatedDir;
        }

        private static void cleanUnusedCopies(String dirPrefix, String fileName) {
            // Find dir names starting with our prefix
            FileFilter tmpDirFilter = pathname -> pathname.getName().startsWith(dirPrefix);

            // Get all folders from system temp dir that match our filter
            String tmpDirName = System.getProperty("java.io.tmpdir");
            File[] tmpDirs = new File(tmpDirName).listFiles(tmpDirFilter);

            for (File tDir : tmpDirs) {
                // Create a file to represent the lock and test.
                File lockFile = new File( tDir.getAbsolutePath() + "/" + fileName + ".lock");

                // If lock file doesn't exist, it means this directory and lib file are no longer in use, so delete them
                if (!lockFile.exists()) {
                    File[] tmpFiles = tDir.listFiles();
                    for(File tFile : tmpFiles) {
                        tFile.delete();
                    }
                    tDir.delete();
                }
            }
        }
    }
}
