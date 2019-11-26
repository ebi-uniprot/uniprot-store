package org.uniprot.store.datastore.voldemort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * This class contains required voldemort parameters that are used to setup the voldemort load
 * execution.
 *
 * @author gqi
 */
public abstract class VoldemortStoreBuilderCommandParameters {

    private static final Logger logger =
            LoggerFactory.getLogger(VoldemortStoreBuilderCommandParameters.class);

    @Parameter(names = "-voldemortUrl", description = "The Voldemort URL", required = true)
    protected String voldemortUrl;

    @Parameter(names = "-importFilePath", description = "The imported file path", required = true)
    protected String importFilePath;

    @Parameter(
            names = "-releaseNumber",
            description = "The release number in the format: YYYY_MM, e.g., 2015_07",
            required = true)
    protected String releaseNumber;

    @Parameter(
            names = "-statsFile",
            description =
                    "The path to a statistics file that will be generated at the end of indexing",
            required = true)
    protected String statsFilePath;

    @Parameter(names = "--help", help = true)
    private boolean help;

    protected JCommander jCommander;

    public void showUsage() {
        this.jCommander.usage();
    }

    public String getVoldemortUrl() {
        return voldemortUrl;
    }

    public String getImportFilePath() {
        return importFilePath;
    }

    public String getReleaseNumber() {
        return releaseNumber;
    }

    public String getStatsFilePath() {
        return statsFilePath;
    }

    public boolean isHelp() {
        return help;
    }
}
