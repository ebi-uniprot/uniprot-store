package org.uniprot.store.datastore.voldemort.uniprot;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.store.datastore.voldemort.VoldemortStoreBuilderCommandParameters;

/**
 * This class contains specific voldemort parameters that are used to setup Uniprot voldemort load execution.
 *
 * @author lgonzales
 */
public class VoldemortUniprotStoreBuilderCommandParameters extends VoldemortStoreBuilderCommandParameters{

    private static final Logger logger = LoggerFactory.getLogger(VoldemortUniprotStoreBuilderCommandParameters.class);

    @Parameter(names="-goExtendedPMIDPath", description="Go Extended PMID Cross Reference file path", required=true)
    private String goExtendedPMIDPath;

    @Parameter(names="-diseaseFilePath", description="Disease file path", required=true)
    private String diseaseFilePath;

    @Parameter(names="-keywordFilePath", description="Keyword file path", required=true)
    private String keywordFilePath;

    @Parameter(names="-subcellularLocationFilePath", description="subcellular Location File Path", required=true)
    private String subcellularLocationFilePath;

    String getGoExtendedPMIDPath() {
        return goExtendedPMIDPath;
    }

    String getDiseaseFilePath() {
        return diseaseFilePath;
    }

    String getKeywordFilePath() {
        return keywordFilePath;
    }

    String getSubcellularLocationFilePath(){
        return subcellularLocationFilePath;
    }

    public static VoldemortUniprotStoreBuilderCommandParameters fromCommand(String[] args) {
        VoldemortUniprotStoreBuilderCommandParameters commandParameters = new VoldemortUniprotStoreBuilderCommandParameters();
        commandParameters.jCommander = new JCommander(commandParameters);
        try {
            commandParameters.jCommander.parse(args);
        } catch (ParameterException e) {
            commandParameters.showUsage();
            logger.error("Error for Uniprot command line parameters: ", e);
            return null;
        }
        return commandParameters;
    }

}
