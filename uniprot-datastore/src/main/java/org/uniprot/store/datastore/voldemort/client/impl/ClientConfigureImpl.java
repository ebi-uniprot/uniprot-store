package org.uniprot.store.datastore.voldemort.client.impl;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.google.common.base.Strings;

public class ClientConfigureImpl implements ClientConfigure {
    @Parameter(
            names = "-e",
            splitter = CommaParameterSplitter.class,
            description = "a list of accessions")
    private List<String> accessions = new ArrayList<>();

    @Parameter(names = "-f", description = "input accession file")
    private String inputAccessionFile;

    @Parameter(names = "-o", description = "output file name")
    private String outputFile;

    @Parameter(
            names = "-url",
            description = "voldemort server url with default value:tcp://ves-hx-c3:6666")
    private String voldemortUrl = "tcp://ves-hx-c3:6666";

    @Parameter(names = "--help", help = true)
    private boolean help;

    private JCommander jCommander;

    private ClientConfigureImpl() {}

    public static final ClientConfigureImpl fromCommandLine(String[] args) {
        ClientConfigureImpl configurator = new ClientConfigureImpl();
        configurator.jCommander = new JCommander(configurator, args);

        return configurator;
    }

    @Override
    public String getUsage() {
        StringBuilder out = new StringBuilder();
        jCommander.usage(out);

        return out.toString();
    }

    @Override
    public String getInputAccessionfile() {
        return inputAccessionFile;
    }

    @Override
    public List<String> getAccession() {
        return accessions;
    }

    @Override
    public String getOutputFile() {
        return outputFile;
    }

    @Override
    public boolean validate() {
        if (Strings.isNullOrEmpty(inputAccessionFile)
                && ((accessions == null) || accessions.isEmpty())) {
            System.out.println("input file or accessions are not set");
            return false;
        }
        if (Strings.isNullOrEmpty(outputFile)) {
            System.out.println("output file is not set");
            return false;
        }
        return true;
    }

    @Override
    public String getVoldemortUrl() {
        return this.voldemortUrl;
    }
}
