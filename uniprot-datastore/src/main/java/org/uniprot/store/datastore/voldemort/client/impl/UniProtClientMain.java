package org.uniprot.store.datastore.voldemort.client.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.flatfile.writer.impl.UniProtFlatfileWriter;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.client.ClientFactory;
import org.uniprot.store.datastore.voldemort.client.UniProtClient;

import joptsimple.internal.Strings;

public class UniProtClientMain {
    private static final String DEFAULT_VOLDEMORT_URL = "tcp://ves-hx-c3.ebi.ac.uk:6666";
    private static final int FLUSH_SIZE = 100;
    private static final Logger logger = LoggerFactory.getLogger(UniProtClientMain.class);

    public static void main(String[] args) throws Exception {

        ClientConfigure configure = ClientConfigureImpl.fromCommandLine(args);
        if (!configure.validate()) {
            logger.error(configure.getUsage());
            System.exit(1);
        }

        String voldemortUrl = DEFAULT_VOLDEMORT_URL;
        if (!Strings.isNullOrEmpty(configure.getVoldemortUrl())) {
            voldemortUrl = configure.getVoldemortUrl();
        }

        try (ClientFactory factory = new DefaultClientFactory(voldemortUrl, 20);
                UniProtClient client = factory.createUniProtClient()) {
            UniProtClientMain main = new UniProtClientMain();
            main.execute(configure, client);
        }
    }

    private void execute(ClientConfigure configure, UniProtClient client) {
        List<String> accessions = configure.getAccession();
        if (accessions == null) {
            accessions = new ArrayList<>();
        }
        if (!Strings.isNullOrEmpty(configure.getInputAccessionfile())) {
            accessions.addAll(getAccessionFromFile(configure.getInputAccessionfile()));
        }
        try (Writer writer = new BufferedWriter(new FileWriter(configure.getOutputFile()))) {
            List<String> accList = new ArrayList<>();
            for (String accession : accessions) {

                accList.add(accession);
                if (accList.size() >= FLUSH_SIZE) {
                    write(client, writer, accList);
                    accList.clear();
                }
                Thread.sleep(61L);
            }
            if (!accList.isEmpty()) {
                write(client, writer, accList);
                accList.clear();
            }
        } catch (Exception e) {
            logger.error("Error while writing the file", e);
        }
    }

    private void write(UniProtClient client, Writer writer, List<String> accessions)
            throws Exception {
        List<UniProtKBEntry> entries = client.getEntries(accessions);
        for (UniProtKBEntry entry : entries) {
            try {
                writer.write(UniProtFlatfileWriter.write(entry, true, false) + "\n");
                logger.info("Fetch entry: " + entry.getPrimaryAccession().getValue() + " done.");
            } catch (Exception e) {
                logger.error(
                        "convert entry to flatfile:"
                                + entry.getPrimaryAccession().getValue()
                                + " failed.");
            }
        }
        writer.flush();
    }

    private List<String> getAccessionFromFile(String accessionfile) {
        List<String> accessions = new ArrayList<>();
        try (BufferedReader inputStream = new BufferedReader(new FileReader(accessionfile))) {
            String line;
            while ((line = inputStream.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("--") || line.isEmpty()) continue;
                accessions.add(line);
            }
        } catch (Exception e) {
            logger.error("executeByList", e);
        }
        return accessions;
    }
}
