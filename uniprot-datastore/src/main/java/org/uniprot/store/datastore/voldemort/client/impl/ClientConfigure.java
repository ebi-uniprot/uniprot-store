package org.uniprot.store.datastore.voldemort.client.impl;

import java.util.List;

public interface ClientConfigure {
    String getInputAccessionfile();

    List<String> getAccession();

    String getOutputFile();

    boolean validate();

    String getVoldemortUrl();

    String getUsage();
}
