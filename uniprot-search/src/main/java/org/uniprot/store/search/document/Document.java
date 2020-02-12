package org.uniprot.store.search.document;

import java.io.Serializable;

import org.springframework.data.solr.core.mapping.SolrDocument;

/** @author lgonzales */
@SolrDocument
public interface Document extends Serializable {

    String getDocumentId();
}
