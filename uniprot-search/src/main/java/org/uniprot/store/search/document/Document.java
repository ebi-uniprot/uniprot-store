package org.uniprot.store.search.document;

import org.springframework.data.solr.core.mapping.SolrDocument;

/** @author lgonzales */
@SolrDocument
public interface Document {

    String getDocumentId();
}
