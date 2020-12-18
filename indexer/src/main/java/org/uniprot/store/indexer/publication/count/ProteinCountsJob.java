package org.uniprot.store.indexer.publication.count;

/**
 * Created 17/12/2020
 *
 * @author Edd
 */
public class ProteinCountsJob {
    /* here's an outline of our current way of thinking, to inject the protein counts for each PublicationDocument

    1. index all PublicationDocuments to publication collection (community/computational/uniprotkb)
    2. Run this job to update the protein count for each document
        document fields are: <id, pubmed, accession, type, protein_count_by_type, obj>
        e.g.,
        <ID1, 1234, P12345, COMMUNITY, 3, OBJ1>
        <ID2, 1234, P12346, COMMUNITY, 3, OBJ2>
        <ID3, 1234, P12347, COMMUNITY, 3, OBJ3>
        <ID4, 1235, P12347, COMMUNITY, 1, OBJ4>
        ...
        for pubmed 1234, the protein_count_by_type 3 means that 1234 is mapped to 3 proteins (accessions) of the
        COMMUNITY publications type.

    2.a.)
      Reader can stream the accession IDs from the literature collection

    2.b.) (Maybe we don't need to do a facet query for each accession, but just compute the counts over the docs per type
           for each accession)
      Processor processes each $accession
      - doclist <- solr query to fetch all documents with $accession
      - Map<type, list<doc>> type2doclist <- group $doclist by type
      - for each type in $type2doclist
        - count <- computeUniqueAccessionsIn($type2doclist.get($type))
        - for each doc in $type2doclist.get($type)
          - update doc and set protein_count_by_type = $count
          # THIS NEXT BIT MIGHT TAKE PLACE IN THE WRITER, NOT PROCESSOR
          - delete $doc from solr (to remove the old one, which didn't have count)
          - add $doc to solr (to add the updated one, which has count)


       Note: Please comment if you think this will work.
       Also, I *think* this should make the REST side of things work too, but think
       we should think it through carefully too, to make sure we're not missing anything
    */
}
