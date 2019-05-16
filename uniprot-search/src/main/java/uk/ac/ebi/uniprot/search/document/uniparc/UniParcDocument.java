package uk.ac.ebi.uniprot.search.document.uniparc;


import org.apache.solr.client.solrj.beans.Field;
import uk.ac.ebi.uniprot.search.document.Document;

import java.util.HashSet;
import java.util.Set;


public class UniParcDocument implements Document {
    
	@Field("upi")
	public String upi;

//	@Field("xrefs")
//	public List<String> crossReferences = new ArrayList<>();

	@Field("database_type")
	public Set<String> database = new HashSet<>();

	@Field("accession")
	public Set<String> accessions = new HashSet<>();

	@Field("uniprot_accession")
	public Set<String> uniprotAccessions = new HashSet<>();

	@Field("gene")
	public Set<String> geneNames = new HashSet<>();

	@Field("protein")
	public Set<String> proteinNames = new HashSet<>();

	@Field("taxId")
	public Set<Integer> ncbiTaxIds = new HashSet<>();

	@Field("organism_name")
	public Set<String> organismNames = new HashSet<>();

//	@Field("sequence")
//	public String sequence;

	@Field("sequence_checksum")
	public String sequenceChecksum;
	
    @Field("upid")
    public Set<String> upid = new HashSet<>();
    
    @Field("up_component")
    public Set<String> upComponent = new HashSet<>();
    
    @Field("ipr")
    public Set<String> interproId = new HashSet<>();
    
    
    @Field("signature_type")
    public Set<String> signatureType = new HashSet<>();
    
    @Field("signature_id")
    public Set<String> signatureId = new HashSet<>();

    @Field("length")
	public int seqLength;
    
    @Field("avro_binary")
    public byte[] avroEntry;

	@Override
	public String getDocumentId() {
		return upi;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof UniParcDocument)) return false;

		UniParcDocument that = (UniParcDocument) o;

		if (accessions != null ? !accessions.equals(that.accessions) : that.accessions != null) return false;
//		if (crossReferences != null ? !crossReferences.equals(that.crossReferences) : that.crossReferences != null)
//			return false;
		if (database != null ? !database.equals(that.database) : that.database != null) return false;
		if (geneNames != null ? !geneNames.equals(that.geneNames) : that.geneNames != null) return false;
		if (ncbiTaxIds != null ? !ncbiTaxIds.equals(that.ncbiTaxIds) : that.ncbiTaxIds != null) return false;
		if (organismNames != null ? !organismNames.equals(that.organismNames) : that.organismNames != null)
			return false;
		if (proteinNames != null ? !proteinNames.equals(that.proteinNames) : that.proteinNames != null) return false;
	//	if (sequence != null ? !sequence.equals(that.sequence) : that.sequence != null) return false;
		if (sequenceChecksum != null ? !sequenceChecksum.equals(that.sequenceChecksum) : that.sequenceChecksum != null)
			return false;
		if (uniprotAccessions != null ? !uniprotAccessions.equals(that.uniprotAccessions) : that.uniprotAccessions != null)
			return false;
		if (upi != null ? !upi.equals(that.upi) : that.upi != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = upi != null ? upi.hashCode() : 0;
//		result = 31 * result + (crossReferences != null ? crossReferences.hashCode() : 0);
		result = 31 * result + (database != null ? database.hashCode() : 0);
		result = 31 * result + (accessions != null ? accessions.hashCode() : 0);
		result = 31 * result + (uniprotAccessions != null ? uniprotAccessions.hashCode() : 0);
		result = 31 * result + (geneNames != null ? geneNames.hashCode() : 0);
		result = 31 * result + (proteinNames != null ? proteinNames.hashCode() : 0);
		result = 31 * result + (ncbiTaxIds != null ? ncbiTaxIds.hashCode() : 0);
		result = 31 * result + (organismNames != null ? organismNames.hashCode() : 0);
	//	result = 31 * result + (sequence != null ? sequence.hashCode() : 0);
		result = 31 * result + (sequenceChecksum != null ? sequenceChecksum.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "UniParcDocument{" +
				"upi='" + upi + '\'' +
		//		", crossReferences=" + crossReferences +
				", database=" + database +
				", accessions=" + accessions +
				", uniprotAccessions=" + uniprotAccessions +
				", geneNames=" + geneNames +
				", proteinNames=" + proteinNames +
				", ncbiTaxIds=" + ncbiTaxIds +
				", organismNames=" + organismNames +
			//	", sequence='" + sequence + '\'' +
				", sequenceChecksum='" + sequenceChecksum + '\'' +
				'}';
	}
}