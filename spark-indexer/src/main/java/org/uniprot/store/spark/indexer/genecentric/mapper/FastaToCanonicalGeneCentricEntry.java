package org.uniprot.store.spark.indexer.genecentric.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.uniprot.core.fasta.UniProtKBFasta;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.genecentric.impl.ProteinBuilder;
import org.uniprot.core.parser.fasta.uniprot.UniProtKBFastaParser;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
public class FastaToCanonicalGeneCentricEntry extends FastaToGeneCentricEntry {

    private static final long serialVersionUID = -6177886337091396725L;

    @Override
    Tuple2<String, GeneCentricEntry> parseEntry(
            String proteomeId, Tuple2<LongWritable, Text> fastaTuple) {
        String fastaInput = fastaTuple._2.toString();

        UniProtKBFasta uniProtKBFasta;
        try {
            uniProtKBFasta = UniProtKBFastaParser.fromFasta(fastaInput);
        } catch (Exception e) {
            throw new IndexHPSDocumentsException(
                    "In Proteome: " + proteomeId + ", unable to parse fastaInput: " + fastaInput,
                    e);
        }
        Protein protein = ProteinBuilder.from(uniProtKBFasta).build();
        String accession = protein.getId();

        GeneCentricEntry entry =
                new GeneCentricEntryBuilder()
                        .proteomeId(proteomeId)
                        .canonicalProtein(protein)
                        .build();

        return new Tuple2<>(accession, entry);
    }
}
