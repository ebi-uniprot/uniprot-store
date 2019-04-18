package uk.ac.ebi.uniprot.datastore.voldemort.uniprot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortRemoteJsonBinaryStore;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.json.parser.uniprot.UniprotJsonConfig;

/**
 * This class contains methods to save Uniprot voldemort entry remotely.
 *
 * Created 05/10/2017
 *
 * @author lgonzales
 */
public class VoldemortRemoteUniprotEntryStore extends VoldemortRemoteJsonBinaryStore<UniProtEntry> {

    public static final String UNIPROT_VOLDEMORT_URL = "uniprotVoldemortUrl";
    public static final String UNIPROT_VOLDEMORT_STORE_NAME = "uniprotVoldemortStoreName";

    @Inject
    public VoldemortRemoteUniprotEntryStore(
            @Named(UNIPROT_VOLDEMORT_STORE_NAME) String storeName,
            @Named(UNIPROT_VOLDEMORT_URL) String voldemortUrl) {
        super(storeName, voldemortUrl);
    }

    public VoldemortRemoteUniprotEntryStore(int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(UniProtEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniprotJsonConfig.getInstance().getObjectMapper();
    }

    @Override
    public Class<UniProtEntry> getEntryClass() {
        return UniProtEntry.class;
    }


/*    public static void main(String[] args) throws IOException, UniProtParserException {

        VoldemortRemoteUniprotEntryStore voldemortEntryStore =
                new VoldemortRemoteUniprotEntryStore("avro-uniprot", "tcp://ves-oy-ea.ebi.ac.uk:7666");

        Optional<EntryObject> i3USS6 = voldemortEntryStore.getEntry("P01106");
        System.out.println(i3USS6);
    }*/
}
