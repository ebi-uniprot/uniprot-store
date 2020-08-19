package org.uniprot.store.config.uniparc;

import java.util.AbstractMap;
import java.util.Map;

import org.uniprot.core.uniparc.UniParcDatabase;

/**
 *
 * @author jluo
 * @date: 19-Aug-2020
 *
 */

public class UniParcConfigUtil {
	public static Map.Entry<String, String> uniparcDatabaseToSearchField(UniParcDatabase database) {
		String name = database.getDisplayName();
		String value = database.getDisplayName();
		switch (database) {
		case SWISSPROT:
		case TREMBL:
			name = "UniProtKB";
			value = "Uniprot";
			break;
		case SWISSPROT_VARSPLIC:
			name = "UniProtKB/Swiss-Prot isoforms";
			value = "isoforms";
			break;
		case EMBL:
			name = "EMBL CDS";
			value = "embl-cds";
			break;
		default:
			name = database.getDisplayName();
			value = database.getDisplayName();
			break;
		}
		return new AbstractMap.SimpleEntry<>(name, value);
	}
}
