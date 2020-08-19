package org.uniprot.store.config.uniparc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.uniparc.UniParcDatabase;

/**
 *
 * @author jluo
 * @date: 19-Aug-2020
 *
*/

public class UniParcConfigUtilTest {
	
    @ParameterizedTest
    @MethodSource("setData")
	void uniparcDatabaseToSearchField(UniParcDatabase db, String name, String value ) {
		Map.Entry<String, String> entry =  UniParcConfigUtil.uniparcDatabaseToSearchField(db);
		Assertions.assertEquals(name, entry.getKey());
		Assertions.assertEquals(value, entry.getValue());
	}
   
	
	private static List<Arguments> setData() {
		List<Arguments> result= new ArrayList<>();
		for(UniParcDatabase db: UniParcDatabase.values()) {
			if(db ==UniParcDatabase.EMBL ) {
				result.add(Arguments.of(db, "EMBL CDS", "embl-cds"));
			}else if ((db ==UniParcDatabase.SWISSPROT)|| (db ==UniParcDatabase.TREMBL)) {
				result.add(Arguments.of(db, "UniProtKB", "Uniprot"));
			}else if(db ==UniParcDatabase.SWISSPROT_VARSPLIC ) {
				result.add(Arguments.of(db, "UniProtKB/Swiss-Prot isoforms", "isoforms"));
			}else {
				result.add(Arguments.of(db, db.getDisplayName(), db.getDisplayName()));
			}
		}
		return result;
    }
}

