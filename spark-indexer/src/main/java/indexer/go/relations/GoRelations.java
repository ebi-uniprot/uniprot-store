package indexer.go.relations;


import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.*;

/**
 * @author lgonzales
 * @since 2019-10-25
 */
@Slf4j
public class GoRelations implements Serializable {

    private static final long serialVersionUID = -1281779255249045417L;
    private LinkedList<GoTerm> goTerms;
    private Map<String, Set<String>> goRelations;

    public GoRelations() {
        goRelations = new HashMap<>();
        goTerms = new LinkedList<>();
    }

    LinkedList<GoTerm> getGoTerms() {
        return goTerms;
    }

    void addTerms(List<GoTerm> terms) {
        goTerms.addAll(terms);
    }

    void addRelations(Map<String, Set<String>> goRelations) {
        this.goRelations.putAll(goRelations);
    }




}
