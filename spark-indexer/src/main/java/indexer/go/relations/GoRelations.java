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

    public void addTerms(List<GoTerm> terms) {
        goTerms.addAll(terms);
    }

    public void addRelations(Map<String, Set<String>> goRelations) {
        this.goRelations.putAll(goRelations);
    }

    public Set<GoTerm> getAncestors(String goTermId) {
        GoTerm term = getGoTermById(goTermId);
        Set<GoTerm> visited = new TreeSet<>();
        Queue<String> queue = new LinkedList<>();
        queue.add(term.getId());
        visited.add(term);
        while (!queue.isEmpty()) {
            String vertex = queue.poll();
            for (String relatedGoId : goRelations.getOrDefault(vertex, Collections.emptySet())) {
                GoTerm relatedGoTerm = getGoTermById(relatedGoId);
                if (!visited.contains(relatedGoTerm)) {
                    visited.add(relatedGoTerm);
                    queue.add(relatedGoId);
                }
            }
        }
        return visited;
    }

    private GoTerm getGoTermById(String goTermId) {
        GoTerm goTerm = new GoTermFileReader.GoTermImpl(goTermId, null);
        if (goTerms.contains(goTerm)) {
            return goTerms.get(goTerms.indexOf(goTerm));
        } else {
            log.warn("GO TERM NOT FOUND FOR GO RELATION ID: " + goTermId);
            return goTerm;
        }
    }


}
