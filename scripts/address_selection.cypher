CALL {
    // find all addresses that are not involved in any many-to-many transactions
    CALL {
        MATCH (a:Address)
        WHERE NOT EXISTS {
            MATCH (a)-[t:Transaction]-()
            WHERE t.num_inputs > 1 AND t.num_outputs > 1
        }
        RETURN collect(a) AS potentialCandidateAddresses
    }

    // find all addresses that are in at least one many-to-many transaction
    CALL {
        MATCH (b:Address)
        WHERE EXISTS {
            MATCH (b)-[t:Transaction]-()
            WHERE t.num_inputs > 1 AND t.num_outputs > 1
        }
        RETURN collect(b) AS excludedAddresses
    }

    // find all addresses not involved in any many-to-many transactions that are at most
    // three transactions away from an addresses involved in at least one many-to-many transaction
    MATCH (candidate:Address)-[t:Transaction*1..3]-(excluded:Address)
    WHERE candidate IN potentialCandidateAddresses AND excluded IN excludedAddresses
    RETURN DISTINCT candidate AS c
}
SET c.candidate = true;

MATCH (:Address {candidate: true}) WITH COUNT(*) AS candidateCount
MATCH (addr:Address {candidate: true})
WHERE rand() < 2000.0 / candidateCount
SET addr.selected = true;




