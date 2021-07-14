// collect all nodes that are not involved in any many to many transactions
CALL {
    MATCH (a:Address)
    WHERE NOT EXISTS {
        MATCH (a)-[t:Transaction]-()
        WHERE t.num_inputs > 1 AND t.num_outputs > 1
    }
    RETURN collect(a) AS potentialCandidateAddresses
}

// collect all nodes that are in at least one many to many transaction
CALL {
    MATCH (b:Address)
    WHERE EXISTS {
        MATCH (b)-[t:Transaction]-()
        WHERE t.num_inputs > 1 AND t.num_outputs > 1
    }
    RETURN collect(b) AS excludedAddresses
}

// find all addresses in potentialCandidateAddresses that are 
// at most three transactions away from an excluded address
MATCH (candidate:Address)-[t:Transaction*1..3]-(excluded:Address)
WHERE candidate IN potentialCandidateAddresses AND excluded IN excludedAddresses
RETURN DISTINCT candidate;