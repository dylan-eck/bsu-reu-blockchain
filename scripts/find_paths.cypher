// create graph project for use with path finding algorithm
CALL gds.graph.create(
    'selectionGraph',
    'Address',
    'Transaction'
);

// get the length of the shortest path between all desired pairs of nodes 
MATCH (a:Address {candidate: true}), (b:Address {candidate: true})
WITH a AS source, b AS target
UNWIND(source) AS sources
UNWIND(target) AS targets

CALL gds.shortestPath.dijkstra.stream('selectionGraph', {
    sourceNode: sources,
    targetNode: targets
})
YIELD totalCost

RETURN sources.address, targets.address, totalCost
ORDER BY totalCost DESC;