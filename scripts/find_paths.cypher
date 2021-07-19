CALL gds.graph.create(
    'selectionGraph',
    'Address',
    'Transaction'
);

MATCH (a:Address {selected: true}), (b:Address {selected: true})
UNWIND(a) AS source
UNWIND(b) AS target

WITH source, target
WHERE source <> target
CALL gds.shortestPath.dijkstra.stream('selectionGraph', {
    sourceNode: source,
    targetNode: target
})
YIELD totalCost

RETURN source.address, target.address, totalCost;



