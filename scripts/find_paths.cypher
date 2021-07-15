// create graph project for use with path finding algorithm
CALL gds.graph.create(
    'myGraph',
    'Location',
    'ROAD',
    {
        relationshipProperties: 'cost'
    }
);

// get the length of the shortest path between all desired pairs of nodes 
MATCH (a:Location), (b:Location)
WITH a AS source, b AS target
UNWIND(source) AS sources
UNWIND(target) AS targets

CALL gds.shortestPath.dijkstra.stream('myGraph', {
    sourceNode: sources,
    targetNode: targets,
    relationshipWeightProperty: 'cost'
})
YIELD totalCost
RETURN sources, targets, totalCost;