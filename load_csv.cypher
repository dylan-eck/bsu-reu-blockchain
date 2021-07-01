LOAD CSV WITH HEADERS FROM 'file:///test.csv' AS row
WITH row WHERE row.transaction_hash IS NOT NULL

FOREACH(i IN SPLIT(row.input_addresses, ':') |
    FOREACH(j IN SPLIT(row.output_addresses, ':') |
        MERGE (a:Address {address: i})
        MERGE (b:Address {address: j})
        CREATE (a)-[:Transaction {hash: row.transaction_hash}]->(b)
    )
);