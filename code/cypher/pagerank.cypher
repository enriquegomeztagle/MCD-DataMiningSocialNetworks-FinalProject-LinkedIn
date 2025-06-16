CALL {
  CALL
    gds.pageRank.stream(
      'pg',
      {
        relationshipWeightProperty: 'weight',
        dampingFactor: 0.85,
        maxIterations: 20,
        tolerance: 1e-6
      }
    )
    YIELD nodeId, score
  RETURN nodeId, score
  ORDER BY score DESC
  LIMIT 10
}

MATCH (n)
WHERE id(n) = nodeId
OPTIONAL MATCH (n)-[r]->(m)
RETURN n AS nodo, r AS relacion, m AS nodoRelacionado, score
ORDER BY score DESC
