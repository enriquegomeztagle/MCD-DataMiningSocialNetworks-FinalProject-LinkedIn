CALL
  gds.graph.project.CYPHER(
    'lc',
    'MATCH (u:User) RETURN id(u) AS id',
    '
        MATCH (u1:User)-[:WORKED_AT]->(c:Company)<-[:WORKED_AT]-(u2:User)
        WITH id(u1) AS source, id(u2) AS target, count(c) AS weight
        RETURN source, target, weight
      '
  );
CALL
  gds.louvain.stream(
    'lc',
    {relationshipWeightProperty: 'weight'}
  )
  YIELD nodeId, communityId
WITH gds.util.asNode(nodeId) AS usuario, communityId
RETURN usuario.name AS usuario, communityId AS comunidad
ORDER BY comunidad, usuario;
CALL
  gds.louvain.write(
    'lc',
    {writeProperty: 'communityId', relationshipWeightProperty: 'weight'}
  )
  YIELD nodePropertiesWritten, communityCount;

MATCH (u:User)
WHERE u.communityId IS NOT NULL
WITH collect(DISTINCT u.communityId) AS communities

UNWIND communities AS commId

CALL {
  WITH commId
  MATCH (u:User)
  WHERE u.communityId = commId
  RETURN count(u) AS numUsers
}

CALL {
  WITH commId
  MATCH (u:User)-[:WORKED_AT]->(c:Company)
  WHERE u.communityId = commId
  WITH c.name AS company, count(*) AS freq
  ORDER BY freq DESC
  RETURN collect(company)[0 .. 5] AS top5Empresas
}

RETURN
  commId AS Comunidad,
  numUsers AS `# Usuarios`,
  top5Empresas AS `Top-5 Empresas`
ORDER BY `# Usuarios` DESC
LIMIT 5;
