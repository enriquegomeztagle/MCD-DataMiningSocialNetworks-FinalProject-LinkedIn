CALL
  gds.graph.project(
    'userCompanyGraph',
    ['User', 'Company'],
    {WORKED_AT: {orientation: 'UNDIRECTED'}}
  );

CALL
  gds.nodeSimilarity.write(
    'userCompanyGraph',
    {
      similarityCutoff: 1.0,
      writeRelationshipType: 'SAME_COMPANY',
      writeProperty: 'score'
    }
  )
  YIELD relationshipsWritten;

MATCH ()-[r:SAME_COMPANY]->()
RETURN count(r) AS pares_colegas;

MATCH (u)-[:SAME_COMPANY]-()
RETURN count(DISTINCT u) AS usuarios_con_colegas;

MATCH ()-[r:SAME_COMPANY]-()
RETURN 2.0 * count(r) / count(DISTINCT startNode(r)) AS grado_promedio;

MATCH (c:Company)<-[:WORKED_AT]-(u:User)
WITH c, count(u) AS empleados
WHERE empleados > 1
RETURN
  c.name AS empresa,
  empleados AS n_empleados,
  empleados * (empleados - 1) / 2 AS pares_colegas
ORDER BY pares_colegas DESC
LIMIT 10;
