CREATE
  /* Users */
  (u1:User {id: 1, name: 'Ana Torres',  title: 'Full-Stack Developer'}),
  (u2:User {id: 2, name: 'Carlos Ruiz', title: 'Data Analyst'}),

  /* Companies */
  (e1:Company {id: 1, name: 'InnovaTech',  industry: 'Technology'}),
  (e2:Company {id: 2, name: 'FinanzaCorp', industry: 'Finance'}),

  /* Universities */
  (uni1:University {id: 1, name: 'National University'}),
  (uni2:University {id: 2, name: 'Polytechnic Institute'}),

  /* Skills */
  (h1:Skill {id: 1, name: 'Python'}),
  (h2:Skill {id: 2, name: 'SQL'}),
  (h3:Skill {id: 3, name: 'Project Management'}),

  /* Posts */
  (p1:Post {id: 1, title: 'Introduction to GraphQL',          date: date('2025-06-04')}),
  (p2:Post {id: 2, title: 'Data Analysis Tips',               date: date('2025-06-04')}),

  /* User-user connections (bidirectional represented with two edges) */
  (u1)-[:KNOWS]->(u2),
  (u2)-[:KNOWS]->(u1),

  /* Work relationships */
  (u1)-[:WORKS_AT]->(e1),
  (u2)-[:WORKS_AT]->(e2),

  /* Education relationships */
  (u1)-[:STUDIED_AT]->(uni1),
  (u2)-[:STUDIED_AT]->(uni2),

  /* Skill relationships */
  (u1)-[:HAS_SKILL]->(h1),
  (u1)-[:HAS_SKILL]->(h3),
  (u2)-[:HAS_SKILL]->(h1),
  (u2)-[:HAS_SKILL]->(h2),

  /* Recommendation */
  (u1)-[:RECOMMENDED {date: date('2025-06-04')}]->(u2),

  /* Posts and interactions */
  (u1)-[:POSTED]->(p1),
  (u2)-[:POSTED]->(p2),
  (u2)-[:INTERACTED_WITH {type: 'LIKE'}]->(p1),
  (u1)-[:INTERACTED_WITH {type: 'COMMENT'}]->(p2);
