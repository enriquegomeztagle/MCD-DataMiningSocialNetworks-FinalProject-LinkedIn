// — Nodes —
CREATE
  // Users
  (u:User
    {
      id: 1,
      name: 'Ana Torres',
      title: 'Full-Stack Developer',
      headline: '…',
      summary: '…',
      industry: 'Technology',
      geoLocation: 'Madrid, España',
      birthDate: date('1990-05-12')
    }),

  // Companies
  (c1:Company {id: 1, name: 'InnovaTech', industry: 'Technology'}),
  (c2:Company {id: 2, name: 'FinanzaCorp', industry: 'Finance'}),

  // Universities
  (uni1:University {id: 1, name: 'National University'}),
  (uni2:University {id: 2, name: 'Polytechnic Institute'}),

  // Skills
  (s1:Skill {id: 1, name: 'Python', nameLower: 'python'}),
  (s2:Skill {id: 2, name: 'SQL', nameLower: 'sql'}),

  // Languages
  (l1:Language {name: 'Spanish'}),
  (l2:Language {name: 'English'}),

  // Certifications (with expiration date in the node)
  (cert1:Certification
    {
      name: 'Responsible AI: A Cloud Practitioner',
      authority: 'Google',
      start: date('2024-01-01'),
      end: date('2025-01-01'),
      license: '7876257'
    }),

  // Posts
  (p1:Post
    {id: 1, title: 'Introduction to GraphQL', postedDate: date('2025-06-04')}),

  // Shares
  (sh1:Share
    {
      id: 1,
      text: '🎓 Just completed my course!',
      sharedAt: datetime('2025-04-13T04:53:32')
    }),

  // Job offers
  (j1:Job {id: 1, title: 'Associate Consultant', company: 'Oracle'}),

  // — RELATIONS —
  // Studies
  (u)-
    [:STUDIED_AT {
        degree: 'Bachelor of Science - BS',
        from: date('2020-08-01'),
        to: date('2024-12-01')
      }]->
  (uni1),

  // Work history
  (u)-
    [:WORKED_AT {
        title: 'Data Engineer',
        from: date('2022-11-01'),
        to: date('2024-09-01'),
        location: 'Ciudad de México'
      }]->
  (c1),

  // Skills
  (u)-[:HAS_SKILL]->(s1),
  (u)-[:HAS_SKILL]->(s2),

  // Languages
  (u)-[:SPEAKS {proficiency: 'Native or bilingual'}]->(l1),
  (u)-[:SPEAKS {proficiency: 'Full professional'}]->(l2),

  // Certifications
  (u)-[:HAS_CERT]->(cert1),

  // Posts
  (u)-[:POSTED {on: date('2025-06-04')}]->(p1),

  // Interactions
  (u)-[:INTERACTED_WITH {type: 'LIKE', at: datetime('2025-06-05T10:15:00')}]->
  (p1),

  // Saved jobs
  (u)-[:SAVED_JOB {savedDate: datetime('2025-05-29T17:37:00')}]->(j1),
