// ───── UNIQUE NODES ─────
CREATE CONSTRAINT user_id IF NOT EXISTS
FOR (u:User)
REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT company_id IF NOT EXISTS
FOR (c:Company)
REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT university_id IF NOT EXISTS
FOR (u:University)
REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT skill_name IF NOT EXISTS
FOR (s:Skill)
REQUIRE s.name IS UNIQUE;
CREATE CONSTRAINT language_name IF NOT EXISTS
FOR (l:Language)
REQUIRE l.name IS UNIQUE;
CREATE CONSTRAINT cert_name_auth IF NOT EXISTS
FOR (c:Certification)
REQUIRE (c.name, c.authority) IS NODE KEY;
CREATE CONSTRAINT job_id IF NOT EXISTS
FOR (j:Job)
REQUIRE j.id IS UNIQUE;
CREATE CONSTRAINT post_id IF NOT EXISTS
FOR (p:Post)
REQUIRE p.id IS UNIQUE;

// ───── INDEXES ─────
CREATE INDEX user_name IF NOT EXISTS
FOR (u:User)
ON (u.name);
CREATE INDEX company_name IF NOT EXISTS
FOR (c:Company)
ON (c.name);
CREATE INDEX skill_lower IF NOT EXISTS
FOR (s:Skill)
ON (s.nameLower);
CREATE INDEX cert_start IF NOT EXISTS
FOR (c:Certification)
ON (c.start);
CREATE INDEX cert_end IF NOT EXISTS
FOR (c:Certification)
ON (c.end);

// ───── LABELS ─────
// :User, :Company, :University, :Skill, :Language, :Certification, :Job, :Post, :Share
// RELATIONSHIP PROPERTIES:
//   :STUDIED_AT {degree, from, to}
//   :WORKED_AT   {title, location, from, to}
//   :SPEAKS      {proficiency}
//   :HAS_CERT    (no extra props on rel; dates live on Certification node)
//   :SAVED_JOB   {savedDate}
//   :CONNECTED   {since}
//   :FOLLOWS     {since}
