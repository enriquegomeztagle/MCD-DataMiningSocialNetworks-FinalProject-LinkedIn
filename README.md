# Final Project – **Data Mining & Social Networks (MCD‑UP)**
**Modelling and analysing LinkedIn with Neo4j, Apache Spark & GDS**

<div align="center">

[Repository](https://github.com/enriquegomeztagle/MCD-DataMiningSocialNetworks-FinalProject-LinkedIn) ·
[Technical paper (LaTeX)](docs/Doc-MCD-DataMiningSocialNetworks-FinalProject-LinkedIn.pdf)

</div>

---

## 1. Short overview

This project builds **a directed graph of the LinkedIn professional network** from the data exports of four students in the **Master’s in Data Science – Universidad Panamericana**.  
Using **Apache Spark** we clean, normalise and load the data straight into **Neo4j**; afterwards we run **Graph Data Science (GDS)** algorithms to explore three classic graph‑analytics tasks:

| Category               | GDS algorithm  | Goal                                                         |
|------------------------|---------------|--------------------------------------------------------------|
| Centrality             | `PageRank`    | Identify companies that concentrate most of the talent flow  |
| Community detection    | `Louvain`     | Cluster users who share main employers                       |
| Node similarity        | `nodeSimilarity` | Connect users with *identical* job histories               |

All the analysis, figures and tables can be found in `docs/doc.tex`.

---

## 2. Repository structure <!-- omit in toc -->

```text
mcd-dataminingsocialnetworks-finalproject-linkedin/
├── README.md              ← ▶ this file
├── Makefile               ← Utility tasks (clean temp, format LaTeX)
├── code/                  ← Scripts, notebooks & Cypher queries
│   ├── clean.cypher       ← Delete the entire graph
│   ├── DDL/               ← Schema rules & constraints
│   ├── dataPrep/          ← CSV cleaning (pandas)
│   ├── insertion/         ← Spark ETL → Neo4j
│   ├── cypher/            ← GDS algorithms (Louvain, PageRank, Similarity)
│   ├── exploration/       ← Exploring CSVs in Spark
│   └── tests/             ← Neo4j connection tests
└── docs/
    ├── doc.tex            ← Full academic report (LaTeX)
    └── images/            ← Generated diagrams & plots
```

---

## 3. Requirements

| Tool / Library            | Suggested version | Notes |
|---------------------------|-------------------|-------|
| **Neo4j Desktop**         | 2.x LTS           | Enable GDS plugin (≥ 2.5) |
| **Apache Spark**          | 3.5.*             | Stand‑alone or `local[*]` |
| Java (JDK)                | 11 or 17          | Spark compatible |
| Python                    | 3.12+             | `pandas`, `pyarrow`, `pyspark`, `neo4j` |
| Spark‑Neo4j connector     | `neo4j-connector-apache-spark_2.12‑5.3.8_for_spark_3.jar` |
| LaTeX (TeX Live / MikTeX) | full installation | To compile the report |

> **Tip:** use *conda* or *venv* to isolate the Python environment.

---

## 4. Data preparation

1. **Download your LinkedIn package**  
   *Settings → Data privacy → Get a copy of your data*  
   Extract and place the relevant CSVs in `data/`.
2. *(Optional)* Run the notebooks / scripts in `code/dataPrep/` to normalise names and dates:

```bash
python code/dataPrep/clean.py
```

---

## 5. Loading data into Neo4j

1. Start Neo4j and create an empty database (edit the name in the scripts).  
2. Drop the **Spark⇄Neo4j connector** jar into `$SPARK_HOME/jars/`.
3. Launch Spark:

```bash
pyspark --packages neo4j-contrib:neo4j-connector-apache-spark_2.12:5.3.8_for_spark_3
```

4. Execute the **ETL pipeline**:

```bash
python code/insertion/insertion.py
```

   - Creates `:User`, `:Company`, `:University`, `:Skill`, `:Language`, `:Certification`, `:Job` nodes  
   - Inserts relationships `WORKED_AT`, `STUDIED_AT`, `HAS_SKILL`, etc.

5. Apply schema constraints & indexes:

```cypher
:source code/DDL/constraints.cypher
```

---

## 6. Graph analysis with GDS

With the graph loaded:

```cypher
:source code/cypher/pagerank.cypher
:source code/cypher/louvain.cypher
:source code/cypher/simmilarity.cypher
```

Results are written as node/relationship properties (`score`, `communityId`, `SAME_COMPANY`).

---

## 7. Build the PDF report

```bash
cd docs
latexmk -pdf doc.tex
```

> You can also run `make format-docs` to auto‑indent the LaTeX file.

---

## 8. Quick queries in Neo4j Bloom / Browser

*Explore a random sample:*

```cypher
MATCH (u:User)-[:WORKED_AT]->(c:Company)
RETURN u,c LIMIT 50;
```

*Filter by Louvain community:*

```cypher
MATCH (u:User {communityId: 205})-[:WORKED_AT]->(c)
RETURN u,c;
```

*Recommend colleagues with identical companies:*

```cypher
MATCH (u:User {name:'Your Name'})-[:SAME_COMPANY]-(peer)
RETURN peer.name, peer.title
ORDER BY peer.score DESC;
```

---

## License

Copyright (c) 2025  
Enrique Ulises Báez Gómez Tagle, Luis Alejandro Guillén Álvarez,  
Joel Vázquez Anaya & José Pablo Ugalde Ortiz

This project is released under the [MIT License](LICENSE).  
When you copy, modify, or redistribute any part of this repository **you must keep the copyright notice and this license
file**, thereby giving due credit to the original authors.

---

## Credits & Authors

| Author                                | Role                                             
|---------------------------------------|--------------------------------------------------
| **Enrique Ulises Báez Gómez Tagle**   | Data & AI Specialist
| **Luis Alejandro Guillén Álvarez**    | AI Engineer  
| **José Pablo Ugalde Ortiz**           | Quality Assurance Engineer   
| **Joel Vázquez Anaya**                | Data Analyst

---

### Happy graph‑mining! 🚀
