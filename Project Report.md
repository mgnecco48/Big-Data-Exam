**Project Report: Big Data Processing and Analytics with MongoDB, PySpark and Tableau**  
**Anonymous – PGR108**  
**Big Data and Cloud Computing 2026**

### Introduction
**Domain and Purpose (Motivation):**  
This project operates in the domain of **educational data analytics** using big data and cloud computing principles. It demonstrates a complete, privacy-preserving pipeline for extracting meaningful insights from large volumes of anonymized student learning records. The synthetic dataset (`synthetic_student_learning_dataset_10000.csv`, 10,000 records) serves as a realistic testbed that mirrors real-world educational data, containing both structured attributes (education level, study hours per day, preferred learning method, main learning challenge, motivation level, device used for study) and unstructured free-text opinions on online learning.

The motivation, as articulated in the project notes (`report_notes.md`), is to provide researchers and educators with scalable tools to understand student behaviour in online and hybrid learning environments, supporting evidence-based improvements in teaching strategies, student support, and platform design. This aligns directly with Chapter 1 (*Understanding Big Data*) and the overall course emphasis on processing semi-structured and unstructured data at scale.

**Why this project (Problem):**  
Educational data is messy, high-volume, and contains free-text opinions that traditional relational systems cannot handle efficiently. Chapter 7 (*Big Data Storage Technology*) states: “relational technology is simply not scalable in a manner to support Big Data volumes… businesses can find genuine value in processing semi-structured and unstructured data.” The project solves this by implementing a modern pipeline that processes data without long-term storage, explicitly accounting for real-world messiness (inconsistent text, stopwords, punctuation) as described in `Data_Into_Mongo.ipynb` and `report_notes.md`.

### Project Objective/Goal
The primary objective is to deliver a complete, reusable **Student Data Analytics Framework** — a set of guidelines, tools, and best practices documented in `report_notes.md` — that enables researchers to make sense of anonymized student data. The framework covers the full big data lifecycle and provides the methodological foundation to answer targeted research questions such as:  
- How can researchers identify relationships between study hours and motivation levels?  
- How can researchers find correlations between the device students use and their motivation levels?  
- How does education level affect students’ sentiment toward online learning?

### Why These Technologies (and Why Better Than Alternatives)
The stack was chosen after evaluating alternatives and is fully implemented in the provided files:  
- **MongoDB** (NoSQL document database – Topic 2 and Chapter 5): Used for flexible storage of respondent-level documents with embedded token arrays (`processed_dataset` collection), word-frequency summaries (`word_count`), and long-format token bridge (`opinion_token_bridge`). This design is explicitly described in `report_notes.md` under “MongoDB Collection Design” and follows BASE properties with sharding/replication support (Chapter 5). Relational databases were rejected due to scalability limits (Chapter 7).  
- **PySpark** (Topic 2 and Chapter 6): Handles parallel/distributed processing, text cleaning, sentiment analysis (VADER), token extraction, and aggregation. The complete implementation is in `Data_Into_Mongo.ipynb` (including explode/split, lowercasing, punctuation removal, NLTK stopwords, custom stopword filtering, and grouping back to respondent level).  
- **Tableau** (Topic 5 and Chapter 8): Provides visual analysis through the workbook `Vizualization.twb` / `Vizualization.twbx`, connecting via MongoDB BI Connector (noted as legacy but used for demonstration in `report_notes.md`).  

**What can you do now that wasn’t possible before?**  
Researchers can now ingest, clean, store temporarily in a document database, perform distributed text processing and aggregation, and create interactive dashboards — all while maintaining privacy (process and delete) and handling real-world data messiness.

**Limitations and Assumptions**  
- Data is fully synthetic and anonymized (generated per `Specific_Prompt_Template_with_User_Guideline.docx` and `data_dictionary.csv`).  
- Local Docker-based environment (scalable to cloud clusters – Topic 3).  
- Batch processing only (Chapter 6).  
- The framework explicitly accounts for messiness through the steps in `Data_Into_Mongo.ipynb`.

**Project Management (Group Contributions)**  
- Member A: Data generation and MongoDB ingestion (`Data_Into_Mongo.ipynb`).  
- Member B: PySpark processing, text cleaning, sentiment analysis, and collection design.  
- Member C: Tableau workbook (`Vizualization.twb`), framework documentation (`report_notes.md`), and final report.  
All members collaborated on integration, testing, and GitHub version control.

### Background and Literature Review
The framework is directly built from the provided files and course materials:  
- Big Data lifecycle and characteristics (Chapter 1).  
- NoSQL storage concepts, clusters, sharding/replication, CAP/BASE (Chapter 5).  
- Parallel/distributed processing and batch workloads (Chapter 6).  
- Storage technology evolution and NoSQL advantages (Chapter 7).  
- Analysis techniques and visual analysis (Chapter 8).  
- Big Data BI vs traditional ETL/OLAP (Chapter 4).  
- Practical MongoDB + PySpark integration (Using MongoDB and pyspark Via Docker.pptx).  
- Tableau for visual big data analytics (Topic 5).  

**The Student Data Analytics Framework** (detailed in `report_notes.md`):  
1. **Privacy-first guidelines**: Anonymized data only; process and delete after analysis.  
2. **Tool stack**: Docker, MongoDB (3 collections), PySpark (text processing pipeline), Tableau.  
3. **Standardised methodology**: CSV → PySpark cleaning/tokenization/sentiment → MongoDB → Tableau.  
4. **Reproducibility**: GitHub repository with notebooks and IaC.  

This framework accounts for “messiness of real world data” through the explicit steps in `Data_Into_Mongo.ipynb` (lowercasing, punctuation removal, NLTK stopwords + domain-specific stopwords, collect_set for tokens).

### Research Methodology
**Dataset:** `synthetic_student_learning_dataset_10000.csv` – exactly 10,000 records matching `data_dictionary.csv`.

**Big Data Life Cycle & Architecture Flow (from `report_notes.md` and notebooks):**  
```
Raw CSV (synthetic_student_learning_dataset_10000.csv)  
          ↓ (PySpark – Data_Into_Mongo.ipynb)  
Text Processing & Sentiment (split/explode, lower, regexp_replace, NLTK stopwords, VADER)  
          ↓  
MongoDB (3 collections: processed_dataset with tokens, word_count, opinion_token_bridge)  
          ↓ (Mongo Atlas cloud. Direct Tableau connection)  
Analysis & Aggregation  
          ↓  
Tableau (Vizualization.twb / .twbx) → Interactive Dashboards  
          ↓  
Privacy-compliant deletion of raw/processed data
```

**Practical Implementation** (Docker from provided PPT):  
```bash
docker run -d -p 27017:27017 --name mongodb mongo
docker run -p 8888:8888 jupyter/pyspark-notebook
```
Full ingestion, cleaning, sentiment (VADER), and insertion code is in `Data_Into_Mongo.ipynb`. The three-collection design and bridge collection for Tableau filtering are described in `report_notes.md`.

### Results and Discussion
The framework was fully implemented and validated using the files provided. It enables researchers to answer the three research questions through a repeatable, scalable methodology:

**RQ1: How can researchers identify relationships between study hours and motivation levels?**  
PySpark SQL aggregations on the `processed_dataset` collection (or exported results) combined with Tableau visualisations allow computation of averages, distributions, and correlations.

**RQ2: How can researchers find correlations between the device students use and their motivation levels?**  
Cross-tabulation queries in Spark SQL and Tableau heatmaps/stacked bars provide the pathway to explore proportional relationships.

**RQ3: How does education level affect sentiment toward online learning?**  
Sentiment scores and labels (added via VADER in `Data_Into_Mongo.ipynb`) grouped by education level, visualised in Tableau, enable analysis of opinion polarity.

**Additional Framework Capabilities:**  
- Token-based filtering via the `opinion_token_bridge` collection (designed specifically for Tableau – `report_notes.md`).  
- Word-frequency summaries in the `word_count` collection for quick visualisations.  
- Interactive Tableau dashboards (`Vizualization.twb`) with filters on all dimensions.  
- Full reproducibility via GitHub (notebooks, Docker setup, and IaC scripts).

### Conclusions
This project has successfully delivered a practical, privacy-preserving **Student Data Analytics Framework** using the exact pipeline documented in `report_notes.md`, `Data_Into_Mongo.ipynb`, and `Vizualization.twb`. The framework provides researchers with the tools and guidelines needed to answer complex questions about student learning behaviour while respecting privacy and handling real-world data messiness.

The end-to-end solution was validated on the synthetic test dataset and is directly extensible to any dataset in the Home-Exam-projects list.pdf. All course concepts were applied in practice.

**Particular problems encountered:**  
- MongoSpark connector configuration in Docker (resolved via URI).  
- Text cleaning for domain-specific stopwords and punctuation (handled in `Data_Into_Mongo.ipynb`).  
- Tableau connection to remote MongoDB (BI Connector noted as legacy in `report_notes.md`).

**Key learnings:** Modern big data tools (MongoDB for storage, PySpark for processing, Tableau for visualisation) enable scalable insight generation that was previously impossible with traditional systems (Chapter 7). The framework is fully reproducible via GitHub.

**References** (all from provided files):  
- `report_notes.md`, `Data_Into_Mongo.ipynb`, `Vizualization.twb` / `.twbx`, `process.ipynb`, Chapters 1–8 PDFs, Topics 2 & 5, Using MongoDB and pyspark Via Docker.pptx, `data_dictionary.csv`, `Specific_Prompt_Template_with_User_Guideline.docx`, Home-Exam-projects list.pdf.  
Full code, notebooks, and framework documentation are available in the private GitHub repository.

**Appendix:**  
- Complete PySpark notebook (`Data_Into_Mongo.ipynb`)  
- Tableau workbook (`Vizualization.twb`)  
- Framework guidelines (`report_notes.md`)  
- Docker and GitHub setup instructions
