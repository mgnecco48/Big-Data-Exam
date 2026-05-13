# Report Notes

## Role of MongoDB in the Pipeline

MongoDB is used to store the dataset after it has been processed with PySpark. Instead of storing only a direct copy of the original CSV file, each respondent is stored as a document in the `processed_dataset` collection.

The original columns, such as education level, study hours, learning method, challenge, motivation level, and study device, are kept. In addition, the cleaned opinion tokens from the free-text field are stored as a list inside the same document. This lets us keep the structured data and the processed text data together.

The `word_count` collection stores the word count result, with one document per relevant word and its count/percentage across opinions. This collection can be used directly for word-frequency visualizations, while `processed_dataset` can be used for respondent-level filtering and Tableau dashboards. A third collection, `opinion_token_bridge`, stores the long-format relationship between each respondent and each cleaned token in their opinion. This makes token-based filtering easier in Tableau because each respondent-token pair is represented as a separate row, without repeating the full opinion text in the bridge collection.

For this specific project, the dataset is static. Since the CSV file does not change over time, connecting Tableau to MongoDB is not strictly necessary from a technical point of view. A simpler workflow could process the CSV with PySpark, export Tableau-ready CSV files, and connect Tableau directly to those outputs. The MongoDB step is still useful because it demonstrates how the same pipeline could support a more realistic data system where responses are not fixed in advance.

MongoDB becomes more valuable if the project grows into a monitoring system for student sentiment about a specific online learning platform. In that scenario, new responses could be inserted as students submit them, PySpark or another processing layer could add sentiment scores and cleaned tokens, and Tableau could visualize updated trends from a central database instead of relying on manually regenerated files. MongoDB is well suited to that kind of workflow because it can store structured survey attributes together with semi-structured analysis outputs such as token arrays, sentiment labels, topic labels, or platform metadata.

This also makes the pipeline easier to extend. If future analysis adds new text features or metadata fields, MongoDB documents can include those fields without redesigning a rigid table structure first. At the same time, the bridge collection gives Tableau a more relational view of the token data, which is useful because Tableau works best when filter values are exposed as rows or columns rather than nested arrays inside documents.

The project uses a remote MongoDB cluster instead of only a local MongoDB instance. For this static dataset, a local database would be enough, but a remote cluster better reflects how a larger big data pipeline would share processed outputs across tools and users. It also makes the later Tableau connection more realistic, because the current recommended practice is to connect BI tools to a hosted MongoDB deployment rather than relying on a database running only on one local machine.

## MongoDB Collection Design

The final MongoDB design uses three collections:

- `processed_dataset`: one document per respondent, containing the original dataset fields plus a `tokens` list created from the free-text opinion field.
- `word_count`: one document per relevant word, containing the word, the number of opinions containing that word, and the percentage of opinions in which it appears.
- `opinion_token_bridge`: one document per respondent-token pair, containing the `respondent_id` and cleaned token.

This design keeps the respondent-level data in one main collection, stores aggregate word-frequency results separately, and exposes token membership in a long format for Tableau filtering. The `word_count` collection is separate because it represents summary results rather than individual respondents. The `opinion_token_bridge` collection is separate because it represents a many-to-many relationship: one opinion can contain many relevant tokens, and the same token can appear in many opinions.

## Use of Original Respondent ID

The original `respondent_id` is used as the MongoDB `_id` field in the `processed_dataset` collection. This keeps the original unique identifier from the dataset and avoids creating unrelated MongoDB IDs. It also makes it easier to connect a MongoDB document back to the original CSV row.

For the `word_count` collection, MongoDB can generate the `_id` automatically. The word itself is kept as a normal `word` field, which is simpler to read and easier to use when exporting or visualizing the collection.

## Text Processing Design

The free-text `online_learning_opinion` field is cleaned in PySpark before loading the data into MongoDB. The processing steps are:

- Splitting each opinion into individual words with `split` and `explode`.
- Keeping `respondent_id` so each cleaned word can be connected back to the original opinion.
- Lowercasing words to avoid counting the same word separately because of capitalization.
- Removing common punctuation marks while preserving hyphenated terms such as `theory-based` and `self-discipline`.
- Removing English stopwords with NLTK.
- Removing dataset-specific frequent words such as `online` and `learning`, because they appear often due to the topic of the dataset and do not add much analytical value.

After cleaning, the words are grouped back to respondent level using a token list. This creates a useful MongoDB structure where each opinion document contains both the original text and the cleaned words extracted from it.

## Word Percentage Calculation

The word-frequency table uses `countDistinct("respondent_id")` instead of a simple word count. This measures how many opinions contain each word, instead of only counting how many times the word appears in total.

This is safer because a word may appear more than once in the same opinion. Counting unique respondent IDs avoids making repeated words inside one response look more important than they are.

## Scalability Note

The notebook currently converts Spark DataFrames to local Python dictionaries using `.collect()` before inserting them into MongoDB with `insert_many()`. For this synthetic dataset of 10,000 rows, this is acceptable because the dataset is small enough to fit in local memory.

For larger datasets, `.collect()` would not work as well because it moves all Spark data into the main Python process. A better design for larger data would use one of these approaches:

- The MongoDB Spark Connector to write Spark DataFrames directly into MongoDB.
- `foreachPartition()` to insert records partition by partition instead of collecting all records at once.
- Batch writes per Spark partition to reduce memory use and avoid overloading the main process.

This limitation should be mentioned in the report as an implementation tradeoff: the current approach is simple and good enough for this dataset size, while a connector-based or partition-based write strategy would be better for truly large-scale data.

## Tableau Design Decision

The project keeps most visual exploration in Tableau, especially category distributions, heatmaps, and dashboard filtering. PySpark is used for the preprocessing steps that Tableau is less suited for, especially text splitting, stopword removal, cleaned token creation, and word counting.

This gives each technology a clear role:

- PySpark performs preprocessing and transformations.
- MongoDB stores the enriched documents and word count results.
- Tableau visualizes the structured fields and processed outputs.

## Connecting MongoDB to Tableau

To visualize MongoDB data in Tableau, an extra connection layer is needed because Tableau works most naturally with tabular data sources. One option is the MongoDB Connector for BI, which runs a service called `mongosqld`. This service acts as a bridge between MongoDB and SQL-based BI tools. Tableau connects to `mongosqld` through ODBC, while `mongosqld` connects to the MongoDB database and exposes the document collections in a table-like form.

Because the data is stored in a remote MongoDB cluster, Tableau can connect to the shared database environment instead of depending on local files or a local MongoDB instance. This is closer to a production BI workflow, where dashboards usually read from hosted databases that can be accessed consistently across machines.

The general process is:

- Install and run MongoDB.
- Load the processed collections into MongoDB.
- Install the MongoDB BI Connector and its ODBC driver.
- Start `mongosqld` and point it to the MongoDB server, usually with a MongoDB URI such as `mongodb://localhost:27017`.
- Configure Tableau to connect through ODBC to the host and port where `mongosqld` is running, often `127.0.0.1:3307` by default.
- Select the MongoDB database and collections exposed by the connector.

This setup is useful because it allows Tableau to work with MongoDB collections without manually exporting everything to CSV first. However, it also adds complexity. The user must have MongoDB, the BI Connector, the ODBC driver, Tableau, and the correct local connection settings installed and working at the same time.

There is also an important limitation: the MongoDB BI Connector and `mongosqld` are legacy tools and are deprecated, with end-of-life planned for September 2026. This means the approach can still be useful for a class project or an existing setup, but it is not the best long-term choice for a new production system. MongoDB recommends newer SQL interface options for future projects.

This affects how easy the project is to reuse. The pipeline is not fully "plug and play" for another user because several separate tools must be installed and configured correctly before Tableau can connect to MongoDB. For a more portable version of the project, it may be better to export Tableau-ready files from PySpark or MongoDB, such as CSV or Parquet outputs, or to document the required setup steps very clearly in a README file.

## Introduction/Scope/Parts of the project

The project is divided in two parts, First we have the framework parts, how to clean the dataset, and insert it into MongoDB. Then we will do a 'hypothetical'
analysis of our dataset, where we are using the processed dataset we produced in our first part, to answer some research questions that we consider would be
relevant for a future research project with real-world data. We will create visualizations in Tableau showing the hypothetical answers to our research
questions, and we will also explain how we would use MongoDB to support the visualizations and filtering in Tableau.
