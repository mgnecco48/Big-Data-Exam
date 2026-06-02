# Student Learning Data Pipeline: PySpark, MongoDB Atlas, and Tableau

## Overview

This project transforms a fully synthetic student learning dataset with PySpark, stores the processed data in MongoDB Atlas, and connects MongoDB Atlas to Tableau for visualization.

The source data is synthetic research data. It does not contain real people, real survey responses, or proprietary data sources.

## Quick Access: Final Tableau Dashboard

If you do not want to run the Python scripts, create a MongoDB Atlas cluster, or configure Atlas SQL for Tableau, you can view the final dashboard directly.

1. Open the Tableau Workbook included included in the submission.

The workbook already includes:

- Processed data from MongoDB
- MongoDB-fed dashboards
- Visualizations and analytics

This means you do not need to create or use your own MongoDB database unless you want to rebuild the pipeline yourself.

You can also view the finished visualization on Tableau Public: [Online Learning Analysis](https://public.tableau.com/views/OnlineLearningAnalysis_17803869549000/FrontPage?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link).

---

## Requirements

To rebuild the pipeline yourself, you need:

- Python
- Jupyter Notebook, such as through VS Code
- Packages listed in `requirements.txt`
- MongoDB Atlas account
- Tableau
- MongoDB JDBC driver
- Tableau Connector for MongoDB (`.taco` file)

---

## Project Files

The pipeline uses these files:

- `requirements.txt`: Python dependencies
- `helpers.py`: reusable helper functions imported by the notebook
- `data.csv`: dataset containing the synthetic student data.
- `data_pipeline.ipynb`: runs the PySpark transformation and MongoDB insertion steps
- `Online-Learning-Analysis.twbx`: final dashboard file for Tableau, if included with the submission package.
- `URI.txt`: local file used to store the MongoDB Atlas connection string

### Important

Do not commit or share real MongoDB credentials. To rebuild the pipeline, add your own MongoDB Atlas connection string to `URI.txt` before running the notebook.

---

## Rebuild the Pipeline

Follow these steps if you want to process the source data, upload it to your own MongoDB Atlas cluster, and connect it to Tableau.

### 1. Set Up Your Python Environment

You can use Jupyter Notebook in VS Code to run the project.

#### Create and Activate a Virtual Environment

```bash
python -m venv venv
```

Activate it:

**Windows**

```bash
venv\Scripts\activate
```

**Mac/Linux**

```bash
source venv/bin/activate
```

#### Install Required Packages

A `requirements.txt` file is provided:

```bash
pip install -r requirements.txt
```

Otherwise, install the Python packages listed in `requirements.txt` manually.

---

### 2. Prepare the Source Data

1. Make sure the CSV file `data.csv` is located in the same directory as the `data_pipeline.ipynb`, `URI.txt` and `helpers.py` files.
2. Open the Jupyter Notebook in VS Code.
3. Run the notebook cells sequentially.

The notebook imports reusable functions from `helpers.py`. The data will be cleaned, transformed, and processed automatically.

---

### 3. Create a MongoDB Atlas Account

1. Go to [MongoDB Atlas](https://www.mongodb.com/products/platform/atlas-database) and create an account.
2. Create a new cluster using the free tier.

---

### 4. Configure Database Access

MongoDB Atlas connection documentation: [Connect to Your Cluster](https://www.mongodb.com/docs/atlas/tutorial/connect-to-your-cluster/).

#### Save the MongoDB Connection String

1. Inside the cluster, click **Connect**.
2. Copy and save the MongoDB connection string.

The connection string is used by the Python `pymongo` driver when the notebook uploads the processed documents to MongoDB.

#### Add Your IP Address

1. Go to **Network Access** or follow the setup prompts.
2. Add your current IP address to the IP access list.

---

### 5. Run the Notebook and Store Processed Data

After MongoDB Atlas is configured, add your own Atlas connection string to `URI.txt`. Then run the notebook cells that upload the processed documents.

The notebook stores the processed data in your MongoDB Atlas cluster using the `pymongo` driver. After the upload finishes, the database is ready to connect to Tableau.

---

### 6. Prepare Atlas SQL for Tableau

This project uses the Atlas-based SQL Interface workflow for Tableau. This avoids older local BI Connector or `mongosqld` workflows, which are legacy/deprecated approaches.

If you run into any issues, refer to the MongoDB SQL Interface documentation for Tableau: [Connect Your BI Tool](https://www.mongodb.com/docs/sql-interface/connect/).

1. Make sure the processed collections have been loaded into your Atlas database.
2. Configure the database for the SQL Interface in Atlas.
3. Create a database user with access to the database and collections used by the dashboard.
4. Allow your current IP address in **Network Access** if Atlas requires it.
5. Install the MongoDB JDBC driver.
6. Download the latest Tableau Connector for MongoDB `.taco` file from the MongoDB download center.
7. Move the `.taco` file into Tableau's connector directory:

- Windows: `C:\Users\<user>\Documents\My Tableau Repository\Connectors`
- macOS: `~/Documents/My Tableau Repository/Connectors`
- Linux: `/opt/tableau/connectors`

If you already have an older MongoDB Tableau connector file in that directory, remove the old `.taco` file so Tableau uses the latest connector.

#### Get the Atlas SQL Connection Information

1. In Atlas, go to the federated database instance or SQL Interface connection area for the project.
2. Click **Connect**.
3. Select **Atlas SQL Interface**.
4. Select the **Tableau Connector** option.
5. Choose the database that contains the processed collections.
6. Copy the SQL Interface connection string and connection parameters.

Save the following information:

- SQL Interface connection string
- Database name
- Authentication method
- Username
- Password

#### Create Authentication Credentials

1. In Atlas, go to **Database Access**.
2. Create a database user, or use an existing database user with permission to read the dashboard collections.
3. Save the username and password securely.

---

### 7. Connect MongoDB Atlas to Tableau

Use the MongoDB Tableau Connector after the `.taco` file has been installed.

1. Open Tableau.
2. Go to the **Connect** menu.
3. Select **MongoDB SQL Interface by MongoDB**.
4. Enter the SQL Interface connection string copied from Atlas.
5. Select the authentication method. For a normal username/password setup, use the database user's username and password.
6. Click **Sign In**.
7. Select the Atlas database and collections needed for the visualizations, such as `processed_dataset`, `word_count`, and `opinion_token_bridge`.

Your MongoDB Atlas database is now connected to Tableau through the hosted Atlas SQL Interface 
