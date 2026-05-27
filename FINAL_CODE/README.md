# Setup and Connect MongoDB Atlas to Tableau

## Use the Provided Tableau Workspace

If do not want to run all the python scripts, or make a MongoDB Atla cluster, or add ATLAS SQL for Tableau, but you want to view the complete final dashboard directly:

1. Download the Tableau workspace we provided.
2. Open it in Tableau.

The workspace already includes:

- All processed data coming from MongoDB
- Existing MongoDB-fed dashboards
- Visualizations and analytics

This means you do **not** need to create or use your own MongoDB database unless you want to rebuild the pipeline yourself.

---

Otherwise:

---

## 1. Set Up Your Python Environment

You can use **Jupyter Notebook in VS Code** to run the project.

### Create and activate a virtual environment

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

### Install required packages

A `requirements.txt` file is provided:

```bash
pip install -r requirements.txt
```

Otherwise, install the Python packages listed in `requirements.txt` manually.

---

## 2. Prepare the Source Data

1. Make sure the CSV file is inside the `sources` folder.
2. Open the Jupyter Notebook in VS Code.
3. Run the notebook cells sequentially.
   - The notebook imports reusable functions from the `helpers.py` file.
   - The data will be cleaned, transformed, and processed automatically.

---

## 3. Create a MongoDB Atlas Account

1. Go to MongoDB Atlas and create an account.
2. Create a new cluster using the **Free Tier** subscription.

---

## 4. Configure Database Access

### Save the MongoDB Connection String

1. Inside the cluster, click **Connect**.
2. Copy and save the **MongoDB connection string**.
   - This will later be used with the Python `pymongo` driver.

### Add Your IP Address

1. Go to **Network Access** or follow the setup prompts.
2. Add your current IP address to the safe IP list.

---

## 5. Enable Atlas SQL for Tableau

1. Inside your cluster, click **Connect**.
2. Choose **Atlas SQL**.

Save the following information:

- Atlas SQL URL
- Database name

### Create Authentication Credentials

1. Open **Security Quickstart** from the left panel.
2. Create a username and password.
3. Save these credentials securely.

---

## 6. Store Processed Data in MongoDB

After running the notebook:

- The processed documents are sent to and stored in your MongoDB Atlas cluster using the `pymongo` driver.
- The database is then ready to be connected to Tableau.

---

## 7. Connect MongoDB to Tableau

1. Open Tableau.

2. Add the **MongoDB SQL Interface** connector.
   - This is the recommended replacement for the legacy MongoDB BI Connector.

3. Enter:
   - Atlas SQL URL
   - Database name
   - Username
   - Password

4. Click **Connect**.

✅ Your MongoDB Atlas database is now connected to Tableau.

---
