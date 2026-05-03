# Parivesh Web Automation & Dashboard

An automated monitoring and data management system designed to track, process, and visualize project documents (Agendas and Minutes of Meetings) from the **Parivesh** portal. This tool specifically monitors geographical keywords in Chhattisgarh, India, and provides a consolidated view of relevant environmental clearances.

## 🚀 Features

- **Automated Scraping:** Fetches metadata for SEIAA, SEAC, and EAC committees directly from the Parivesh server.
- **Intelligent PDF Processing:** Parallelized downloading and text extraction from meeting agendas to identify specific geographical keywords (e.g., Dantewada, Bastar, Bijapur).
- **Consolidated Dashboard:** A Streamlit-based UI to filter, search, and analyze project data.
- **Smart Data Linking:** Automatically links meeting agendas with their corresponding Minutes of Meetings (MOM).
- **Export Capabilities:** Export filtered data views directly to Excel for offline analysis.
- **Database Integration:** Built for Supabase (PostgreSQL) with materialized views for high-performance data retrieval.

## 🛠️ Tech Stack

- **Frontend:** [Streamlit](https://streamlit.io/)
- **Data Processing:** [Pandas](https://pandas.pydata.org/), [PyPDF2](https://pypdf2.readthedocs.io/)
- **Database:** PostgreSQL (Supabase)
- **Concurrency:** Python `ThreadPoolExecutor`
- **Networking:** `requests` with robust retry strategies

## 📋 Prerequisites

- Python 3.9 or higher
- A PostgreSQL database (Supabase recommended)

## 🔧 Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/web-automation-v2.git
   cd web-automation-v2
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv .venv
   # On Windows:
   .venv\Scripts\activate
   # On macOS/Linux:
   source .venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Secrets:**
   Create a `.streamlit/secrets.toml` file in the project root:
   ```toml
   DATABASE_URL = "postgresql://user:password@host:port/dbname"
   ```

## 🖥️ Usage

1. **Launch the Dashboard:**
   ```bash
   streamlit run app.py
   ```

2. **Sync Data:**
   Use the "Fetch New Documents" button in the dashboard to trigger the automated scraper and PDF processor.

3. **Refresh View:**
   After syncing, use "Refresh View" to update the consolidated project list in the UI.

## 📁 Project Structure

- `app.py`: Main Streamlit application and dashboard UI.
- `utils.py`: Core scraper logic and PDF processing engine.
- `constants.py`: Centralized keyword list and database configuration.
- `setup_mat_view.py`: SQL script for creating the materialized view in PostgreSQL.
- `requirements.txt`: List of Python dependencies.

## ⚠️ Security Note

Ensure that `.streamlit/secrets.toml` and any local `.db` files are kept out of version control. The `.gitignore` provided in this repository is pre-configured to handle this.
