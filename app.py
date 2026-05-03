import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import io
import re
import traceback
import logging
from utils import PariveshScraper
from constants import KEYWORDS, TABLE_NAME
from datetime import datetime

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PariveshApp")

# ─── PAGE CONFIGURATION ───
st.set_page_config(
    page_title="Parivesh Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── DATABASE CONNECTION ───
def get_db_connection():
    conn_string = st.secrets.get("DATABASE_URL")
    if not conn_string:
        st.error("DATABASE_URL not found in Streamlit secrets.")
        st.stop()
    return psycopg2.connect(conn_string)

# ─── STYLING ───
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stButton>button { border-radius: 5px; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# ─── DATA ENGINE ───
@st.cache_data(show_spinner="🚀 Fetching fresh data from Supabase...")
def load_consolidated_data(include_text=False):
    conn_string = st.secrets.get("DATABASE_URL")
    if not conn_string:
        return pd.DataFrame()
    
    conn = psycopg2.connect(conn_string)
    
    cols = [
        "id", "processed_on", "norm_subject", "meeting_id", "date", 
        "committee_type", "meeting_start_date", "meeting_end_date", 
        "sector_name", "statename_derived", "matched_keywords", 
        "agenda_pdf_path", "mom_pdf_path", "is_processed", "raw_subject"
    ]
    if include_text:
        cols.append("pdf_text")
        
    query = f"SELECT {', '.join(cols)} FROM mv_consolidated_projects ORDER BY id DESC"
    
    try:
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            df['id'] = df['id'].astype(str)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def refresh_materialized_view():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Increase timeout to 5 minutes specifically for this operation
        cur.execute("SET statement_timeout = '300s'")
        cur.execute("REFRESH MATERIALIZED VIEW mv_consolidated_projects")
        conn.commit()
        st.cache_data.clear() # Clear cache so app pulls fresh data
        st.success("Database View Refreshed!")
    except Exception as e:
        st.error("Failed to refresh materialized view (Database Timeout).")
        st.exception(e)
    finally:
        conn.close()

# ─── SIDEBAR DIAGNOSTICS ───
with st.sidebar:
    st.header("Settings & Data")
    # include_text change will naturally trigger a cache re-run because it's an argument
    include_text = st.checkbox("🔍 Include PDF Text", value=False, help="Loading text data increases load time significantly.")
    
    st.divider()
    if st.checkbox("Show Database Diagnostics"):
        st.subheader("DB Status")
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            total_rows = cur.fetchone()[0]
            st.write(f"✅ Total rows in `{TABLE_NAME}`: **{total_rows}**")
            cur.execute(f"SELECT ref_type, COUNT(*) FROM {TABLE_NAME} GROUP BY ref_type")
            ref_counts = cur.fetchall()
            st.write("Distribution:")
            for r, c in ref_counts:
                st.write(f"- {r}: {c}")
            conn.close()
        except Exception as e:
            st.error("Diagnostics failed.")
            st.exception(e)
    
    if st.button("Clear App Cache & Rerun", help="Clears temporary session data and restarts the application flow."):
        st.cache_data.clear()
        st.rerun()

# ─── HEADER SECTION ───
col1, col2 = st.columns([2, 1])
with col1:
    st.title("Parivesh Dashboard")
    st.markdown("Automated Monitoring & Data Management System")

with col2:
    st.write("")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Fetch New Documents", use_container_width=True, help="Downloads the latest meeting agendas and minutes from the Parivesh server."):
            st.session_state.is_syncing = True
    with c2:
        if st.button("Stop Processing", use_container_width=True, help="Safely stops the background sync process after the current document is finished."):
            st.session_state.is_syncing = False
    with c3:
        if st.button("Refresh View", use_container_width=True, help="Re-calculates the consolidated view in the database to show recent updates."):
            refresh_materialized_view()
            st.rerun()

    if st.session_state.get('is_syncing', False):
        with st.status("Syncing with Parivesh Server...", expanded=True) as status:
            try:
                scraper = PariveshScraper(conn_string=st.secrets["DATABASE_URL"], keywords=KEYWORDS, table_name=TABLE_NAME)
                
                # Stage 1: Metadata Fetching
                committees = ["SEIAA", "SEAC", "EAC"]
                ref_types = ["AGENDA", "MOM"]
                total_meta = len(committees) * len(ref_types)
                meta_bar = st.progress(0, text="Initializing metadata fetch...")
                
                new_docs_total = 0
                for i, (fetch_msg, new_count) in enumerate(scraper.fetch_all_committees(committees, ref_types), 1):
                    new_docs_total += new_count
                    meta_bar.progress(i / total_meta, text=f"Stage 1/2: {fetch_msg}")
                
                meta_bar.empty()
                
                # Stage 2: PDF Processing
                my_bar = st.progress(0, text="Stage 2/2: Preparing PDF processing...")
                processed_total = 0
                for progress in scraper.process_pdfs_and_update():
                    processed_total += 1
                    curr, total = progress["current"], progress["total"]
                    pct = curr / total
                    my_bar.progress(pct, text=f"Stage 2/2: Processing {curr}/{total} (ID: {progress['id']}) - {progress['status']}")
                
                scraper.close()
                status.write("Finalizing view...")
                refresh_materialized_view() # This also clears cache
                
                # Display Stats
                st.session_state.last_sync_stats = {
                    "new_docs": new_docs_total,
                    "processed_pdfs": processed_total,
                    "time": datetime.now().strftime("%H:%M:%S")
                }
                
                status.update(label="Sync Complete!", state="complete", expanded=False)
            except Exception as e:
                st.error(f"Sync failed due to network error: {e}")
                st.info("The Parivesh server may have closed the connection. Retrying later is recommended.")
                if 'scraper' in locals():
                    scraper.close()
        st.session_state.is_syncing = False
        st.rerun()

# ─── SYNC STATS CALLOUT ───
if "last_sync_stats" in st.session_state:
    stats = st.session_state.last_sync_stats
    st.success(f"✅ **Last Sync Successful ({stats['time']})**: Added **{stats['new_docs']}** new documents and processed **{stats['processed_pdfs']}** PDFs.")
    if st.button("Clear Stats"):
        del st.session_state.last_sync_stats
        st.rerun()

st.divider()

# ─── MAIN CONTENT ───
try:
    df = load_consolidated_data(include_text=include_text)

    if df.empty:
        st.info("No records found. Click 'Fetch New Documents' to begin.")
    else:
        # ─── SMART FILTERS ───
        with st.container():
            st.markdown("### Filters")
            f1, f2, f3, f4 = st.columns(4)
            with f1:
                subject_search = st.text_input("Search Subject", placeholder="Type keywords...")
            with f2:
                all_states = sorted(df['statename_derived'].dropna().unique().tolist())
                selected_states = st.multiselect("State Name", options=all_states)
            with f3:
                all_committees = sorted(df['committee_type'].dropna().unique().tolist())
                selected_committees = st.multiselect("Committee Type", options=all_committees)
            with f4:
                status_filter = st.selectbox("Process Status", options=["All", "Processed", "Pending"])

            d1, d2, d3, d4 = st.columns(4)
            with d1:
                meeting_range = st.date_input("Meeting Date Range", value=[], help="Select start and end dates")
            with d2:
                processed_range = st.date_input("Processed On Range", value=[], help="Select start and end dates")
            with d3:
                kws_set = set()
                df['matched_keywords'].dropna().apply(lambda x: kws_set.update(x.split(',')) if x else None)
                keyword_filter = st.multiselect("Keyword Filter", options=sorted(list(kws_set)))
            with d4:
                mom_filter = st.selectbox("MOM Status", options=["All", "With MOM", "Without MOM"])

            filtered_df = df.copy()
            
            if subject_search:
                filtered_df = filtered_df[filtered_df['norm_subject'].str.contains(subject_search, case=False, na=False)]
            if selected_states:
                filtered_df = filtered_df[filtered_df['statename_derived'].isin(selected_states)]
            if selected_committees:
                filtered_df = filtered_df[filtered_df['committee_type'].isin(selected_committees)]
            if status_filter == "Processed":
                filtered_df = filtered_df[filtered_df['is_processed'] == 1]
            elif status_filter == "Pending":
                filtered_df = filtered_df[filtered_df['is_processed'] == 0]
            if mom_filter == "With MOM":
                filtered_df = filtered_df[filtered_df['mom_pdf_path'].notna()]
            elif mom_filter == "Without MOM":
                filtered_df = filtered_df[filtered_df['mom_pdf_path'].isna()]
            if keyword_filter:
                filtered_df = filtered_df[filtered_df['matched_keywords'].apply(
                    lambda x: any(kw in str(x) for kw in keyword_filter) if pd.notna(x) else False
                )]
            if len(meeting_range) == 2:
                start_date, end_date = meeting_range
                temp_dates = pd.to_datetime(filtered_df['date'], errors='coerce').dt.date
                filtered_df = filtered_df[(temp_dates >= start_date) & (temp_dates <= end_date)]
            if len(processed_range) == 2:
                start_date, end_date = processed_range
                temp_proc = pd.to_datetime(filtered_df['processed_on'], errors='coerce').dt.date
                filtered_df = filtered_df[(temp_proc >= start_date) & (temp_proc <= end_date)]

        # ─── METRICS ───
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Viewing", len(filtered_df), delta=f"Total: {len(df)}")
        m2.metric("With MOM", len(filtered_df[filtered_df['mom_pdf_path'].notna()]))
        m3.metric("Unprocessed", len(filtered_df[filtered_df['is_processed'] == 0]))
        m4.metric("Keyword Matches", len(filtered_df[filtered_df['matched_keywords'].notna()]))

        # ─── MAIN CONSOLIDATED DATAFRAME ───
        st.markdown(f"### Consolidated Projects ({len(filtered_df)})")
        
        col_config = {
            "id": None,
            "is_processed": st.column_config.CheckboxColumn("Processed", width="small"),
            "processed_on": st.column_config.DatetimeColumn("Processed On"),
            "norm_subject": st.column_config.TextColumn("Normalized Subject", width="large"),
            "meeting_id": st.column_config.TextColumn("Meeting ID"),
            "date": st.column_config.DateColumn("Date"),
            "committee_type": st.column_config.TextColumn("Committee"),
            "statename_derived": st.column_config.TextColumn("State"),
            "matched_keywords": st.column_config.TextColumn("Keywords"),
            "agenda_pdf_path": st.column_config.LinkColumn("Agenda PDF"),
            "mom_pdf_path": st.column_config.LinkColumn("MOM PDF"),
            "raw_subject": None,
        }
        if include_text:
            col_config["pdf_text"] = st.column_config.TextColumn("PDF Text", width="small")
        else:
            col_config["pdf_text"] = None

        st.dataframe(
            filtered_df,
            use_container_width=True,
            height=600,
            column_config=col_config,
            hide_index=True
        )

        # ─── FOOTER ACTIONS ───
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            filtered_df.to_excel(writer, index=False, sheet_name='Consolidated')
        
        st.download_button(
            label="📥 Download Consolidated Data as Excel",
            data=output.getvalue(),
            file_name=f"parivesh_export_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            help="Saves the currently filtered results into an Excel (.xlsx) file for offline analysis."
        )

except Exception as e:
    st.error("A critical error occurred in the application UI.")
    st.exception(e)
    if st.button("🔄 Reset App State", help="Clears internal session state and reruns the app to resolve persistent errors."):
        st.session_state.clear()
        st.rerun()
