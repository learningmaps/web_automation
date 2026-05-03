import io
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import re
from datetime import datetime
from typing import Iterable, List, Optional, Dict, Generator, Tuple
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from PyPDF2 import PdfReader
from constants import KEYWORDS, TABLE_NAME
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("PariveshScraper")

STATE_MAPPING: Dict[int, str] = {
    35: "Andaman and Nicobar Islands", 28: "Andhra Pradesh", 12: "Arunachal Pradesh",
    18: "Assam", 10: "Bihar", 4: "Chandigarh", 22: "Chhattisgarh", 7: "Delhi",
    30: "Goa", 24: "Gujarat", 6: "Haryana", 2: "Himachal Pradesh", 1: "Jammu and Kashmir",
    20: "Jharkhand", 29: "Karnataka", 32: "Kerala", 37: "Ladakh", 31: "Lakshadweep",
    23: "Madhya Pradesh", 27: "Maharashtra", 14: "Manipur", 17: "Meghalaya",
    15: "Mizoram", 13: "Nagaland", 21: "Odisha", 34: "Puducherry", 3: "Punjab",
    8: "Rajasthan", 11: "Sikkim", 33: "Tamil Nadu", 36: "Telangana",
    38: "The Dadra and Nagar Haveli and Daman and Diu", 16: "Tripura",
    5: "Uttarakhand", 9: "Uttar Pradesh", 19: "West Bengal"
}

class PariveshScraper:
    BASE_URL = "https://parivesh.nic.in"
    API_URL = f"{BASE_URL}/agendamom/getAgendaMomDocumentByCommitteeV2"

    def __init__(
        self,
        conn_string: str,
        keywords: Iterable[str] = (),
        table_name: str = "agenda_mom",
    ) -> None:
        logger.info("Initializing Scraper...")
        self.conn = psycopg2.connect(conn_string)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        self.keywords: List[str] = [k.lower() for k in keywords]
        self.keyword_patterns = {
            kw: re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
            for kw in self.keywords
        }
        self.table_name = table_name
        
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self._create_table()

    def _create_table(self) -> None:
        # Note: Unquoted identifiers in PostgreSQL become lowercase automatically.
        # Changed pdfFilePath -> pdffilepath for consistency.
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id BIGINT PRIMARY KEY, created_on TEXT, updated_on TEXT,
                created_by INTEGER, updated_by INTEGER, vers TEXT, date TEXT,
                ref_id INTEGER, ref_type TEXT, committee_type TEXT, pdffilepath TEXT,
                workgroup_id INTEGER, meeting_start_date TEXT, meeting_end_date TEXT,
                meeting_id TEXT, subject TEXT, sector TEXT, selected_sector INTEGER,
                sector_name TEXT, state TEXT, statename TEXT, statename_derived TEXT,
                is_active INTEGER, is_deleted INTEGER, is_processed INTEGER DEFAULT 0,
                matched_keywords TEXT, processed_on TEXT, pdf_text TEXT
            )
        """)
        self.conn.commit()

    def _format_date(self, date_str: Optional[str]) -> Optional[str]:
        if not date_str: return None
        try:
            clean_date = date_str.split(".")[0].replace("Z", "")
            return datetime.fromisoformat(clean_date).strftime("%Y-%m-%d %H:%M")
        except: return date_str

    def _derive_state_name(self, state_code: Optional[str | int]) -> Optional[str]:
        if state_code is None: return None
        try: return STATE_MAPPING.get(int(state_code))
        except: return None

    def fetch_for_committee(self, committee, ref_type: str) -> int:
        logger.info(f"Fetching metadata for {committee} ({ref_type})")
        params = {"committee": committee, "ref_type": ref_type, "workgroupId": "1"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
        try:
            resp = self.session.post(self.API_URL, params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json().get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch {committee} {ref_type}: {e}")
            return 0

        if not data: 
            logger.info(f"No documents found for {committee} {ref_type}")
            return 0

        insert_values = []
        for item in data:
            raw_path = item.get("pdfFilePath")
            code = item.get("state")
            insert_values.append((
                item.get("id"), self._format_date(item.get("created_on")),
                self._format_date(item.get("updated_on")), item.get("created_by"),
                item.get("updated_by"), item.get("vers"), self._format_date(item.get("date")),
                item.get("ref_id"), item.get("ref_type"), item.get("committee_type"),
                urljoin(self.BASE_URL, raw_path) if raw_path else None,
                item.get("workgroup_id"), self._format_date(item.get("meeting_start_date")),
                self._format_date(item.get("meeting_end_date")), item.get("meeting_id"),
                item.get("subject"), item.get("sector"), item.get("selected_sector"),
                item.get("sector_name"), code, item.get("stateName"),
                self._derive_state_name(code), int(bool(item.get("is_active"))),
                int(bool(item.get("is_deleted"))), 0, None
            ))

        sql = f"""
            INSERT INTO {self.table_name} (
                id, created_on, updated_on, created_by, updated_by, vers, date, 
                ref_id, ref_type, committee_type, pdffilepath, workgroup_id, 
                meeting_start_date, meeting_end_date, meeting_id, subject, 
                sector, selected_sector, sector_name, state, statename, statename_derived,
                is_active, is_deleted, is_processed, processed_on
            ) VALUES %s ON CONFLICT (id) DO NOTHING
        """
        try:
            execute_values(self.cur, sql, insert_values)
            new_rows = self.cur.rowcount
            self.conn.commit()
            logger.info(f"Bulk inserted {new_rows} new records for {committee}")
            return new_rows
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error during bulk insert: {e}")
            return 0

    def fetch_all_committees(
        self,
        committees: Iterable[str] = ("SEIAA", "SEAC", "EAC"),
        ref_types: Iterable[str] = ("AGENDA", "MOM"),
    ) -> Generator[Tuple[str, int], None, None]:
        for ctype in committees:
            for ref in ref_types:
                msg = f"Fetching {ctype} - {ref}..."
                new_count = self.fetch_for_committee(ctype, ref_type=ref)
                yield msg, new_count
                time.sleep(0.5)

    def _download_and_extract_text(self, rec_id: int, pdfpath: str) -> Tuple[int, str, List[str], str]:
        """Worker function for threads: Downloads, extracts, cleans, and matches."""
        try:
            logger.debug(f"Downloading PDF for ID {rec_id}")
            resp = self.session.get(pdfpath, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
            resp.raise_for_status()
            reader = PdfReader(io.BytesIO(resp.content))
            raw_text = "\n".join([p.extract_text() or "" for p in reader.pages])
            
            # Cleaning
            stop_patterns = ["List & Correspondence addresses", "Composition of Expert Appraisal Committee"]
            lower_text = raw_text.lower()
            stop_idx = len(raw_text)
            for p in stop_patterns:
                idx = lower_text.find(p.lower())
                if idx != -1 and idx < stop_idx: stop_idx = idx
            cleaned_text = raw_text[:stop_idx]

            # Matching
            matched = [kw for kw, pat in self.keyword_patterns.items() if pat.search(cleaned_text.lower())]
            
            return rec_id, cleaned_text, matched, "Success"
        except Exception as e:
            logger.warning(f"Failed to process PDF for ID {rec_id}: {e}")
            return rec_id, "", [], f"Error: {str(e)}"

    def process_pdfs_and_update(
        self, limit: int | None = None, max_workers: int = 5
    ) -> Generator[Dict, None, None]:
        """Parallel processing of PDFs with batch updates to database."""
        # FIX: Ensure pdffilepath is lowercase and NOT quoted if unquoted in DB
        sql = f"SELECT id, pdffilepath FROM {self.table_name} WHERE is_processed = 0 AND ref_type = 'AGENDA'"
        if limit:
            self.cur.execute(sql + " LIMIT %s", (limit,))
        else:
            self.cur.execute(sql)
        
        rows = self.cur.fetchall()
        total = len(rows)
        if total == 0: 
            logger.info("No pending PDFs to process.")
            return

        logger.info(f"Starting parallel processing for {total} PDFs using {max_workers} workers...")

        update_sql = f"""
            UPDATE {self.table_name} AS t SET
                is_processed = 1,
                matched_keywords = v.matched,
                processed_on = v.proc_on,
                pdf_text = v.txt
            FROM (VALUES %s) AS v(id, matched, proc_on, txt)
            WHERE v.id = t.id
        """

        results_batch = []
        batch_size = 5

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # FIX: Use lowercase key 'pdffilepath'
            future_to_id = {executor.submit(self._download_and_extract_text, r["id"], r["pdffilepath"]): r["id"] for r in rows if r["pdffilepath"]}
            
            for i, future in enumerate(as_completed(future_to_id), 1):
                rec_id, text, keywords, status = future.result()
                now = datetime.now().strftime("%Y-%m-%d %H:%M")
                
                if status == "Success":
                    results_batch.append((rec_id, ",".join(keywords) if keywords else None, now, text))
                
                if len(results_batch) >= batch_size or i == total:
                    if results_batch:
                        try:
                            execute_values(self.cur, update_sql, results_batch)
                            self.conn.commit()
                            logger.info(f"Batch updated {len(results_batch)} results to database.")
                            results_batch = []
                        except Exception as e:
                            self.conn.rollback()
                            logger.error(f"Batch update failed: {e}")
                            status = f"Batch Error"

                yield {"current": i, "total": total, "id": rec_id, "status": status}

    def close(self) -> None:
        logger.info("Closing Scraper connection.")
        self.conn.close()
