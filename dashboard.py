import json
import os
import subprocess
import threading
import time
from pathlib import Path
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Load local .env variables
load_dotenv()

# Set Page Config
st.set_page_config(
    page_title="B2B Lead Gen Pipeline Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (Theme-aware accents)
st.markdown("""
    <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        .stMetric {
            background-color: var(--secondary-background-color);
            padding: 15px;
            border-radius: 10px;
            border: 1px solid rgba(128, 128, 128, 0.2);
            box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.05);
        }
        .stMetric label {
            color: var(--text-color) !important;
            opacity: 0.7;
            font-size: 0.875rem !important;
            font-weight: 600 !important;
        }
        .stMetric div[data-testid="stMetricValue"] {
            color: var(--text-color) !important;
            font-size: 1.875rem !important;
            font-weight: 700 !important;
        }
    </style>
""", unsafe_allow_html=True)

# ----------------------------------------------------
# 1. State Management for Processes & Full Pipeline
# ----------------------------------------------------
class PipelineOrchestrator:
    def __init__(self):
        self.running = False
        self.progress = 0
        self.step_name = "Idle"
        self.logs = ""
        self.error = None
        self.proc = None

    def run(self, query, niche, paths):
        self.running = True
        self.progress = 0
        self.step_name = "Starting full pipeline..."
        self.logs = ""
        self.error = None

        # Step 1: Sourcer (0% -> 20%)
        self.step_name = "Step 1/5: Sourcing Leads (Serper.dev)..."
        self.progress = 10
        cmd = ["uv", "run", "python", "sourcer.py", query, "-u", str(paths["source_urls"]), "-n", str(paths["no_website"])]
        if not self._run_step(cmd, "Sourcer"):
            return

        # Step 2: Crawler (20% -> 40%)
        self.step_name = "Step 2/5: Crawling Websites (Crawl4AI)..."
        self.progress = 30
        cmd = ["uv", "run", "python", "main.py", "-f", str(paths["source_urls"]), "-o", str(paths["leads"])]
        if not self._run_step(cmd, "Crawler"):
            return

        # Step 3: Lead Qualifier (40% -> 60%)
        self.step_name = "Step 3/5: AI Qualifying Websites (Rotating Keys)..."
        self.progress = 50
        cmd = ["uv", "run", "python", "analyzer.py", "-i", str(paths["leads"]), "-o", str(paths["qualified"])]
        if not self._run_step(cmd, "Lead Qualifier"):
            return

        # Step 4: Contact Enricher (60% -> 80%)
        self.step_name = "Step 4/5: Enriching Contact Emails (Apollo)..."
        self.progress = 70
        cmd = ["uv", "run", "python", "enricher.py", "-n", str(paths["no_website"]), "-q", str(paths["qualified"]), "-o", str(paths["enriched"])]
        if not self._run_step(cmd, "Contact Enricher"):
            return

        # Step 5: Outreach Injector (80% -> 100%)
        self.step_name = "Step 5/5: Injecting Leads (Instantly.ai)..."
        self.progress = 90
        cmd = ["uv", "run", "python", "outreach.py", "-i", str(paths["enriched"]), "-t", str(paths["outreach_history"])]
        if not self._run_step(cmd, "Outreach Injector"):
            return

        self.progress = 100
        self.step_name = "Pipeline Completed Successfully!"
        self.running = False

    def _run_step(self, cmd, name):
        self.logs += f"\n--- Starting {name} ---\nCommand: {' '.join(cmd)}\n"
        try:
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            # Read logs in real time
            for line in self.proc.stdout:
                self.logs += line
                # Keep logs bounded
                if len(self.logs) > 50000:
                    self.logs = self.logs[-30000:]
            
            self.proc.wait()
            ret = self.proc.returncode
            if ret != 0:
                self.error = f"{name} failed with exit code {ret}."
                self.logs += f"\nERROR: {self.error}\n"
                self.running = False
                return False
            return True
        except Exception as e:
            self.error = f"Exception running {name}: {e}"
            self.logs += f"\nERROR: {self.error}\n"
            self.running = False
            return False

    def terminate(self):
        if self.proc:
            try:
                self.proc.terminate()
                self.proc.wait()
            except:
                pass
            self.proc = None
        self.running = False
        self.step_name = "Terminated by user"


if "processes" not in st.session_state:
    st.session_state.processes = {}

if "orchestrator" not in st.session_state:
    st.session_state.orchestrator = PipelineOrchestrator()


def run_pipeline_step(step_name, cmd):
    # Check if already running
    if step_name in st.session_state.processes:
        proc_info = st.session_state.processes[step_name]
        proc = proc_info["proc"]
        if proc.poll() is None:
            st.sidebar.warning(f"⚠️ {step_name} is already running!")
            return

    try:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        # Use lowercase step name for log filename
        log_filename = log_dir / f"{step_name.lower().replace(' ', '_')}_run.log"
        log_file = open(log_filename, "w", encoding="utf-8")

        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=log_file,
            text=True
        )
        st.session_state.processes[step_name] = {
            "proc": proc,
            "log_file": log_file,
            "cmd": cmd,
            "log_path": log_filename
        }
        st.sidebar.success(f"🚀 Started {step_name}!")
    except Exception as e:
        st.sidebar.error(f"Failed to start {step_name}: {e}")


# ----------------------------------------------------
# 2. File Helpers & Niche Managers
# ----------------------------------------------------
DATA_DIR = Path("data")


def get_available_niches():
    niches = set()
    if not DATA_DIR.exists():
        return ["All Leads"]
    
    suffixes = [
        "_source_urls.txt",
        "_no_website_leads.jsonl",
        "_leads.jsonl",
        "_qualified_leads.jsonl",
        "_enriched_leads.jsonl",
        "_outreach_history.jsonl"
    ]
    try:
        for file in DATA_DIR.iterdir():
            if file.is_file():
                name = file.name
                for suffix in suffixes:
                    if name.endswith(suffix):
                        niche = name[:-len(suffix)]
                        if niche:
                            niches.add(niche.capitalize())
    except Exception as e:
        st.error(f"Error scanning data directory: {e}")
        
    return ["All Leads"] + sorted(list(niches))


def get_niche_paths(niche):
    if not niche or niche == "All Leads":
        prefix = ""
    else:
        prefix = f"{niche.lower()}_"
        
    return {
        "source_urls": DATA_DIR / f"{prefix}source_urls.txt",
        "no_website": DATA_DIR / f"{prefix}no_website_leads.jsonl",
        "leads": DATA_DIR / f"{prefix}leads.jsonl",
        "qualified": DATA_DIR / f"{prefix}qualified_leads.jsonl",
        "enriched": DATA_DIR / f"{prefix}enriched_leads.jsonl",
        "outreach_history": DATA_DIR / f"{prefix}outreach_history.jsonl"
    }


def count_file_lines(path):
    if not path.exists():
        return 0
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return sum(1 for line in f if line.strip())
    except:
        return 0


def count_qualified_leads(path):
    if not path.exists():
        return 0
    try:
        count = 0
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    if data.get("is_qualified") is True:
                        count += 1
                except:
                    pass
        return count
    except:
        return 0


def count_enriched_contacts(path):
    if not path.exists():
        return 0
    try:
        count = 0
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    if data.get("contact_email"):
                        count += 1
                except:
                    pass
        return count
    except:
        return 0


# Load raw data into Pandas DataFrames
def load_dataframe(path):
    if not path.exists():
        return pd.DataFrame()

    try:
        if path.suffix == ".txt":
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                urls = [line.strip() for line in f if line.strip()]
            return pd.DataFrame(urls, columns=["Sourced URL"])
        else:
            records = []
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        records.append(json.loads(line))
                    except:
                        pass
            return pd.DataFrame(records)
    except Exception as e:
        st.error(f"Error loading {path.name}: {e}")
        return pd.DataFrame()


# Read last N lines of a log file
def read_log_tail(path, n=50):
    if not path.exists():
        return "Log file does not exist yet. Run the script/daemon to generate logs."
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return "".join(lines[-n:])
    except Exception as e:
        return f"Error reading log file: {e}"


# ----------------------------------------------------
# 2.1 DataFrame Formatters & Search Filter
# ----------------------------------------------------
def format_qualified_df(df):
    if df.empty:
        return df
    formatted = pd.DataFrame()
    formatted["Business Name"] = df.get("business_name", "Unknown")
    formatted["Is Qualified"] = df.get("is_qualified", False)
    formatted["Automation Score"] = df.get("automation_score", 0)
    formatted["Qualification Reason"] = df.get("reason", "N/A")
    formatted["Website"] = df.get("url", "N/A")
    formatted["Found Email"] = df.get("contact_email", "N/A")
    return formatted


def format_enriched_df(df):
    if df.empty:
        return df
    formatted = pd.DataFrame()
    formatted["Business Name"] = df.get("business_name", df.get("title", "Unknown"))
    
    # Decision Maker Contact
    dm_contact = []
    for _, row in df.iterrows():
        fname = row.get("contact_first_name") or ""
        lname = row.get("contact_last_name") or ""
        title = row.get("contact_title") or ""
        full_name = f"{fname} {lname}".strip()
        if full_name and title:
            dm_contact.append(f"{full_name} ({title})")
        elif full_name:
            dm_contact.append(full_name)
        elif title:
            dm_contact.append(title)
        else:
            dm_contact.append("Not Found")
    formatted["Decision Maker Contact"] = dm_contact
    
    # Email & Status
    emails = []
    for _, row in df.iterrows():
        email = row.get("contact_email") or ""
        status = row.get("contact_email_status") or ""
        if email and status:
            emails.append(f"{email} ({status})")
        elif email:
            emails.append(email)
        else:
            emails.append("Not Found")
    formatted["Email & Status"] = emails
    
    formatted["Qualification Reason"] = df.get("lead_qualification_reason", df.get("reason", "N/A"))
    formatted["Website"] = df.get("url", "N/A")
    if "phone" in df.columns:
        formatted["Phone"] = df["phone"]
        
    return formatted


def filter_df_by_search(df, query):
    if df.empty or not query:
        return df
    try:
        mask = df.astype(str).apply(lambda x: x.str.contains(query, case=False)).any(axis=1)
        return df[mask]
    except:
        return df


def get_daemon_progress():
    path = Path("logs/daemon.log")
    if not path.exists():
        return 0, "Idle"
        
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            
        current_query = "Unknown"
        current_step = "Idle"
        progress = 0
        
        for line in reversed(lines):
            # Check if daemon is currently alert waiting (empty queries.txt)
            if "Alert: Queries file" in line and "empty or missing" in line:
                if current_step == "Idle":
                    current_step = "Waiting / Idle: data/queries.txt is empty. Add queries to resume."
                    progress = 0
                break
            elif "B2B Lead Generation Daemon initialized." in line and "Settings:" not in line:
                if current_step == "Idle":
                    current_step = "Daemon initialized, checking queries..."
                    progress = 0
                break
            elif "STARTING NEW PIPELINE CYCLE" in line:
                parts = line.split("QUERY: '")
                if len(parts) > 1:
                    current_query = parts[1].split("'")[0]
                if current_step == "Idle":
                    current_step = f"Starting cycle for '{current_query}'..."
                    progress = 5
                break
            elif "sourcer.py" in line and "Executing:" in line:
                if current_step == "Idle":
                    current_step = "Step 1/5: Sourcing Leads (Serper)..."
                    progress = 20
            elif "main.py" in line and "Executing:" in line:
                if current_step == "Idle":
                    current_step = "Step 2/5: Crawling Pages (Crawl4AI)..."
                    progress = 40
            elif "analyzer.py" in line and "Executing:" in line:
                if current_step == "Idle":
                    current_step = "Step 3/5: AI Qualifying Leads..."
                    progress = 60
            elif "enricher.py" in line and "Executing:" in line:
                if current_step == "Idle":
                    current_step = "Step 4/5: Enriching Contacts (Apollo)..."
                    progress = 80
            elif "outreach.py" in line and "Executing:" in line:
                if current_step == "Idle":
                    current_step = "Step 5/5: Injecting to Instantly..."
                    progress = 95
            elif "COMPLETED SUCCESSFULLY" in line:
                if current_step == "Idle":
                    current_step = "Completed last cycle successfully. Sleeping..."
                    progress = 100
                break
                
        return progress, current_step
    except Exception as e:
        return 0, f"Error parsing daemon log: {e}"


# ----------------------------------------------------
# 3. Main Dashboard UI Layout
# ----------------------------------------------------
st.title("💼 B2B Lead Gen Pipeline Dashboard")
st.markdown("Monitor sourcing, crawling, AI qualification, contact enrichment, and outreach injection in real-time.")

# 3.1 Niche Selector Dropdown
available_niches = get_available_niches()
selected_niche = st.selectbox("🎯 Select Niche/Lead Type View:", available_niches, index=0)

# Sourcing paths based on active niche
paths = get_niche_paths(selected_niche)

# Visual Pipeline Flow Stepper
st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; background-color: var(--secondary-background-color); padding: 15px; border-radius: 10px; margin-bottom: 20px; border: 1px solid rgba(128, 128, 128, 0.15); box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.03);">
        <div style="text-align: center; flex: 1;">🔍 <b>1. Sourcer</b><br><span style="font-size: 0.8rem; opacity: 0.7;">Extracts URLs</span></div>
        <div style="color: #38bdf8; font-weight: bold; padding: 0 5px;">➔</div>
        <div style="text-align: center; flex: 1;">🕸️ <b>2. Crawler</b><br><span style="font-size: 0.8rem; opacity: 0.7;">Scrapes Content</span></div>
        <div style="color: #38bdf8; font-weight: bold; padding: 0 5px;">➔</div>
        <div style="text-align: center; flex: 1;">🧠 <b>3. Lead Qualifier</b><br><span style="font-size: 0.8rem; opacity: 0.7;">AI Qualification</span></div>
        <div style="color: #38bdf8; font-weight: bold; padding: 0 5px;">➔</div>
        <div style="text-align: center; flex: 1;">💎 <b>4. Contact Enricher</b><br><span style="font-size: 0.8rem; opacity: 0.7;">Finds Contacts</span></div>
        <div style="color: #38bdf8; font-weight: bold; padding: 0 5px;">➔</div>
        <div style="text-align: center; flex: 1;">🚀 <b>5. Outreach</b><br><span style="font-size: 0.8rem; opacity: 0.7;">Injects to Instantly</span></div>
    </div>
""", unsafe_allow_html=True)

# 3.2 One-Click Pipeline & Daemon Progress Bar Display
orchestrator = st.session_state.orchestrator
daemon_info = st.session_state.processes.get("Daemon")
is_daemon_running = (daemon_info["proc"].poll() is None) if (daemon_info and "proc" in daemon_info) else False

if orchestrator.running:
    st.markdown("### ⚡ Full Pipeline Execution Progress")
    st.progress(orchestrator.progress)
    st.info(f"⏳ **Current Status**: {orchestrator.step_name}")
    with st.expander("📋 Real-time Pipeline Execution Logs", expanded=True):
        st.code(orchestrator.logs, language="log")
elif is_daemon_running:
    daemon_progress, daemon_status = get_daemon_progress()
    st.markdown("### 🔄 Background Daemon Loop Progress")
    st.progress(daemon_progress)
    st.info(f"⏳ **Daemon Status**: {daemon_status}")

# Active background process warning
active_processes = [name for name, info in st.session_state.processes.items() if info["proc"].poll() is None]
active_manuals = [name for name in active_processes if name != "Daemon"]
if active_manuals and not orchestrator.running:
    st.info(f"⏳ **Active Manual Subprocesses**: {', '.join(active_manuals)} in progress. The dashboard is auto-refreshing every 2 seconds to fetch new logs and data.")

# Top metrics Row (KPIs using niche-specific counts)
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric("Total Sourced", f"{count_file_lines(paths['source_urls'])} URLs")
with col2:
    st.metric("Hot (No Website)", f"{count_file_lines(paths['no_website'])} Leads")
with col3:
    st.metric("Pages Crawled", f"{count_file_lines(paths['leads'])} Pages")
with col4:
    st.metric("AI Qualified", f"{count_qualified_leads(paths['qualified'])} Leads")
with col5:
    st.metric("Emails Found", f"{count_enriched_contacts(paths['enriched'])} Contacts")
with col6:
    st.metric("Outreach Injected", f"{count_file_lines(paths['outreach_history'])} Sent")

st.markdown("---")

# 3.3 Interactive Search Input
search_val = st.text_input("🔍 Global Search Leads Live (filters active tab rows by business, contact, email, address, reason, etc.):", "")

# Load raw niche DataFrames
df1 = load_dataframe(paths["source_urls"])
df2 = load_dataframe(paths["no_website"])
df3 = load_dataframe(paths["leads"])
df4 = load_dataframe(paths["qualified"])
df5 = load_dataframe(paths["enriched"])
df6 = load_dataframe(paths["outreach_history"])

# Data Browsing Tabs (7 tabs, including Glossary)
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📋 Sourced URLs",
    "🔥 Hot Leads",
    "🕸️ Crawled Website Data",
    "🧠 AI Qualified Leads",
    "💎 Enriched Contacts",
    "🚀 Outreach History",
    "📖 Software Guide & Terms"
])

with tab1:
    st.subheader(f"Sourced URLs ({paths['source_urls'].name})")
    df1_filtered = filter_df_by_search(df1, search_val)
    if not df1_filtered.empty:
        st.dataframe(df1_filtered, use_container_width=True)
        st.write(f"Total Rows: {len(df1_filtered)}")
    else:
        st.info("No matching sourced URLs found.")

with tab2:
    st.subheader(f"Hot Leads with No Website ({paths['no_website'].name})")
    df2_filtered = filter_df_by_search(df2, search_val)
    if not df2_filtered.empty:
        # Rename default columns for better readability if present
        df2_show = df2_filtered.rename(columns={
            "title": "Business Name",
            "phoneNumber": "Phone Number",
            "category": "Category",
            "address": "Address"
        }, errors="ignore")
        st.dataframe(df2_show, use_container_width=True)
        st.write(f"Total Rows: {len(df2_filtered)}")
    else:
        st.info("No matching website-less leads found.")

with tab3:
    st.subheader(f"Raw Crawled Data ({paths['leads'].name})")
    df3_filtered = filter_df_by_search(df3, search_val)
    if not df3_filtered.empty:
        st.dataframe(df3_filtered, use_container_width=True)
        st.write(f"Total Rows: {len(df3_filtered)}")
    else:
        st.info("No matching crawled pages found.")

with tab4:
    st.subheader(f"AI Qualified Lead Analysis ({paths['qualified'].name})")
    df4_formatted = format_qualified_df(df4)
    df4_filtered = filter_df_by_search(df4_formatted, search_val)
    if not df4_filtered.empty:
        st.dataframe(df4_filtered, use_container_width=True)
        st.write(f"Total Rows: {len(df4_filtered)}")
    else:
        st.info("No matching qualified leads found.")

with tab5:
    st.subheader(f"Enriched Decision Makers & Contacts ({paths['enriched'].name})")
    df5_formatted = format_enriched_df(df5)
    df5_filtered = filter_df_by_search(df5_formatted, search_val)
    if not df5_filtered.empty:
        st.dataframe(df5_filtered, use_container_width=True)
        st.write(f"Total Rows: {len(df5_filtered)}")
    else:
        st.info("No matching enriched contacts found.")

with tab6:
    st.subheader(f"Outreach Injector History ({paths['outreach_history'].name})")
    df6_filtered = filter_df_by_search(df6, search_val)
    if not df6_filtered.empty:
        st.dataframe(df6_filtered, use_container_width=True)
        st.write(f"Total Rows: {len(df6_filtered)}")
    else:
        st.info("No matching outreach history found.")

with tab7:
    st.subheader("📖 Software Guide & Lead Category Glossary")
    st.markdown("""
    ### Pipeline Architecture & Workflow
    The B2B lead generation bot executes five sequential stages to identify, crawl, evaluate, enrich, and contact prospective leads:
    
    1. **Sourcing (`sourcer.py`)**: Uses the Serper.dev Places API to search local business listings matching your target niche and location. 
       - Leads WITH active websites are written to `source_urls.txt`.
       - Leads WITHOUT active websites are saved directly to `no_website_leads.jsonl` (Hot web-development prospects).
    2. **Crawling (`crawler.py` / `main.py`)**: Initiates Crawl4AI's asynchronous headless browser engine to extract raw HTML from active websites, converts them to Markdown, and writes them to `leads.jsonl`.
    3. **Lead Qualification (`analyzer.py`)**: Evaluates crawled websites using rotating LLM keys. The LLM judges if the business is service-based and if they lack booking automation or have structured flaws, outputting a qualification score and reason. Saved to `qualified_leads.jsonl`.
    4. **Contact Enrichment (`enricher.py`)**: Interfaces with the Apollo.io People API to locate decision-maker contacts (Owners, CEOs, Presidents) and their emails, falling back to scraped domain emails. Saved to `enriched_leads.jsonl`.
    5. **Outreach Injection (`outreach.py`)**: Automatically uploads enriched leads to your Instantly.ai campaigns, filtering for unique emails and skipping duplicates logged in `outreach_history.jsonl`.
    
    ### Lead Category Glossary
    - **Raw Sourced Data**: Initial listings fetched from Google Maps/Serper before filters or crawls are applied.
    - **Hot Leads (No Website)**: Businesses listing phone numbers on Google Maps but lacking a website—prime immediate targets for web development outreach.
    - **Crawled Data**: Markdown text parsed from active websites by Crawl4AI, representing the raw text scraped from homepages.
    - **AI Qualified Leads**: Websites scanned by LLMs and scored high for automation opportunities (such as missing schedulers, static contact forms, or broken elements).
    - **Enriched Leads**: Leads populated with contact details (names, roles, LinkedIn profiles, and verified emails) matching your criteria.
    - **Active Outreach**: Records pushed to email campaigns. Duplicates are blocked using the outreach tracker file.
    """)

# ----------------------------------------------------
# 4. Live Logs View
# ----------------------------------------------------
st.markdown("---")
st.subheader("📝 Live Execution Logs")

log_files = {
    "Daemon Loop Logs (logs/daemon.log)": Path("logs/daemon.log"),
    "Sourcer Output Logs": Path("logs/sourcer_run.log"),
    "Crawler Output Logs": Path("logs/crawler_run.log"),
    "Analyzer Output Logs": Path("logs/analyzer_run.log"),
    "Enricher Output Logs": Path("logs/enricher_run.log"),
    "Outreach Output Logs": Path("logs/outreach_run.log")
}

selected_log_name = st.selectbox("Select Log Stream to Monitor:", list(log_files.keys()))
selected_log_path = log_files[selected_log_name]

log_expander = st.expander(f"📊 Displaying last 50 lines of {selected_log_name}", expanded=True)
with log_expander:
    log_text = read_log_tail(selected_log_path, n=50)
    st.code(log_text, language="log")

# Add a manual refresh button for data and logs
if st.button("🔄 Refresh Data & Logs"):
    st.rerun()

# ----------------------------------------------------
# 5. Sidebar Controls & Credentials Check
# ----------------------------------------------------
st.sidebar.title("🛠️ Lead-Gen Controls")

# 5.1 One-Click Automation
st.sidebar.subheader("⚡ One-Click Automation")

orchestrator = st.session_state.orchestrator

if orchestrator.running:
    st.sidebar.info(f"⏳ Running: {orchestrator.step_name}")
    st.sidebar.progress(orchestrator.progress)
    if st.sidebar.button("🛑 Terminate Pipeline", use_container_width=True):
        orchestrator.terminate()
        st.sidebar.error("Terminated pipeline run.")
        st.rerun()
else:
    # Text input query for full pipeline run
    full_pipeline_query = st.sidebar.text_input("Full Pipeline Search Query:", "Roofers in Dallas, TX", key="full_q")
    if st.sidebar.button("🚀 Run Full Pipeline", use_container_width=True):
        t = threading.Thread(
            target=orchestrator.run,
            args=(full_pipeline_query, selected_niche, paths)
        )
        t.start()
        st.rerun()

st.sidebar.divider()

# 5.2 Infinite Daemon Mode Toggle Switch
st.sidebar.subheader("🔄 Infinite Daemon Mode")
daemon_info = st.session_state.processes.get("Daemon")
is_daemon_running = (daemon_info["proc"].poll() is None) if (daemon_info and "proc" in daemon_info) else False

toggle_val = st.sidebar.toggle("Enable Background Daemon", value=is_daemon_running)

if toggle_val != is_daemon_running:
    if toggle_val:
        # Start daemon.py in background
        run_pipeline_step("Daemon", ["uv", "run", "python", "daemon.py"])
    else:
        # Stop daemon
        if daemon_info and "proc" in daemon_info:
            daemon_info["proc"].terminate()
            daemon_info["proc"].wait()
            try:
                daemon_info["log_file"].close()
            except:
                pass
            st.sidebar.error("Stopped Daemon Loop")
            st.session_state.processes.pop("Daemon", None)
    st.rerun()

st.sidebar.divider()

# 5.3 Individual Step Controllers
with st.sidebar.expander("⚙️ Manual Step Controllers", expanded=False):
    # Sourcing Control
    st.markdown("**Step 1: Lead Sourcing**")
    sourcing_query = st.text_input("Serper Search Query:", "Roofers in Dallas, TX", key="single_q")
    if st.button("Run Sourcer", use_container_width=True):
        run_pipeline_step("Sourcer", ["uv", "run", "python", "sourcer.py", sourcing_query, "-u", str(paths["source_urls"]), "-n", str(paths["no_website"])])

    st.markdown("---")
    # Crawler Control
    st.markdown("**Step 2: Web Crawler**")
    if st.button("Run Crawler", use_container_width=True):
        run_pipeline_step("Crawler", ["uv", "run", "python", "main.py", "-f", str(paths["source_urls"]), "-o", str(paths["leads"])])

    st.markdown("---")
    # Analyzer Control
    st.markdown("**Step 3: Lead Qualifier**")
    if st.button("Run Lead Qualifier", use_container_width=True):
        run_pipeline_step("Lead Qualifier", ["uv", "run", "python", "analyzer.py", "-i", str(paths["leads"]), "-o", str(paths["qualified"])])

    st.markdown("---")
    # Enricher Control
    st.markdown("**Step 4: Contact Enricher**")
    if st.button("Run Contact Enricher", use_container_width=True):
        run_pipeline_step("Contact Enricher", ["uv", "run", "python", "enricher.py", "-n", str(paths["no_website"]), "-q", str(paths["qualified"]), "-o", str(paths["enriched"])])

    st.markdown("---")
    # Outreach Control
    st.markdown("**Step 5: Outreach Injector**")
    if st.button("Run Outreach Injector", use_container_width=True):
        run_pipeline_step("Outreach Injector", ["uv", "run", "python", "outreach.py", "-i", str(paths["enriched"]), "-t", str(paths["outreach_history"])])

st.sidebar.divider()

# 5.4 API Credentials Status Check in neat Expander
with st.sidebar.expander("🔑 API Credentials Status", expanded=False):
    credentials = {
        "Serper API Key": "SERPER_API_KEY",
        "OpenAI API / Keys JSON": "OPENAI_API_KEY",
        "Apollo API Key": "APOLLO_API_KEY",
        "Instantly API Key": "INSTANTLY_API_KEY",
        "Instantly Campaign ID": "INSTANTLY_CAMPAIGN_ID"
    }
    for name, env_var in credentials.items():
        val = os.getenv(env_var)
        if name == "OpenAI API / Keys JSON" and Path("data/keys.json").exists():
            st.write(f"🟢 **{name}**: Configured")
        elif val and not val.startswith("your_"):
            st.write(f"🟢 **{name}**: Configured")
        else:
            st.write(f"🔴 **{name}**: Missing")

st.sidebar.divider()

# 5.5 Active Subprocess Monitoring
st.sidebar.subheader("🔄 Active Subprocesses")
has_active = False

for name, info in list(st.session_state.processes.items()):
    proc = info["proc"]
    poll = proc.poll()
    if poll is None:
        has_active = True
        col_name, col_btn = st.sidebar.columns([3, 1])
        col_name.write(f"⏳ **{name}**")
        if col_btn.button("🛑 Kill", key=f"kill_{name}", use_container_width=True):
            proc.terminate()
            proc.wait()
            try:
                info["log_file"].close()
            except:
                pass
            st.sidebar.error(f"Killed {name}")
    else:
        try:
            info["log_file"].close()
        except:
            pass
        if poll == 0:
            st.sidebar.success(f"✅ **{name}**")
        else:
            st.sidebar.error(f"❌ **{name}** (Exit: {poll})")

if not has_active:
    st.sidebar.caption("No active background subprocesses.")

# ----------------------------------------------------
# 6. Auto-Refresh Engine (UX Upgrade)
# ----------------------------------------------------
is_active = len(active_processes) > 0 or orchestrator.running

if is_active:
    time.sleep(2.0)
    st.rerun()
