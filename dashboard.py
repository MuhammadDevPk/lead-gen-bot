import json
import os
import subprocess
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
# 1. State Management for Processes
# ----------------------------------------------------
if "processes" not in st.session_state:
    st.session_state.processes = {}


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
# 2. File Helpers & KPI Calculations
# ----------------------------------------------------
DATA_DIR = Path("data")
SOURCE_URLS = DATA_DIR / "source_urls.txt"
NO_WEBSITE = DATA_DIR / "no_website_leads.jsonl"
LEADS = DATA_DIR / "leads.jsonl"
QUALIFIED = DATA_DIR / "qualified_leads.jsonl"
ENRICHED = DATA_DIR / "enriched_leads.jsonl"
OUTREACH_HISTORY = DATA_DIR / "outreach_history.jsonl"


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
# 3. Main Dashboard UI Layout
# ----------------------------------------------------
st.title("💼 B2B Lead Gen Pipeline Dashboard")
st.markdown("Monitor sourcing, crawling, AI qualification, contact enrichment, and outreach injection in real-time.")

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

# Active background process warning
active_processes = [name for name, info in st.session_state.processes.items() if info["proc"].poll() is None]
if active_processes:
    st.info(f"⏳ **Active Processes**: {', '.join(active_processes)} in progress. The dashboard is auto-refreshing every 2 seconds to fetch new logs and data.")

# Top metrics Row (KPIs)
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric("Total Sourced", f"{count_file_lines(SOURCE_URLS)} URLs")
with col2:
    st.metric("Hot (No Website)", f"{count_file_lines(NO_WEBSITE)} Leads")
with col3:
    st.metric("Pages Crawled", f"{count_file_lines(LEADS)} Pages")
with col4:
    st.metric("AI Qualified", f"{count_qualified_leads(QUALIFIED)} Leads")
with col5:
    st.metric("Emails Found", f"{count_enriched_contacts(ENRICHED)} Contacts")
with col6:
    st.metric("Outreach Injected", f"{count_file_lines(OUTREACH_HISTORY)} Sent")

st.markdown("---")

# Data Browsing Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📋 Sourced URLs",
    "🔥 Hot Leads",
    "🕸️ Crawled Website Data",
    "🧠 AI Qualified Leads",
    "💎 Enriched Contacts",
    "🚀 Outreach History"
])

with tab1:
    st.subheader("Sourced URLs (source_urls.txt)")
    df1 = load_dataframe(SOURCE_URLS)
    if not df1.empty:
        st.dataframe(df1, use_container_width=True)
        st.write(f"Total Rows: {len(df1)}")
    else:
        st.info("No sourced URLs found. Run the Sourcer to extract website links.")

with tab2:
    st.subheader("Hot Leads with No Website (no_website_leads.jsonl)")
    df2 = load_dataframe(NO_WEBSITE)
    if not df2.empty:
        st.dataframe(df2, use_container_width=True)
        st.write(f"Total Rows: {len(df2)}")
    else:
        st.info("No website-less leads found. Run the Sourcer to extract leads.")

with tab3:
    st.subheader("Raw Crawled Data (leads.jsonl)")
    df3 = load_dataframe(LEADS)
    if not df3.empty:
        st.dataframe(df3, use_container_width=True)
        st.write(f"Total Rows: {len(df3)}")
    else:
        st.info("No crawled pages found. Run the Crawler to extract markdown content.")

with tab4:
    st.subheader("AI Qualified Lead Analysis (qualified_leads.jsonl)")
    df4 = load_dataframe(QUALIFIED)
    if not df4.empty:
        st.dataframe(df4, use_container_width=True)
        st.write(f"Total Rows: {len(df4)}")
    else:
        st.info("No qualified leads found. Run the Lead Qualifier/Analyzer.")

with tab5:
    st.subheader("Enriched Decision Makers & Contacts (enriched_leads.jsonl)")
    df5 = load_dataframe(ENRICHED)
    if not df5.empty:
        st.dataframe(df5, use_container_width=True)
        st.write(f"Total Rows: {len(df5)}")
    else:
        st.info("No enriched contacts found. Run the Contact Enricher.")

with tab6:
    st.subheader("Outreach Injector History (outreach_history.jsonl)")
    df6 = load_dataframe(OUTREACH_HISTORY)
    if not df6.empty:
        st.dataframe(df6, use_container_width=True)
        st.write(f"Total Rows: {len(df6)}")
    else:
        st.info("No outreach injection logs found. Run the Outreach Injector.")

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

# 5.1 Credentials Status Check in neat Expander
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

# 5.2 Subprocesses Controllers
st.sidebar.subheader("⚙️ Pipeline Controllers")

# Sourcing Control
st.sidebar.markdown("**Step 1: Lead Sourcing**")
sourcing_query = st.sidebar.text_input("Serper Search Query:", "Roofers in Dallas, TX")
if st.sidebar.button("Run Sourcer", use_container_width=True):
    run_pipeline_step("Sourcer", ["uv", "run", "python", "sourcer.py", sourcing_query])

# Crawler Control
st.sidebar.markdown("**Step 2: Web Crawler**")
if st.sidebar.button("Run Crawler", use_container_width=True):
    run_pipeline_step("Crawler", ["uv", "run", "python", "main.py", "--file", "data/source_urls.txt"])

# Analyzer Control
st.sidebar.markdown("**Step 3: Lead Qualifier**")
if st.sidebar.button("Run Lead Qualifier", use_container_width=True):
    run_pipeline_step("Lead Qualifier", ["uv", "run", "python", "analyzer.py"])

# Enricher Control
st.sidebar.markdown("**Step 4: Contact Enricher**")
if st.sidebar.button("Run Contact Enricher", use_container_width=True):
    run_pipeline_step("Contact Enricher", ["uv", "run", "python", "enricher.py"])

# Outreach Control
st.sidebar.markdown("**Step 5: Outreach Injector**")
if st.sidebar.button("Run Outreach Injector", use_container_width=True):
    run_pipeline_step("Outreach Injector", ["uv", "run", "python", "outreach.py"])

st.sidebar.divider()

# Daemon loop
st.sidebar.markdown("**🔄 Background Orchestrator**")
if st.sidebar.button("Start Daemon Loop", use_container_width=True):
    run_pipeline_step("Daemon", ["uv", "run", "python", "daemon.py"])

st.sidebar.divider()

# 5.3 Active Subprocess Monitoring
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
if active_processes:
    time.sleep(2.0)
    st.rerun()
