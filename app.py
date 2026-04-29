import os
import streamlit as st
from databricks import sql
from datetime import datetime
import hashlib, uuid, io
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdf_canvas

# ── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_TOKEN = os.getenv('passkey')
SERVER_HOSTNAME  = os.getenv('server_hostname')
HTTP_PATH        = os.getenv('http_path')
TABLE_NAME       = "hive_metastore.labor_rates.unified_labor_rates"

if not (DATABRICKS_TOKEN and SERVER_HOSTNAME and HTTP_PATH):
    st.error("Missing environment variables: 'passkey', 'server_hostname', 'http_path'.")
    st.stop()

# ── SCHEMA ────────────────────────────────────────────────────────────────────
POSITION_VALUES = sorted([
    "ACCOUNT MANAGER","ACCOUNTANT","ADMIN","ANALYST","ASBESTOS, INSULATION, PIPE COVERERS",
    "BIM","BILLING","BOILERMAKERS","BRICKLAYERS","CAD","CARPENTERS",
    "CARPET AND LINOLEUM LAYERS","CEMENT MASON","COMMISSIONING","CONSTRUCTION MANAGER",
    "CONSULTANT","CONTRACTS","CONTROLS","COORDINATOR","DESIGN","ELECTRICIANS",
    "ELEVATOR MECHANIC","ENGINEER","EQUIPMENT OPERATOR: CRANE","ESTIMATOR",
    "FOOD SERVICE","GENERAL LABORER","GLAZIERS","HR","HVAC TECHNICIANS",
    "IT AND SOFTWARE","INTERN","IRONWORKER","LABORER","LIGHT EQUIPMENT OPERATOR",
    "MANAGER","MED EQUIPMENT OPERATOR","MEDICAL","MILLWRIGHTS","OFFICE MANAGER",
    "OPERATIONS MANAGER","PAINTER","PILE DRIVERS","PIPE FITTER","PLASTERER",
    "PLUMBERS, PIPEFITTERS, STEAMFITTERS, SPRINKLER INSTALLERS","PRECONSTRUCTION",
    "PROCUREMENT","PROCUREMENT MANAGER","PROGRAM COORDINATOR","PROGRAM DIRECTOR",
    "PROGRAM MANAGER","PROGRAMMER","PROJECT COORDINATOR","PROJECT DIRECTOR",
    "PROJECT EXECUTIVE","PROJECT MANAGER","PROJECT PRINCIPAL","PURCHASING","QC",
    "RIGGING","ROOFER","SAFETY","SCHEDULER","SECRETARY","SHEET METAL WORKERS",
    "SITE UTILITIES","SITEWORK","STONE MASON","SUPERINTENDENT","SUPPORT","SURVEYOR",
    "SUSTAINABILITY","TEAMSTER","TECHNICIAN","TECHNOLOGY","TILE LAYERS","WINDOW COVERINGS",
])
TRADE_TIER_VALUES = [
    "1-GENERAL FOREMAN","2-FOREMAN","3-JOURNEYMAN","4-APPRENTICE",
    "HELPER","PRE-APPRENTICE","MASTER","CE1","CE2","CE3",
    "CW1","CW2","CW3","CW4","I","II","III","IV","V","VI","VII","VIII",
    "CREW","ADMINISTRATIVE",
]
SENIORITY_VALUES = sorted([
    "ASSISTANT","ASSISTANT MANAGER","ASSOCIATE","ASSOCIATE PRINCIPAL","CHIEF",
    "COORDINATOR","DIRECTOR","DOCTOR","ENGINEER","EXPEDITOR","INTERN","JUNIOR",
    "LEAD","MANAGER","MEDIC","MID","NURSE","PRINCIPAL","PROFESSIONAL",
    "REGIONAL","REGIONAL MANAGER","SENIOR","SENIOR ASSOCIATE","SENIOR MANAGER",
    "SENIOR PRINCIPAL","SENIOR SPECIALIST","SENIOR SUPERVISOR",
    "SENIOR VICE PRESIDENT","SPECIALIST","SUPERVISOR","UNKNOWN","VICE PRESIDENT",
])
WORKER_ORIGIN_VALUES  = ["LOCAL","INTERNATIONAL","TRAVELER"]
FIELD_SUGGESTIONS = sorted(list(set([
    "ACCESS CONTROL","ACCESS FLOORING","ACOUSTIC","ADDED SHOP LABOR","ADMINISTRATOR",
    "ARCHITECT","ARCHITECTURAL","AREA","AUTOMATION","AUTOMATION TECHNICIAN","AV",
    "BMS","BITUMINOUS","BLADE","BRICK MASONRY","BRICKLAYER","CAD","CSA","CARPENTERS",
    "CARPENTRY","CASEWORK","CEMENT MASON","CIVIL","CLEANING","CLEANROOM","COMMISSIONING",
    "COMMERCIAL","COMPLIANCE","CONCRETE","CONSTRUCTABILITY","CONSTRUCTION","CONTROLS",
    "CORE AND SHELL","CORPORATE","COST","CX","DATABASE","DEI","DECORATOR","DELIVERY",
    "DEMOLITION","DESIGN","DETAILER","DOCUMENT","DOCUMENT PROCESSING","DOOR AND FENCE",
    "DOZER","DRYWALL","DUCTWORKS","EHS","EMS","ELECTRICAL","ELECTRICAL DESIGN",
    "ELEVATOR","ENERGY","ENVIRONMENT","EPOXY","EQUIPMENT","EXECUTION","FABRICATION",
    "FIELD","FIELD INSTALL","FIELD OPERATIONS","FINANCE","FINISHER","FIRE ALARM",
    "FIRE PROTECTION","FIREPROOFING","FIRESTOPPER","FLAGGER","FLATWORK","FORKLIFT",
    "FOUNDATION","GIS","GEOTECH","GLAZIERS","GRADING","GRAPHIC","HSE","HVAC",
    "HAZARDOUS WASTE","HEAVY EQUIPMENT","HIGH VOLTAGE","IT","INFRASTRUCTURE",
    "INSPECTION","INSTRUMENTATION AND CONTROLS","INTERIOR","IRONWORKER","KITCHEN",
    "LEED","LABORER","LANDSCAPE","LANDSCAPE IRRIGATION","LICENSED","LIFT","LIGHTING",
    "LOADER","LOGISTICS","MANUFACTURING","MEP","MAINTENANCE","MARKET","MARKETING",
    "MASTER TECHNICIAN","MATERIAL HANDLER","MECHANIC","MECHANICAL","MILLWRIGHTS",
    "MODELLING","MOVER","NON PWR","OFCI EQUIPMENT PURCHASING","OFE/MOFE","OFFICE",
    "OFFICE PLANNER","OILER","OPENINGS","OPERATIONS","ORNAMENTAL","PAINTER",
    "PILE DRIVERS","PROCESS","PROCESS EQUIPMENT","PROCESS PIPING","PROCESS PIPING WELDER",
    "PROCUREMENT","PRODUCTIVITY","PROGRAM","PROJECT","PROJECT CONTROLS","QC","QUANTITY",
    "REBAR","REINFORCING","RIGGING","RISK","ROLLER","ROOFER","SAFETY","SCAFFOLDING",
    "SCANNING","SCHEDULING","SCRAPER","SECURITY DEVICES TECHNICIAN","SERVICE",
    "SERVICE TECHNICIAN","SHEET METAL WORKERS","SHOP","SHOP FABRICATOR","SIGNAGE",
    "SITE","SITE OPERATION","SITEWORK","SKILLED","SPECIAL CONSTRUCTION","SPECIALTIES",
    "SPECIALTY GAS AND CHEMICALS","STEEL FRAMING","STRUCTURAL","STRUCTURAL STEEL",
    "SUSTAINABILITY","SYSTEMS","TAILMAN","TAPER","TECHNICIAN","TECHNOLOGY","TELEDATA",
    "TRAVELLING","TRUCK DRIVER","UNSKILLED","UTILITIES","VDC","WAREHOUSE",
    "WATER RESOURCES","WATERPROOFING","WELDER","WORKPLACE STRATEGY",
])))
TIME_VALUES            = ["ST","OT","DT"]
WORK_WEEK_VALUES       = [40, 50, 60, 70, 80]
CONTRACTOR_TYPE_VALUES = ["GC/CM","SUB","OWNER TEAM"]
LABOR_TYPE_VALUES      = ["TRADE","SUPERVISION"]
WORKER_CLASS_VALUES    = ["UNION","OPEN SHOP","GC PM","OWNER PM","UNKNOWN"]
WAGE_TYPE_VALUES       = ["BURDENED","NON-BURDENED","CREW RATE-BURDENED","CREW RATE-UNBURDENED","NO OHP-BURDENED"]
BUILDING_TYPE_VALUES   = sorted([
    "AVIATION","DATA CENTER","EDUCATION","GOVERNMENT","HOSPITALITY","LAB/RESEARCH",
    "MANUFACTURING","MANUFACTURING-AUTOMOTIVE","MANUFACTURING-F&B","MANUFACTURING-PHARMA",
    "MULTIFAMILY","OFFICE-COMMERCIAL","OFFICE-PHARMA","OFFICE-SEMICONDUCTOR","OTHER",
    "RETAIL","SEMICONDUCTOR","STADIUMS","UNKNOWN",
])
REGION_VALUES    = ["AMERICAS","ASIA PACIFIC","UK AND EUROPE","MIDDLE EAST","AFRICA"]
CONFIRMED_VALUES = ["BID","RESEARCHED"]
US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","GU","VI",
]

BURDEN_GROUPS = {
    "TAXES":      [("FICA","FICA"),("FUTA","FUTA"),("SUTA","SUTA")],
    "INSURANCE":  [("WORK_COMP","WORK COMP"),("LIABILITY_INS","LIABILITY INS"),("TAX_INS","TAX / INS")],
    "ALLOWANCES": [("FRINGE_BENEFITS","FRINGE BENEFITS"),("PER_DIEM","PER DIEM"),("SMALL_TOOLS","SMALL TOOLS")],
    "OVERHEAD":   [("OT","OT"),("OTHER_BURDEN","OTHER BURDEN"),("G_AND_A_OH","G&A (OH)"),("PROFIT","PROFIT")],
}
USA_ONLY_GROUPS = {"TAXES"}
ALL_BURDEN_KEYS = [k for grp in BURDEN_GROUPS.values() for k,_ in grp]

# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    return sql.connect(server_hostname=SERVER_HOSTNAME, http_path=HTTP_PATH, access_token=DATABRICKS_TOKEN)

@st.cache_resource
def init_table():
    conn = get_conn(); cur = conn.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        SOURCE STRING, SUBMITTED_BY STRING, POSITION STRING, LABOR_TYPE STRING,
        TRADE_TIER STRING, SENIORITY_LEVEL STRING, WORKER_ORIGIN STRING,
        WORKER_CLASSIFICATION STRING, FIELD STRING, TIME STRING, WORK_WEEK INT,
        CONTRACTOR STRING, CONTRACTOR_TYPE STRING, OWNER STRING, UNION_NUMBER STRING,
        WAGE_TYPE STRING, BUILDING_TYPE STRING, CITY STRING, STATE STRING,
        COUNTRY STRING, REGION STRING, CURRENCY STRING, START_DATE STRING,
        END_DATE STRING, DATE STRING, BILL_RATE DOUBLE, BASE DOUBLE, FICA DOUBLE,
        FUTA DOUBLE, SUTA DOUBLE, WORK_COMP DOUBLE, LIABILITY_INS DOUBLE,
        TAX_INS DOUBLE, FRINGE_BENEFITS DOUBLE, PER_DIEM DOUBLE, SMALL_TOOLS DOUBLE,
        OT DOUBLE, OTHER_BURDEN DOUBLE, G_AND_A_OH DOUBLE, PROFIT DOUBLE,
        NOTE STRING, CONFIRMED STRING, SUBMISSION_TS STRING, HASH_ID STRING,
        PROOF_HASH STRING
    ) USING DELTA""")
    cur.close()

init_table()

def check_duplicate(row: dict) -> bool:
    """Check if a record with same key fields already exists (excluding NOTE, FIELD, metadata)."""
    exclude = {"NOTE","FIELD","SUBMISSION_TS","HASH_ID","PROOF_HASH","SOURCE","SUBMITTED_BY"}
    conditions = []
    for k, v in row.items():
        if k in exclude: continue
        if v is None:
            conditions.append(f"{k} IS NULL")
        elif isinstance(v, (int, float)):
            conditions.append(f"{k} = {v}")
        else:
            conditions.append(f"{k} = '{str(v).replace(chr(39), chr(39)*2)}'")
    where = " AND ".join(conditions)
    conn = get_conn(); cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {where}")
    result = cur.fetchone()
    cur.close()
    return result[0] > 0 if result else False

def insert_row(data: dict):
    conn = get_conn(); cur = conn.cursor()
    ts      = datetime.utcnow().isoformat()
    hash_id = str(uuid.uuid4())
    proof   = hashlib.sha256(("|".join(str(v) for v in data.values())+f"|{ts}").encode()).hexdigest()
    data.update({"SUBMISSION_TS": ts, "HASH_ID": hash_id, "PROOF_HASH": proof})
    cols = ", ".join(data.keys())
    vals = ", ".join(
        "NULL" if v is None else str(v) if isinstance(v,(int,float))
        else f"'{str(v).replace(chr(39),chr(39)*2)}'"
        for v in data.values()
    )
    cur.execute(f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({vals})")
    cur.close()
    return proof, ts

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Labor Rate Intake", page_icon="📋", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700&family=Merriweather:wght@400;700&display=swap');

html, body, [class*="css"] { font-family:'Lato',sans-serif; background-color:#f5f2ef; color:#2c2c2c; }
.stApp { background-color:#f5f2ef; }

.cb-header {
    background:#3b1f52; padding:1.4rem 2rem; margin:-1rem -1rem 1.5rem -1rem;
    display:flex; align-items:center; justify-content:space-between;
}
.cb-header-left h1 { font-family:'Merriweather',serif; font-size:1.3rem; font-weight:700; color:#e3dedb; margin:0; letter-spacing:.02em; }
.cb-header-left p  { font-size:.72rem; color:#b8a8c8; margin:.2rem 0 0; letter-spacing:.06em; text-transform:uppercase; }
.cb-logo { font-family:'Merriweather',serif; font-size:1rem; font-weight:700; color:#e3dedb; opacity:.4; letter-spacing:.08em; }

.gate-wrap { max-width:460px; margin:4rem auto; text-align:center; }
.gate-wrap h2 { font-family:'Merriweather',serif; color:#3b1f52; font-size:1.4rem; margin-bottom:.4rem; }
.gate-wrap p  { color:#7a6a80; font-size:.85rem; margin-bottom:2rem; }

.mode-banner { padding:.55rem 1rem; border-radius:3px; margin:0 0 1.5rem; font-size:.75rem; font-weight:700; letter-spacing:.08em; text-transform:uppercase; }
.mode-usa  { background:#edf4ee; border:1px solid #5a9e6f; color:#2d6e44; }
.mode-intl { background:#f0ecf5; border:1px solid #3b1f52; color:#3b1f52; }

.sec-label { font-size:.65rem; font-weight:700; color:#3b1f52; letter-spacing:.18em; text-transform:uppercase; border-left:3px solid #3b1f52; padding-left:.55rem; margin:1.8rem 0 .9rem; }

.bill-box { background:#3b1f52; border-radius:4px; padding:1rem 1.5rem; margin:1rem 0; display:flex; align-items:baseline; gap:1rem; }
.bill-label { font-size:.68rem; font-weight:700; color:#b8a8c8; letter-spacing:.12em; text-transform:uppercase; }
.bill-value { font-size:2rem; font-weight:700; color:#e3dedb; font-family:'Merriweather',serif; }

.group-lbl { font-size:.62rem; font-weight:700; color:#7a5a9a; letter-spacing:.14em; text-transform:uppercase; margin:.6rem 0 .25rem; }

div[data-testid="stSelectbox"] > div > div,
div[data-testid="stTextInput"] > div > div > input,
div[data-testid="stNumberInput"] > div > div > input,
div[data-testid="stTextArea"] > div > div > textarea,
div[data-testid="stDateInput"] > div > div > input {
    background:#ffffff !important; border:1px solid #d4cdd9 !important;
    color:#2c2c2c !important; border-radius:3px !important;
    font-size:.85rem !important; font-family:'Lato',sans-serif !important;
}
div[data-testid="stTextInput"] > div > div > input:disabled {
    background:#ede8f0 !important; color:#7a5a9a !important;
    border-color:#c9bdd6 !important; font-weight:700 !important;
}
label { color:#5a4a6a !important; font-size:.72rem !important; font-weight:700 !important; letter-spacing:.05em !important; text-transform:uppercase !important; }

/* Chip buttons - pill style */
.stButton > button {
    background:#ffffff !important; border:1px solid #c9bdd6 !important; color:#3b1f52 !important;
    font-family:'Lato',sans-serif !important; font-size:.75rem !important; font-weight:700 !important;
    padding:.3rem .9rem !important; border-radius:20px !important; letter-spacing:.05em !important;
}
.stButton > button:hover { background:#3b1f52 !important; border-color:#3b1f52 !important; color:#e3dedb !important; }

section[data-testid="stSidebar"] { background:#3b1f52 !important; border-right:none !important; }
section[data-testid="stSidebar"] * { color:#e3dedb !important; }
section[data-testid="stSidebar"] .stButton > button {
    background:rgba(255,255,255,.1) !important; border:1px solid rgba(255,255,255,.2) !important; color:#e3dedb !important; border-radius:20px !important;
}
section[data-testid="stSidebar"] .stButton > button:hover { background:rgba(255,255,255,.2) !important; }
section[data-testid="stSidebar"] hr { border-color:rgba(255,255,255,.15) !important; }
hr { border-color:#ddd5e5 !important; }
</style>
""", unsafe_allow_html=True)

# ── SESSION STATE ─────────────────────────────────────────────────────────────
defaults = {
    "session_id":       str(uuid.uuid4()),
    "receipts":         [],
    "collapsed_groups": set(),   # groups that are collapsed but have data
    "burden_data":      {},      # persisted burden values: col_key -> float
    "active_groups":    set(),   # groups user has opened at least once
    "user_name":        None,
    "session_started":  False,
    "confirm_dup":      False,
    "pending_row":      None,
    "form_key":         0,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── HEADER (always) ───────────────────────────────────────────────────────────
st.markdown("""
<div class="cb-header">
  <div class="cb-header-left">
    <h1>Labor Rate Intake</h1>
    <p>Currie &amp; Brown · Construction Business Intelligence</p>
  </div>
  <div class="cb-logo">CBI</div>
</div>""", unsafe_allow_html=True)

# ── GATE ─────────────────────────────────────────────────────────────────────
if not st.session_state["session_started"]:
    st.markdown("""
    <div class="gate-wrap">
      <h2>Welcome</h2>
      <p>Enter your full name to begin your session.<br>
      Your name will be recorded with every entry you submit.</p>
    </div>""", unsafe_allow_html=True)
    _, col, _ = st.columns([1,2,1])
    with col:
        name_input = st.text_input("Full Name", placeholder="e.g. Rachel Personius", label_visibility="collapsed")
        if st.button("Start Session", use_container_width=True):
            if not name_input.strip():
                st.error("Please enter your full name to continue.")
            else:
                st.session_state["user_name"]       = name_input.strip()
                st.session_state["session_started"] = True
                st.rerun()
    st.stop()

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
user_name    = st.session_state["user_name"]
session_date = datetime.now().strftime("%b %d, %Y")
with st.sidebar:
    st.markdown(f"### {user_name}")
    st.caption(session_date)
    st.markdown("---")
    st.markdown(f"**{len(st.session_state['receipts'])} submission(s) this session**")
    st.markdown("---")
    st.markdown("""
**Fields marked ● are required.**

- Toggle USA / International at the top
- Labor Type controls Trade Tier vs Seniority Level
- Expand burden groups with **+** — values persist when collapsed
- Bill Rate updates automatically
- Download session receipt below
""")
    if st.button("Refresh cache"):
        st.cache_data.clear(); st.cache_resource.clear(); st.rerun()

# ── MODE TOGGLE ───────────────────────────────────────────────────────────────
is_usa = st.toggle("USA DATA", value=True, key=f"usa_toggle_{st.session_state['form_key']}")
if is_usa:
    st.markdown('<div class="mode-banner mode-usa">⬤ &nbsp;USA Mode — Country / Region / Currency locked to defaults</div>', unsafe_allow_html=True)
else:
    st.markdown('<div class="mode-banner mode-intl">⬤ &nbsp;International Mode — fill all geography fields</div>', unsafe_allow_html=True)

# ── 01 CLASSIFICATION ─────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">01 · Classification</div>', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
position     = c1.selectbox("Position ●", [""] + POSITION_VALUES, key=f"pos_{st.session_state['form_key']}")
labor_type   = c2.selectbox("Labor Type ●", [""] + LABOR_TYPE_VALUES, key=f"lt_{st.session_state['form_key']}")
# Worker classification: shown and mandatory for USA only. Hidden and NULL for international.
if is_usa:
    worker_class = c3.selectbox("Worker Classification ●", [""] + WORKER_CLASS_VALUES, key=f"wc_{st.session_state['form_key']}")
else:
    worker_class = None  # stored as NULL in Databricks, not shown to user

c1, c2, c3 = st.columns(3)
trade_tier = seniority_level = None
if labor_type == "TRADE":
    trade_tier      = c1.selectbox("Trade Tier ●", [""] + TRADE_TIER_VALUES, key=f"tt_{st.session_state['form_key']}")
elif labor_type == "SUPERVISION":
    seniority_level = c1.selectbox("Seniority Level ●", [""] + SENIORITY_VALUES, key=f"sl_{st.session_state['form_key']}")
else:
    c1.selectbox("Trade Tier / Seniority Level ●", ["— select Labor Type first —"], disabled=True)

# Field: dropdown suggestion + free text fallback
field_select = c2.selectbox("Field (select or type below)", [""] + FIELD_SUGGESTIONS, key=f"fs_{st.session_state['form_key']}")
field_custom = c2.text_input("Field (custom)", placeholder="Or type your own...", label_visibility="collapsed", key=f"fc_{st.session_state['form_key']}")
field = field_custom.strip().upper() if field_custom.strip() else field_select

wage_type = c3.selectbox("Wage Type ●", [""] + WAGE_TYPE_VALUES, key=f"wt_{st.session_state['form_key']}")

# ── 02 PROJECT DETAILS ────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">02 · Project Details</div>', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
contractor      = c1.text_input("Contractor", key=f"con_{st.session_state['form_key']}")
contractor_type = c2.selectbox("Contractor Type ●", [""] + CONTRACTOR_TYPE_VALUES, key=f"ct_{st.session_state['form_key']}")
owner           = c3.text_input("Owner", key=f"own_{st.session_state['form_key']}")

c1, c2, c3 = st.columns(3)
building_type = c1.selectbox("Building Type ●", [""] + BUILDING_TYPE_VALUES, key=f"bt_{st.session_state['form_key']}")
confirmed     = c2.selectbox("Confirmed ●", [""] + CONFIRMED_VALUES, key=f"conf_{st.session_state['form_key']}")
union_number  = c3.text_input("Union Number", key=f"un_{st.session_state['form_key']}") if is_usa else None

# ── 03 GEOGRAPHY ──────────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">03 · Geography</div>', unsafe_allow_html=True)
if is_usa:
    c1, c2, c3 = st.columns(3)
    city  = c1.text_input("City ●", key=f"city_{st.session_state['form_key']}")
    state = c2.selectbox("State ●", [""] + US_STATES, key=f"state_{st.session_state['form_key']}")
    c3.text_input("Country", value="USA", disabled=True)
    country = "USA"
    c1, c2, _ = st.columns(3)
    c1.text_input("Region", value="AMERICAS", disabled=True)
    region = "AMERICAS"
    c2.text_input("Currency", value="USD", disabled=True)
    currency = "USD"
    worker_origin = None
else:
    c1, c2, c3 = st.columns(3)
    city    = c1.text_input("City ●", key=f"city_{st.session_state['form_key']}")
    state   = c2.text_input("State", key=f"statei_{st.session_state['form_key']}")
    country = c3.text_input("Country ●", key=f"cntry_{st.session_state['form_key']}")
    c1, c2, c3 = st.columns(3)
    region        = c1.selectbox("Region ●", [""] + REGION_VALUES, key=f"reg_{st.session_state['form_key']}")
    currency      = c2.text_input("Currency ●", key=f"cur_{st.session_state['form_key']}")
    worker_origin = c3.selectbox("Worker Origin ●", [""] + WORKER_ORIGIN_VALUES, key=f"wo_{st.session_state['form_key']}")

# ── 04 DATES ─────────────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">04 · Dates</div>', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
date_val   = c1.date_input("Date ●", value=None, key=f"dv_{st.session_state['form_key']}")
start_date = c2.date_input("Start Date", value=None, key=f"sd_{st.session_state['form_key']}")
end_date   = c3.date_input("End Date", value=None, key=f"ed_{st.session_state['form_key']}")

# ── 05 RATES ─────────────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">05 · Rates &amp; Burden</div>', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
time_val  = c1.selectbox("Time ●", [""] + TIME_VALUES, key=f"tv_{st.session_state['form_key']}")
work_week = c2.selectbox("Work Week", [None] + WORK_WEEK_VALUES, format_func=lambda x: "" if x is None else str(x), key=f"ww_{st.session_state['form_key']}")
base      = c3.number_input("Base ●", value=0.0, step=0.01, format="%.2f", key=f"base_{st.session_state['form_key']}")

# ── 06 BURDEN CHIPS ───────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">06 · Optional Burden — click to expand / collapse</div>', unsafe_allow_html=True)

# ── BURDEN STATE SETUP ────────────────────────────────────────────────────────
# We use TWO keys per burden column:
#   "bv_{col_key}"        → persisted value (never wiped on collapse)
#   "bvw_{col_key}"       → widget key (only exists when group is open)
# On every change, on_change copies widget value → persisted value.
# On collapse, widget disappears but persisted value remains untouched.

if "grp_open" not in st.session_state:
    st.session_state["grp_open"] = {}

visible_groups = {g: cols for g, cols in BURDEN_GROUPS.items()
                  if not (g in USA_ONLY_GROUPS and not is_usa)}

# Ensure persisted keys exist
for grp, col_pairs in visible_groups.items():
    for col_key, _ in col_pairs:
        if f"bv_{col_key}" not in st.session_state:
            st.session_state[f"bv_{col_key}"] = 0.0

def save_burden(col_key):
    """Copy widget value into persisted key."""
    st.session_state[f"bv_{col_key}"] = st.session_state.get(f"bvw_{col_key}", 0.0)

def get_burden(col_key):
    return st.session_state.get(f"bv_{col_key}", 0.0)

# Chip row
chip_cols = st.columns(len(visible_groups))
for i, grp in enumerate(visible_groups):
    col_pairs = visible_groups[grp]
    is_open   = st.session_state["grp_open"].get(grp, False)
    has_data  = any(get_burden(k) != 0.0 for k, _ in col_pairs)
    if is_open:
        label = f"▲  {grp}"
    elif has_data:
        label = f"●  {grp}"
    else:
        label = f"+  {grp}"
    if chip_cols[i].button(label, key=f"chip_{grp}"):
        # Before collapsing, flush widget values → persisted values
        if is_open:
            for col_key, _ in col_pairs:
                if f"bvw_{col_key}" in st.session_state:
                    st.session_state[f"bv_{col_key}"] = st.session_state[f"bvw_{col_key}"]
        st.session_state["grp_open"][grp] = not is_open
        st.rerun()

# Render expanded groups
for grp, col_pairs in visible_groups.items():
    if not st.session_state["grp_open"].get(grp, False):
        continue
    st.markdown(f'<div class="group-lbl">— {grp}</div>', unsafe_allow_html=True)
    gcols = st.columns(len(col_pairs))
    for j, (col_key, col_label) in enumerate(col_pairs):
        # Seed widget from persisted value
        if f"bvw_{col_key}" not in st.session_state:
            st.session_state[f"bvw_{col_key}"] = get_burden(col_key)
        gcols[j].number_input(
            col_label, step=0.01, format="%.2f",
            key=f"bvw_{col_key}",
            on_change=save_burden, args=(col_key,),
            help="Negative values are valid"
        )
        # Keep persisted in sync on every render
        st.session_state[f"bv_{col_key}"] = st.session_state.get(f"bvw_{col_key}", get_burden(col_key))

bill_rate = base + sum(get_burden(k) for k in ALL_BURDEN_KEYS)
st.markdown(f"""
<div class="bill-box">
  <span class="bill-label">Bill Rate (Auto-Sum)</span>
  <span class="bill-value">${bill_rate:,.2f}</span>
</div>""", unsafe_allow_html=True)

# ── 07 NOTE ───────────────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">07 · Note</div>', unsafe_allow_html=True)
note = st.text_area("Note", height=80, placeholder="Any additional context...", label_visibility="collapsed", key=f"note_{st.session_state['form_key']}")

# ── VALIDATE ─────────────────────────────────────────────────────────────────
def validate():
    errors = []
    if not position:                                               errors.append("Position is required")
    if not labor_type:                                             errors.append("Labor Type is required")
    if labor_type == "TRADE"       and not trade_tier:             errors.append("Trade Tier is required when Labor Type = TRADE")
    if labor_type == "SUPERVISION" and not seniority_level:        errors.append("Seniority Level is required when Labor Type = SUPERVISION")
    if is_usa and not worker_class:                                errors.append("Worker Classification is required")
    if not wage_type:                                              errors.append("Wage Type is required")
    if not contractor_type:                                        errors.append("Contractor Type is required")
    if not building_type:                                          errors.append("Building Type is required")
    if not confirmed:                                              errors.append("Confirmed is required")
    if not city.strip():                                           errors.append("City is required")
    if is_usa and not state:                                       errors.append("State is required")
    if not is_usa and not country.strip():                         errors.append("Country is required")
    if not is_usa and not region:                                  errors.append("Region is required")
    if not is_usa and not currency.strip():                        errors.append("Currency is required")
    if not is_usa and not worker_origin:                           errors.append("Worker Origin is required")
    if not time_val:                                               errors.append("Time is required")
    if date_val is None:                                           errors.append("Date is required")
    if base == 0.0:                                                errors.append("Base is required and cannot be zero")
    return errors

def build_row():
    def n(v): return None if v == 0.0 else v
    return {
        "SOURCE":                "FORM",
        "SUBMITTED_BY":          user_name,
        "POSITION":              position,
        "LABOR_TYPE":            labor_type,
        "TRADE_TIER":            trade_tier or None,
        "SENIORITY_LEVEL":       seniority_level or None,
        "WORKER_ORIGIN":         worker_origin if not is_usa else None,
        "WORKER_CLASSIFICATION": worker_class or None,
        "FIELD":                 field.strip().upper() if field and field.strip() else None,
        "TIME":                  time_val,
        "WORK_WEEK":             work_week,
        "CONTRACTOR":            contractor.strip().upper() if contractor.strip() else None,
        "CONTRACTOR_TYPE":       contractor_type,
        "OWNER":                 owner.strip().upper() if owner.strip() else None,
        "UNION_NUMBER":          union_number.strip().upper() if is_usa and union_number and union_number.strip() else None,
        "WAGE_TYPE":             wage_type,
        "BUILDING_TYPE":         building_type,
        "CITY":                  city.strip().upper(),
        "STATE":                 state if state else None,
        "COUNTRY":               country.strip().upper(),
        "REGION":                region,
        "CURRENCY":              currency.strip().upper(),
        "START_DATE":            str(start_date) if start_date else None,
        "END_DATE":              str(end_date)   if end_date   else None,
        "DATE":                  str(date_val),
        "BILL_RATE":             round(bill_rate, 2),
        "BASE":                  round(base, 2),
        "FICA":   round(get_burden("FICA"), 2) if is_usa else None,
        "FUTA":   round(get_burden("FUTA"), 2) if is_usa else None,
        "SUTA":   round(get_burden("SUTA"), 2) if is_usa else None,
        "WORK_COMP":       n(round(get_burden("WORK_COMP"),       2)),
        "LIABILITY_INS":   n(round(get_burden("LIABILITY_INS"),   2)),
        "TAX_INS":         n(round(get_burden("TAX_INS"),         2)),
        "FRINGE_BENEFITS": n(round(get_burden("FRINGE_BENEFITS"), 2)),
        "PER_DIEM":        n(round(get_burden("PER_DIEM"),        2)),
        "SMALL_TOOLS":     n(round(get_burden("SMALL_TOOLS"),     2)),
        "OT":           n(round(get_burden("OT"),           2)),
        "OTHER_BURDEN": n(round(get_burden("OTHER_BURDEN"), 2)),
        "G_AND_A_OH":   n(round(get_burden("G_AND_A_OH"),   2)),
        "PROFIT":       n(round(get_burden("PROFIT"),       2)),
        "NOTE":         note.strip().upper() if note and note.strip() else None,
        "CONFIRMED":    confirmed,
    }

# ── SUBMIT ────────────────────────────────────────────────────────────────────
st.markdown("---")
submit = st.button("Submit Entry", use_container_width=True)

if submit:
    errors = validate()
    if errors:
        for e in errors:
            st.error(f"✕  {e}")
    else:
        row = build_row()
        # Duplicate check
        is_dup = check_duplicate(row)
        if is_dup and not st.session_state["confirm_dup"]:
            st.session_state["pending_row"] = row
            st.warning(
                "⚠️  A record with identical values already exists in the database "
                "(all fields except Note and Field match). "
                "Check the box below and click Submit again to proceed anyway."
            )
            st.session_state["confirm_dup"] = True
        else:
            with st.spinner("Submitting..."):
                proof, ts = insert_row(row)
            st.success("✓  Entry submitted successfully")
            st.markdown(f"**Timestamp:** `{ts}`  \n**Proof hash:** `{proof}`")
            st.session_state["receipts"].append(row)
            # Reset burden state for next entry
            for k in ALL_BURDEN_KEYS:
                st.session_state[f"bv_{k}"]  = 0.0
                st.session_state.pop(f"bvw_{k}", None)
            st.session_state["grp_open"]    = {}
            st.session_state["confirm_dup"] = False
            st.session_state["pending_row"] = None
            st.session_state["form_key"]   += 1
            st.rerun()

if st.session_state["confirm_dup"]:
    confirmed_dup = st.checkbox("I confirm this is not a duplicate and want to submit anyway")
    if confirmed_dup:
        row = st.session_state["pending_row"]
        if st.button("Confirm & Submit", use_container_width=True):
            with st.spinner("Submitting..."):
                proof, ts = insert_row(row)
            st.success("✓  Entry submitted successfully")
            st.markdown(f"**Timestamp:** `{ts}`  \n**Proof hash:** `{proof}`")
            st.session_state["receipts"].append(row)
            for k in ALL_BURDEN_KEYS:
                st.session_state[f"bv_{k}"]  = 0.0
                st.session_state.pop(f"bvw_{k}", None)
            st.session_state["grp_open"]    = {}
            st.session_state["confirm_dup"] = False
            st.session_state["pending_row"] = None
            st.session_state["form_key"]   += 1
            st.rerun()

# ── SESSION RECEIPT ───────────────────────────────────────────────────────────
if st.session_state["receipts"]:
    buf = io.BytesIO()
    c   = pdf_canvas.Canvas(buf, pagesize=letter)
    y   = 750
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, "Labor Rate Intake — Session Receipt"); y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(50, y, f"Submitted by: {user_name}"); y -= 15
    c.drawString(50, y, f"Session date: {session_date}"); y -= 15
    c.drawString(50, y, f"Total submissions: {len(st.session_state['receipts'])}"); y -= 25
    for idx, rec in enumerate(st.session_state["receipts"], 1):
        c.setFont("Helvetica-Bold", 11)
        c.drawString(50, y, f"Submission {idx}"); y -= 18
        c.setFont("Helvetica", 9)
        for k, v in rec.items():
            if v is None: continue
            c.drawString(60, y, f"{k}: {str(v)[:95]}"); y -= 13
            if y < 80: c.showPage(); y = 750
        y -= 10
    c.save(); buf.seek(0)
    with st.sidebar:
        st.markdown("---")
        st.download_button(
            "⬇ Download Session Receipt", data=buf,
            file_name=f"labor_receipt_{user_name.replace(' ','_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
            mime="application/pdf"
        )