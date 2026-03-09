import io
import struct
import datetime
import pandas as pd
import numpy as np
import streamlit as st

# ─────────────────────────────────────────────
#  Page config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Touche — Till Audit",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  div[data-testid="stMetric"] {
    background: #f9fafb; border: 1px solid #e5e7eb;
    border-radius: 10px; padding: 12px 16px;
  }
  div[data-testid="stMetric"] label { font-size:0.75rem !important; color:#6b7280; font-weight:600; text-transform:uppercase; letter-spacing:0.04em; }
  div[data-testid="stMetricValue"]  { font-size:1.1rem !important; font-weight:700; color:#111827; }
  h2,h3 { margin-top:1.4rem !important; margin-bottom:0.3rem !important; }
  .sidebar-section { font-size:0.78rem; color:#9ca3af; text-transform:uppercase; letter-spacing:0.06em; font-weight:600; margin-top:1rem; margin-bottom:0.2rem; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  Pure-Python XLS reader (no xlrd needed)
#  Handles BIFF5/BIFF8 with LABEL, LABELSST,
#  NUMBER, MULRK records.
# ─────────────────────────────────────────────
def _read_xls_bytes(file_bytes: bytes) -> dict:
    """
    Read a legacy .xls (OLE2/BIFF) file from raw bytes.
    Returns {sheet_name: [[row], [row], ...]}
    Strings that start with \\x01 are BIFF5-style UTF-16LE encoded.
    """
    raw = file_bytes

    sector_size = 2 ** struct.unpack_from("<H", raw, 30)[0]
    num_fat = struct.unpack_from("<I", raw, 44)[0]
    difat = list(struct.unpack_from("<109I", raw, 76))
    fat = []
    for sec in difat[:num_fat]:
        if sec >= 0xFFFFFFFE:
            break
        fat += list(struct.unpack_from(f"<{sector_size//4}I", raw, 512 + sec * sector_size))

    def _read_stream(start):
        chain, s = [], start
        while s < 0xFFFFFFFE:
            chain.append(s)
            s = fat[s] if s < len(fat) else 0xFFFFFFFE
        return b"".join(raw[512+s*sector_size:512+(s+1)*sector_size] for s in chain)

    dir_sector = struct.unpack_from("<I", raw, 48)[0]
    dir_chain, s = [], dir_sector
    while s < 0xFFFFFFFE:
        dir_chain.append(s)
        s = fat[s] if s < len(fat) else 0xFFFFFFFE
    dir_data = b"".join(raw[512+s*sector_size:512+(s+1)*sector_size] for s in dir_chain)

    wb_start = None
    for i in range(len(dir_data) // 128):
        e = dir_data[i*128:(i+1)*128]
        nl = struct.unpack_from("<H", e, 64)[0]
        if nl < 2:
            continue
        name = e[:nl-2].decode("utf-16-le", errors="ignore")
        if name in ("Workbook", "Book"):
            wb_start = struct.unpack_from("<I", e, 116)[0]
            break

    if wb_start is None:
        raise ValueError("No Workbook stream found in XLS file.")

    wb = _read_stream(wb_start)
    SST, sheet_names, sheet_offsets, sheets, current_sheet = [], [], [], {}, None

    pos = 0
    while pos + 4 <= len(wb):
        rt = struct.unpack_from("<H", wb, pos)[0]
        rl = struct.unpack_from("<H", wb, pos+2)[0]
        rd = wb[pos+4:pos+4+rl]

        # SST (Shared String Table)
        if rt == 0x00FC:
            if len(rd) >= 8:
                num_str = struct.unpack_from("<I", rd, 4)[0]
                p = 8
                for _ in range(num_str):
                    if p + 3 > len(rd):
                        break
                    cc = struct.unpack_from("<H", rd, p)[0]
                    flags = rd[p+2]; p += 3
                    rich = bool(flags & 0x08); ext = bool(flags & 0x04)
                    nr = struct.unpack_from("<H", rd, p)[0] if rich and p+2<=len(rd) else 0
                    if rich: p += 2
                    ne = struct.unpack_from("<I", rd, p)[0] if ext and p+4<=len(rd) else 0
                    if ext: p += 4
                    if flags & 0x01:
                        s2 = rd[p:p+cc*2].decode("utf-16-le", errors="ignore"); p += cc*2
                    else:
                        s2 = rd[p:p+cc].decode("latin-1", errors="ignore"); p += cc
                    p += nr*4 + ne
                    SST.append(s2)

        # BOUNDSHEET
        elif rt == 0x0085 and len(rd) >= 6:
            offset = struct.unpack_from("<I", rd, 0)[0]
            nl2 = rd[4]; fl = rd[5]
            nm = rd[6:6+nl2*2].decode("utf-16-le","ignore") if fl&1 else rd[6:6+nl2].decode("latin-1","ignore")
            sheet_names.append(nm); sheet_offsets.append(offset); sheets[nm] = {}

        # BOF
        elif rt == 0x0809:
            if pos in sheet_offsets:
                current_sheet = sheet_names[sheet_offsets.index(pos)]

        # LABELSST
        elif rt == 0x00FD and current_sheet is not None and len(rd) >= 10:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            idx = struct.unpack_from("<I", rd, 6)[0]
            if idx < len(SST):
                sheets[current_sheet][(r, c)] = SST[idx]

        # LABEL (BIFF5-style: flag byte + utf-16-le or latin-1)
        elif rt == 0x0204 and current_sheet is not None and len(rd) >= 8:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            slen = struct.unpack_from("<H", rd, 6)[0]
            # BIFF5 label: byte 8 is flag (0x01 = unicode)
            if len(rd) >= 9:
                flag = rd[8]
                if flag == 0x01:
                    s2 = rd[9:9+slen*2].decode("utf-16-le", errors="ignore")
                else:
                    s2 = rd[8:8+slen].decode("latin-1", errors="ignore")
            else:
                s2 = rd[8:8+slen].decode("latin-1", errors="ignore")
            sheets[current_sheet][(r, c)] = s2

        # NUMBER
        elif rt == 0x0203 and current_sheet is not None and len(rd) >= 14:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            sheets[current_sheet][(r, c)] = struct.unpack_from("<d", rd, 6)[0]

        # RK
        elif rt == 0x027E and current_sheet is not None and len(rd) >= 10:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            rk = struct.unpack_from("<I", rd, 6)[0]
            val = float(rk >> 2) if (rk & 2) else struct.unpack_from("<d", b'\x00'*4 + struct.pack("<I", rk & 0xFFFFFFFC))[0]
            if rk & 1: val /= 100
            sheets[current_sheet][(r, c)] = val

        # MULRK
        elif rt == 0x00BE and current_sheet is not None:
            r = struct.unpack_from("<H", rd, 0)[0]
            cf = struct.unpack_from("<H", rd, 2)[0]
            for k in range((len(rd)-6)//6):
                rk = struct.unpack_from("<I", rd, 6+k*6)[0]
                val = float(rk >> 2) if (rk & 2) else struct.unpack_from("<d", b'\x00'*4 + struct.pack("<I", rk & 0xFFFFFFFC))[0]
                if rk & 1: val /= 100
                sheets[current_sheet][(r, cf+k)] = val

        pos += 4 + rl

    # Convert to 2D grid
    result = {}
    for nm, cells in sheets.items():
        if not cells:
            result[nm] = []; continue
        mr = max(r for r,c in cells)+1
        mc = max(c for r,c in cells)+1
        grid = [[None]*mc for _ in range(mr)]
        for (r,c),v in cells.items():
            grid[r][c] = v
        result[nm] = grid
    return result


def _clean_cell(v):
    """Strip BIFF5 flag byte prefix from strings; return None for garbage floats."""
    if v is None:
        return None
    if isinstance(v, str):
        if v.startswith("\x01"):
            # Remaining bytes are UTF-16LE — decode the raw string chars
            # After the flag byte the string was already decoded, just strip prefix
            return v[1:]
        return v.strip() or None
    if isinstance(v, float):
        # Garbage pointer values from MULRK misparse
        if v < 1 or v > 3_000_000:
            return None
        return v
    return v


def _excel_date(val):
    """Convert Excel serial date to Python date."""
    try:
        f = float(val)
        if f < 1:
            return None
        return (datetime.date(1899, 12, 30) + datetime.timedelta(days=int(f)))
    except Exception:
        return None


def _to_currency(v):
    try:
        return round(float(v), 2) if v is not None else None
    except Exception:
        return None


# ─────────────────────────────────────────────
#  Transform: Till Audit Report  (left table)
#  Power Query keeps: Col2=Date, Col4=Client,
#  Col7=Cash, Col9=Cash1, Col10=Deposits, Col12=Total
#  (0-indexed: 1,3,6,8,9,11)
# ─────────────────────────────────────────────
def process_till_audit_report(file_bytes: bytes) -> pd.DataFrame:
    sheets = _read_xls_bytes(file_bytes)
    if not sheets:
        raise ValueError("No sheets found in Till Audit Report file.")
    grid = next(iter(sheets.values()))

    # Find header row: look for a row containing "Date" and "Client"
    header_row = None
    for i, row in enumerate(grid):
        cleaned = [_clean_cell(v) for v in row]
        texts = [str(v).lower() for v in cleaned if v is not None and isinstance(v, str)]
        if "date" in texts and "client" in texts:
            header_row = i
            break

    if header_row is None:
        # Fallback: use Power Query column indices directly
        # PQ keeps Col2,Col4,Col7,Col9,Col10,Col12 (1-indexed) → 1,3,6,8,9,11 (0-indexed)
        keep_cols = [1, 3, 6, 8, 9, 11]
        data_rows = []
        for row in grid:
            if len(row) < 12:
                continue
            vals = [_clean_cell(row[c]) if c < len(row) else None for c in keep_cols]
            if vals[0] is None and vals[1] is None:
                continue
            data_rows.append(vals)
        df = pd.DataFrame(data_rows, columns=["Date", "Client", "Cash", "Cash1", "Deposits", "Total"])
    else:
        headers = [_clean_cell(v) for v in grid[header_row]]
        rows = []
        for row in grid[header_row+1:]:
            cleaned = [_clean_cell(v) if i < len(row) else None for i, v in enumerate(row)]
            if all(v is None for v in cleaned):
                continue
            rows.append(cleaned[:len(headers)])
        df = pd.DataFrame(rows, columns=headers)
        # Map to expected columns
        col_map = {}
        for col in df.columns:
            if col and isinstance(col, str):
                cl = col.lower()
                if cl == "date":      col_map[col] = "Date"
                elif cl == "client":  col_map[col] = "Client"
                elif cl == "cash" and "Date" not in col_map.values(): col_map[col] = "Cash"
                elif cl == "cash":    col_map[col] = "Cash"
        df = df.rename(columns=col_map)

    # Clean up
    df = df[df["Client"].notna()].copy()
    df["Date"] = df["Date"].apply(lambda v: _excel_date(v) if isinstance(v, float) else v)
    for c in ["Cash", "Cash1", "Deposits", "Total"]:
        if c in df.columns:
            df[c] = df[c].apply(_to_currency)
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
#  Transform: TillAudit  (main table)
#  Power Query keeps: Col2=Date, Col5=Client,
#  Col11=Cash, Col13=Cards, Col16=Other,
#  Col19=Total, Col22=Services, Col25=Retail
#  (0-indexed: 1,4,10,12,15,18,21,24)
#  Then adds Stylist via ffill on rows where Client is null
# ─────────────────────────────────────────────
def process_till_audit(file_bytes: bytes) -> pd.DataFrame:
    sheets = _read_xls_bytes(file_bytes)
    if not sheets:
        raise ValueError("No sheets found in TillAudit file.")
    grid = next(iter(sheets.values()))

    # Find header row
    header_row = None
    for i, row in enumerate(grid):
        cleaned = [_clean_cell(v) for v in row]
        texts = [str(v).lower().strip() for v in cleaned if v is not None and isinstance(v, str)]
        if "date" in texts and "client" in texts:
            header_row = i
            break

    # PQ column indices (0-based): Date=1, Client=4, Cash=10, Cards=12, Other=15, Total=18, Services=21, Retail=24
    KEEP = [1, 4, 10, 12, 15, 18, 21, 24]
    COL_NAMES = ["Date", "Client", "Cash", "Cards", "Other", "Total", "Services", "Retail"]

    if header_row is None:
        data_rows = []
        for row in grid:
            if max(KEEP) >= len(row):
                continue
            vals = [_clean_cell(row[c]) for c in KEEP]
            if all(v is None for v in vals):
                continue
            data_rows.append(vals)
        df_raw = pd.DataFrame(data_rows, columns=COL_NAMES)
    else:
        # Use header, then select matching columns
        rows = []
        for row in grid[header_row+1:]:
            cleaned = [_clean_cell(row[i]) if i < len(row) else None for i in range(len(row))]
            if all(v is None for v in cleaned):
                continue
            vals = [cleaned[c] if c < len(cleaned) else None for c in KEEP]
            rows.append(vals)
        df_raw = pd.DataFrame(rows, columns=COL_NAMES)

    # Add Stylist: rows where Client is None carry the stylist name in Date column
    df_raw["Stylist"] = df_raw.apply(
        lambda r: r["Date"] if (r["Client"] is None and r["Date"] is not None and isinstance(r["Date"], str)) else None,
        axis=1
    )
    df_raw["Stylist"] = df_raw["Stylist"].ffill()

    # Keep only client rows (Client not null)
    df = df_raw[df_raw["Client"].notna()].copy()

    # Reorder
    df = df[["Stylist", "Date", "Client", "Cash", "Cards", "Other", "Total", "Services", "Retail"]]

    # Date conversion
    df["Date"] = df["Date"].apply(lambda v: _excel_date(v) if isinstance(v, float) else v)

    for c in ["Cash", "Cards", "Other", "Total", "Services", "Retail"]:
        df[c] = df[c].apply(_to_currency)

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
#  Join + derived columns  (mirrors Power Query)
# ─────────────────────────────────────────────
def build_output(df_main: pd.DataFrame, df_report: pd.DataFrame) -> pd.DataFrame:
    """
    Left join df_main to df_report on Client + Date.
    Then compute:
      Cash1_Final  = (Cash1_1 + Cash_1) - Retail
      Cash1_Rate   = Cash1_Final * 0.30
      Deposit_Rate = (Deposits_1 / 1.2) * 0.70
      Total_SIQ    = Cash1_Final + Deposits_1
    """
    # Normalise join keys
    df_main = df_main.copy()
    df_report = df_report.copy()

    df_main["_jdate"]   = pd.to_datetime(df_main["Date"], errors="coerce").dt.date
    df_report["_jdate"] = pd.to_datetime(df_report["Date"], errors="coerce").dt.date
    df_main["_jclient"]   = df_main["Client"].astype(str).str.strip().str.lower()
    df_report["_jclient"] = df_report["Client"].astype(str).str.strip().str.lower()

    merged = df_main.merge(
        df_report[["_jclient", "_jdate", "Cash", "Cash1", "Deposits"]].rename(columns={
            "Cash": "Cash_1", "Cash1": "Cash1_1", "Deposits": "Deposits_1"
        }),
        on=["_jclient", "_jdate"],
        how="left",
    ).drop(columns=["_jclient", "_jdate", "Cash", "Cards", "Other", "Total", "Services"])

    for c in ["Cash_1", "Cash1_1", "Deposits_1", "Retail"]:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0.0)

    merged["Cash1_Final"]  = (merged["Cash1_1"] + merged["Cash_1"]) - merged["Retail"]
    merged["Cash1_Rate"]   = (merged["Cash1_Final"] * 0.30).round(2)
    merged["Deposit_Rate"] = ((merged["Deposits_1"] / 1.2) * 0.70).round(2)
    merged["Total_SIQ"]    = (merged["Cash1_Final"] + merged["Deposits_1"]).round(2)
    merged["Cash1_Final"]  = merged["Cash1_Final"].round(2)

    out = merged[["Stylist", "Date", "Client", "Cash1_Final", "Deposits_1",
                  "Cash1_Rate", "Deposit_Rate", "Total_SIQ"]].copy()
    out = out.rename(columns={
        "Cash1_Final":  "Cash (Net of Retail)",
        "Deposits_1":   "Deposits",
        "Cash1_Rate":   "Cash Rate (30%)",
        "Deposit_Rate": "Deposit Rate (58.3%)",
        "Total_SIQ":    "Total SIQ",
    })
    return out.sort_values(["Stylist", "Date"]).reset_index(drop=True)


# ─────────────────────────────────────────────
#  Sidebar
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🧾 Till Audit")
    st.markdown("---")
    st.markdown('<p class="sidebar-section">Upload files</p>', unsafe_allow_html=True)
    till_audit_file  = st.file_uploader("TillAudit (.xls/.xlsx)",         type=["xls","xlsx"])
    till_report_file = st.file_uploader("Till Audit Report (.xls/.xlsx)", type=["xls","xlsx"])
    st.markdown("---")
    st.caption("Upload both files to process and download the joined output.")


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
st.markdown("## 🧾 Till Audit — Clean & Join")
st.markdown("Replicates the Power Query workflow: cleans both files, joins on Client + Date, and computes derived columns.")
st.markdown("---")

if till_audit_file is None or till_report_file is None:
    st.info("👈  Upload **TillAudit** and **Till Audit Report** in the sidebar to begin.", icon="📂")
    st.stop()

# ── Process ──
with st.spinner("Reading and processing files…"):
    try:
        df_main   = process_till_audit(till_audit_file.read())
        df_report = process_till_audit_report(till_report_file.read())
        df_out    = build_output(df_main, df_report)
    except Exception as e:
        st.error(f"❌  Error processing files: {e}")
        import traceback; st.code(traceback.format_exc())
        st.stop()

# ── Summary KPIs ──
st.markdown("### 📊 Summary")

total_siq      = df_out["Total SIQ"].sum()
total_cash_net = df_out["Cash (Net of Retail)"].sum()
total_deposits = df_out["Deposits"].sum()
total_cash_rt  = df_out["Cash Rate (30%)"].sum()
total_dep_rt   = df_out["Deposit Rate (58.3%)"].sum()
n_stylists     = df_out["Stylist"].nunique()
n_clients      = len(df_out)

k1,k2,k3,k4 = st.columns(4)
k1.metric("Transactions",       f"{n_clients:,}")
k2.metric("Stylists",           f"{n_stylists:,}")
k3.metric("Total SIQ",          f"£{total_siq:,.2f}")
k4.metric("Total Deposits",     f"£{total_deposits:,.2f}")

k5,k6,k7,_ = st.columns(4)
k5.metric("Cash (Net Retail)",  f"£{total_cash_net:,.2f}")
k6.metric("Cash Rate (30%)",    f"£{total_cash_rt:,.2f}")
k7.metric("Deposit Rate (58.3%)", f"£{total_dep_rt:,.2f}")

st.markdown("---")

# ── Stylist summary ──
st.markdown("### 👤 Stylist Summary")

summ = df_out.groupby("Stylist", as_index=False).agg(
    Transactions     = ("Client",              "count"),
    Cash_Net_Retail  = ("Cash (Net of Retail)", "sum"),
    Deposits         = ("Deposits",              "sum"),
    Cash_Rate        = ("Cash Rate (30%)",       "sum"),
    Deposit_Rate     = ("Deposit Rate (58.3%)",  "sum"),
    Total_SIQ        = ("Total SIQ",             "sum"),
).round(2)
summ.columns = ["Stylist","Transactions","Cash (Net Retail)","Deposits","Cash Rate (30%)","Deposit Rate (58.3%)","Total SIQ"]

currency_cols = ["Cash (Net Retail)","Deposits","Cash Rate (30%)","Deposit Rate (58.3%)","Total SIQ"]
st.dataframe(
    summ.style.format({c: "£{:,.2f}" for c in currency_cols}),
    use_container_width=True,
    hide_index=True,
    height=min(500, 60 + len(summ)*35),
)
st.markdown("---")

# ── Full detail table ──
st.markdown("### 📋 Full Detail")

# Filters
fa, fb = st.columns(2)
with fa:
    all_stylists = sorted(df_out["Stylist"].dropna().unique())
    sel_stylists = st.multiselect("Filter by Stylist", all_stylists, default=all_stylists)
with fb:
    date_vals = pd.to_datetime(df_out["Date"], errors="coerce").dropna()
    if len(date_vals):
        d_min, d_max = date_vals.min().date(), date_vals.max().date()
        sel_dates = st.date_input("Date range", value=(d_min, d_max), min_value=d_min, max_value=d_max)
    else:
        sel_dates = None

filtered = df_out[df_out["Stylist"].isin(sel_stylists)].copy()
if sel_dates and len(sel_dates) == 2:
    filtered["_d"] = pd.to_datetime(filtered["Date"], errors="coerce").dt.date
    filtered = filtered[(filtered["_d"] >= sel_dates[0]) & (filtered["_d"] <= sel_dates[1])].drop(columns=["_d"])

st.caption(f"Showing {len(filtered):,} of {len(df_out):,} rows.")

currency_detail = ["Cash (Net of Retail)","Deposits","Cash Rate (30%)","Deposit Rate (58.3%)","Total SIQ"]
st.dataframe(
    filtered.style.format({c: "£{:,.2f}" for c in currency_detail if c in filtered.columns}),
    use_container_width=True,
    height=450,
)

st.markdown("---")

# ── Downloads ──
st.markdown("### ⬇️ Downloads")

dl1, dl2, dl3 = st.columns(3)

# Full output
out1 = io.BytesIO()
with pd.ExcelWriter(out1, engine="openpyxl") as writer:
    df_out.to_excel(writer, index=False, sheet_name="Till Audit Output")
    summ.to_excel(writer, index=False, sheet_name="Stylist Summary")
    df_main.to_excel(writer, index=False, sheet_name="TillAudit (cleaned)")
    df_report.to_excel(writer, index=False, sheet_name="Till Report (cleaned)")
out1.seek(0)
with dl1:
    st.download_button(
        "📥 Full Output Workbook",
        data=out1, file_name="Till Audit Output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

# Filtered view
out2 = io.BytesIO()
with pd.ExcelWriter(out2, engine="openpyxl") as writer:
    filtered.to_excel(writer, index=False, sheet_name="Filtered View")
out2.seek(0)
with dl2:
    st.download_button(
        "📥 Filtered View",
        data=out2, file_name="Till Audit Filtered.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

# Stylist summary
out3 = io.BytesIO()
with pd.ExcelWriter(out3, engine="openpyxl") as writer:
    summ.to_excel(writer, index=False, sheet_name="Stylist Summary")
out3.seek(0)
with dl3:
    st.download_button(
        "📥 Stylist Summary",
        data=out3, file_name="Till Audit Stylist Summary.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
