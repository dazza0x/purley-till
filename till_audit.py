import io
import struct
import datetime
import zipfile
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
# ─────────────────────────────────────────────
def _read_xls_bytes(file_bytes: bytes) -> dict:
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

        if rt == 0x00FC:
            if len(rd) >= 8:
                num_str = struct.unpack_from("<I", rd, 4)[0]
                p = 8
                for _ in range(num_str):
                    if p + 3 > len(rd): break
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

        elif rt == 0x0085 and len(rd) >= 6:
            offset = struct.unpack_from("<I", rd, 0)[0]
            nl2 = rd[4]; fl = rd[5]
            nm = rd[6:6+nl2*2].decode("utf-16-le","ignore") if fl&1 else rd[6:6+nl2].decode("latin-1","ignore")
            sheet_names.append(nm); sheet_offsets.append(offset); sheets[nm] = {}

        elif rt == 0x0809:
            if pos in sheet_offsets:
                current_sheet = sheet_names[sheet_offsets.index(pos)]

        elif rt == 0x00FD and current_sheet is not None and len(rd) >= 10:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            idx = struct.unpack_from("<I", rd, 6)[0]
            if idx < len(SST): sheets[current_sheet][(r, c)] = SST[idx]

        elif rt == 0x0204 and current_sheet is not None and len(rd) >= 8:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            slen = struct.unpack_from("<H", rd, 6)[0]
            if len(rd) >= 9:
                flag = rd[8]
                if flag == 0x01:
                    s2 = rd[9:9+slen*2].decode("utf-16-le", errors="ignore")
                else:
                    s2 = rd[8:8+slen].decode("latin-1", errors="ignore")
            else:
                s2 = rd[8:8+slen].decode("latin-1", errors="ignore")
            sheets[current_sheet][(r, c)] = s2

        elif rt == 0x0203 and current_sheet is not None and len(rd) >= 14:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            sheets[current_sheet][(r, c)] = struct.unpack_from("<d", rd, 6)[0]

        elif rt == 0x027E and current_sheet is not None and len(rd) >= 10:
            r = struct.unpack_from("<H", rd, 0)[0]
            c = struct.unpack_from("<H", rd, 2)[0]
            rk = struct.unpack_from("<I", rd, 6)[0]
            val = float(rk >> 2) if (rk & 2) else struct.unpack_from("<d", b'\x00'*4 + struct.pack("<I", rk & 0xFFFFFFFC))[0]
            if rk & 1: val /= 100
            sheets[current_sheet][(r, c)] = val

        elif rt == 0x00BE and current_sheet is not None:
            r = struct.unpack_from("<H", rd, 0)[0]
            cf = struct.unpack_from("<H", rd, 2)[0]
            for k in range((len(rd)-6)//6):
                rk = struct.unpack_from("<I", rd, 6+k*6)[0]
                val = float(rk >> 2) if (rk & 2) else struct.unpack_from("<d", b'\x00'*4 + struct.pack("<I", rk & 0xFFFFFFFC))[0]
                if rk & 1: val /= 100
                sheets[current_sheet][(r, cf+k)] = val

        pos += 4 + rl

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
    if v is None: return None
    if isinstance(v, str):
        if v.startswith("\x01"): return v[1:]
        return v.strip() or None
    if isinstance(v, float):
        if v < 1 or v > 3_000_000: return None
        return v
    return v


def _excel_date(val):
    try:
        f = float(val)
        if f < 1: return None
        return (datetime.date(1899, 12, 30) + datetime.timedelta(days=int(f)))
    except Exception:
        return None


def _to_currency(v):
    try:
        return round(float(v), 2) if v is not None else None
    except Exception:
        return None


# ─────────────────────────────────────────────
#  Transform: Till Audit Report
# ─────────────────────────────────────────────
def process_till_audit_report(file_bytes: bytes) -> pd.DataFrame:
    sheets = _read_xls_bytes(file_bytes)
    if not sheets:
        raise ValueError("No sheets found in Till Audit Report file.")
    grid = next(iter(sheets.values()))

    header_row = None
    for i, row in enumerate(grid):
        cleaned = [_clean_cell(v) for v in row]
        texts = [str(v).lower() for v in cleaned if v is not None and isinstance(v, str)]
        if "date" in texts and "client" in texts:
            header_row = i
            break

    if header_row is None:
        keep_cols = [1, 3, 6, 8, 9, 11]
        data_rows = []
        for row in grid:
            if len(row) < 12: continue
            vals = [_clean_cell(row[c]) if c < len(row) else None for c in keep_cols]
            if vals[0] is None and vals[1] is None: continue
            data_rows.append(vals)
        df = pd.DataFrame(data_rows, columns=["Date", "Client", "Cash", "Cash1", "Deposits", "Total"])
    else:
        headers = [_clean_cell(v) for v in grid[header_row]]
        rows = []
        for row in grid[header_row+1:]:
            cleaned = [_clean_cell(v) if i < len(row) else None for i, v in enumerate(row)]
            if all(v is None for v in cleaned): continue
            rows.append(cleaned[:len(headers)])
        df = pd.DataFrame(rows, columns=headers)
        col_map = {}
        for col in df.columns:
            if col and isinstance(col, str):
                cl = col.lower()
                if cl == "date":     col_map[col] = "Date"
                elif cl == "client": col_map[col] = "Client"
                elif cl == "cash":   col_map[col] = "Cash"
        df = df.rename(columns=col_map)

    df = df[df["Client"].notna()].copy()
    df["Date"] = df["Date"].apply(lambda v: _excel_date(v) if isinstance(v, float) else v)
    for c in ["Cash", "Cash1", "Deposits", "Total"]:
        if c in df.columns:
            df[c] = df[c].apply(_to_currency)
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
#  Transform: TillAudit
#
#  Column detection is header-name driven so that
#  extra or shifted columns (e.g. Gift Cards,
#  Vouchers) never corrupt the positional mapping.
#
#  Any column whose header contains "gift" or
#  "voucher" (case-insensitive) is treated as an
#  additional deposit and summed into Deposits.
# ─────────────────────────────────────────────

# Canonical header → internal name mapping.
# Keys are lowercase substrings to match flexibly.
# ORDER MATTERS: more specific entries must come before broad ones
# ("gift cards" must not match "cards" -> Cards before we check "gift")
_TILL_COL_MAP = {
    "date":      "Date",
    "client":    "Client",
    "cash1":     "Cash1",    # must precede "cash" so Cash1 isn't mapped to Cash
    "cash":      "Cash",
    "deposits":  "Deposits", # exact plural — must precede any "deposit" keyword check
    "other":     "Other",
    "total":     "Total",
    "service":   "Services",
    "retail":    "Retail",
    # "cards" intentionally omitted — "Gift Cards" / "Other Card" / "Stripe" are
    # handled separately as pass-through columns, not a core mapped field
}

# Column headers that should be treated as additional Deposits.
# Checked AFTER _match_col so already-mapped columns are never double-counted.
_DEPOSIT_EXTRA_KEYWORDS = ["gift", "voucher"]   # removed "deposit" — Deposits is mapped above


def _match_col(header: str):
    """Return internal column name for a header string, or None if unrecognised."""
    if not header or not isinstance(header, str):
        return None
    h = header.strip().lower()
    for key, name in _TILL_COL_MAP.items():
        if key in h:
            return name
    return None


def process_till_audit(file_bytes: bytes) -> pd.DataFrame:
    sheets = _read_xls_bytes(file_bytes)
    if not sheets:
        raise ValueError("No sheets found in TillAudit file.")
    grid = next(iter(sheets.values()))

    # ── Find header row ──────────────────────────────────────────
    header_row = None
    for i, row in enumerate(grid):
        cleaned = [_clean_cell(v) for v in row]
        texts = [str(v).lower().strip() for v in cleaned if v is not None and isinstance(v, str)]
        if "date" in texts and "client" in texts:
            header_row = i
            break

    if header_row is None:
        raise ValueError(
            "Could not find a header row containing 'Date' and 'Client' "
            "in TillAudit file. Check the file format."
        )

    # ── Build header→index map ───────────────────────────────────
    raw_headers = [_clean_cell(v) for v in grid[header_row]]

    # col_index["Date"] = 3, col_index["Cash"] = 10, etc.
    col_index: dict[str, int] = {}
    # deposit_extra_indices: indices of gift card / voucher columns
    deposit_extra_indices: list[int] = []

    for idx, h in enumerate(raw_headers):
        if not h or not isinstance(h, str):
            continue
        h_low = h.strip().lower()
        internal = _match_col(h)
        if internal and internal not in col_index:
            col_index[internal] = idx
        # Check for extra deposit-like columns (gift cards, vouchers).
        # Only treat as extra deposit if the column was NOT already mapped
        # to a core internal column — prevents Deposits itself being double-counted.
        if internal is None and any(kw in h_low for kw in _DEPOSIT_EXTRA_KEYWORDS):
            deposit_extra_indices.append(idx)

    required = ["Date", "Client"]
    missing = [c for c in required if c not in col_index]
    if missing:
        raise ValueError(f"TillAudit file is missing required columns: {missing}")

    # ── Parse data rows ──────────────────────────────────────────
    data_rows = []
    for row in grid[header_row + 1:]:
        cleaned = [_clean_cell(row[i]) if i < len(row) else None for i in range(len(row))]
        if all(v is None for v in cleaned):
            continue

        def _get(name):
            idx = col_index.get(name)
            return cleaned[idx] if idx is not None and idx < len(cleaned) else None

        # Sum any gift card / voucher columns for this row
        extra_deposit = 0.0
        for idx in deposit_extra_indices:
            v = cleaned[idx] if idx < len(cleaned) else None
            try:
                extra_deposit += float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                pass

        data_rows.append({
            "Date":     _get("Date"),
            "Client":   _get("Client"),
            "Cash":     _get("Cash"),
            "Cards":    _get("Cards"),
            "Other":    _get("Other"),
            "Total":    _get("Total"),
            "Services": _get("Services"),
            "Retail":   _get("Retail"),
            "_extra_deposit": extra_deposit,
        })

    df_raw = pd.DataFrame(data_rows)

    # ── Stylist fill-down ────────────────────────────────────────
    # Rows where Client is null but Date is a string = stylist name row
    df_raw["Stylist"] = df_raw.apply(
        lambda r: r["Date"]
        if (r["Client"] is None and r["Date"] is not None and isinstance(r["Date"], str))
        else None,
        axis=1,
    )
    df_raw["Stylist"] = df_raw["Stylist"].ffill()

    # Keep only client rows
    df = df_raw[df_raw["Client"].notna()].copy()

    # ── Type conversions ─────────────────────────────────────────
    df["Date"] = df["Date"].apply(lambda v: _excel_date(v) if isinstance(v, float) else v)
    for c in ["Cash", "Cards", "Other", "Total", "Services", "Retail"]:
        df[c] = df[c].apply(_to_currency)
    df["_extra_deposit"] = pd.to_numeric(df["_extra_deposit"], errors="coerce").fillna(0.0)

    # Report which extra columns were found (shown in UI)
    extra_col_names = []
    for idx in deposit_extra_indices:
        if idx < len(raw_headers) and raw_headers[idx]:
            extra_col_names.append(raw_headers[idx])
    df.attrs["extra_deposit_cols"] = extra_col_names

    return df[["Stylist", "Date", "Client", "Cash", "Cards", "Other",
               "Total", "Services", "Retail", "_extra_deposit"]].reset_index(drop=True)


# ─────────────────────────────────────────────
#  Join + derived columns
# ─────────────────────────────────────────────
def build_output(df_main: pd.DataFrame, df_report: pd.DataFrame,
                 cash_rate: float = 0.30, deposit_rate: float = 0.70) -> pd.DataFrame:
    """
    cash_rate    : fraction of Cash1_Final charged as salon service fee
    deposit_rate : fraction of deposit ex-VAT returned as rebate
    """
    df_main   = df_main.copy()
    df_report = df_report.copy()

    df_main["_jdate"]     = pd.to_datetime(df_main["Date"],   errors="coerce").dt.date
    df_report["_jdate"]   = pd.to_datetime(df_report["Date"], errors="coerce").dt.date
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

    # Fold any extra deposit columns (gift cards, vouchers) into Deposits_1
    if "_extra_deposit" in merged.columns:
        merged["_extra_deposit"] = pd.to_numeric(merged["_extra_deposit"], errors="coerce").fillna(0.0)
        merged["Deposits_1"] = merged["Deposits_1"] + merged["_extra_deposit"]

    merged["Cash1_Final"]  = ((merged["Cash1_1"] + merged["Cash_1"]) - merged["Retail"]).round(2)
    merged["Svc_to_Salon"] = (merged["Cash1_Final"] * cash_rate).round(2)
    merged["Dep_Rebate"]   = ((merged["Deposits_1"] / 1.2) * deposit_rate).round(2)
    merged["Total_SIQ"]    = (merged["Cash1_Final"] + merged["Deposits_1"]).round(2)

    out = merged[["Stylist", "Date", "Client", "Cash1_Final", "Deposits_1",
                  "Svc_to_Salon", "Dep_Rebate", "Total_SIQ"]].copy()
    out = out.rename(columns={
        "Cash1_Final":  "Cash (Net of Retail)",
        "Deposits_1":   "Deposits",
        "Svc_to_Salon": "Services to Salon",
        "Dep_Rebate":   "Deposit Rebate",
        "Total_SIQ":    "Total SIQ",
    })
    return out.sort_values(["Stylist", "Date"]).reset_index(drop=True)



# ─────────────────────────────────────────────
#  PDF statement generator (reportlab)
# ─────────────────────────────────────────────
def _build_stylist_statement_pdf(stylist: str, rows: pd.DataFrame,
                                  date_from, date_to) -> bytes:
    """
    Returns a PDF as bytes for a single stylist client statement.
    rows: filtered df_out rows for this stylist.
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    Table, TableStyle, HRFlowable)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=20*mm, bottomMargin=20*mm,
    )

    styles = getSampleStyleSheet()
    heading = ParagraphStyle("heading", fontSize=16, fontName="Helvetica-Bold",
                              alignment=TA_CENTER, spaceAfter=4)
    subhead  = ParagraphStyle("subhead", fontSize=11, fontName="Helvetica",
                               alignment=TA_CENTER, spaceAfter=2, textColor=colors.HexColor("#4b5563"))
    stylist_style = ParagraphStyle("stylist", fontSize=13, fontName="Helvetica-Bold",
                                    alignment=TA_CENTER, spaceAfter=8,
                                    textColor=colors.HexColor("#1e3a5f"))

    story = []

    # Header
    story.append(Paragraph("Touche Hairdressing Purley", heading))
    story.append(Paragraph("Client Statement", subhead))

    # Date range
    def _fmt(d):
        if d is None: return "—"
        try: return d.strftime("%d %b %Y")
        except: return str(d)
    story.append(Paragraph(f"{_fmt(date_from)}  –  {_fmt(date_to)}", subhead))
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width="100%", thickness=1.5,
                              color=colors.HexColor("#1e3a5f")))
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(stylist, stylist_style))
    story.append(HRFlowable(width="100%", thickness=0.5,
                              color=colors.HexColor("#d1d5db")))
    story.append(Spacer(1, 5*mm))

    # Table
    col_headers = ["Date", "Client", "Cash (Net Retail)", "Deposits"]
    table_data  = [col_headers]

    total_cash = 0.0
    total_dep  = 0.0

    for _, row in rows.iterrows():
        d = row.get("Date")
        try:   d_str = d.strftime("%d/%m/%Y") if hasattr(d, "strftime") else str(d)
        except: d_str = str(d)

        cash = row.get("Cash (Net of Retail)", 0.0) or 0.0
        dep  = row.get("Deposits", 0.0) or 0.0
        total_cash += cash
        total_dep  += dep

        table_data.append([
            d_str,
            str(row.get("Client", "")),
            f"£{cash:,.2f}",
            f"£{dep:,.2f}",
        ])

    # Totals row
    table_data.append(["", "TOTAL", f"£{total_cash:,.2f}", f"£{total_dep:,.2f}"])

    col_widths = [28*mm, 75*mm, 38*mm, 32*mm]
    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)

    n_data = len(table_data)
    tbl.setStyle(TableStyle([
        # Header row
        ("BACKGROUND",  (0,0), (-1,0),  colors.HexColor("#1e3a5f")),
        ("TEXTCOLOR",   (0,0), (-1,0),  colors.white),
        ("FONTNAME",    (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",    (0,0), (-1,0),  9),
        ("ALIGN",       (0,0), (-1,0),  "CENTER"),
        ("BOTTOMPADDING",(0,0),(-1,0),  6),
        ("TOPPADDING",  (0,0), (-1,0),  6),
        # Data rows — alternating
        ("FONTNAME",    (0,1), (-1,-2), "Helvetica"),
        ("FONTSIZE",    (0,1), (-1,-2), 8.5),
        ("ROWBACKGROUNDS", (0,1), (-1,-2),
         [colors.white, colors.HexColor("#f3f4f6")]),
        ("ALIGN",       (2,1), (-1,-2), "RIGHT"),
        ("TOPPADDING",  (0,1), (-1,-2), 4),
        ("BOTTOMPADDING",(0,1),(-1,-2), 4),
        # Totals row
        ("BACKGROUND",  (0,-1), (-1,-1), colors.HexColor("#e8edf5")),
        ("FONTNAME",    (0,-1), (-1,-1), "Helvetica-Bold"),
        ("FONTSIZE",    (0,-1), (-1,-1), 9),
        ("ALIGN",       (2,-1), (-1,-1), "RIGHT"),
        ("TOPPADDING",  (0,-1), (-1,-1), 5),
        ("BOTTOMPADDING",(0,-1),(-1,-1), 5),
        ("LINEABOVE",   (0,-1), (-1,-1), 1, colors.HexColor("#1e3a5f")),
        # Grid
        ("GRID",        (0,0),  (-1,-1), 0.4, colors.HexColor("#d1d5db")),
        ("LINEBELOW",   (0,0),  (-1,0),  1.5, colors.HexColor("#1e3a5f")),
    ]))

    story.append(tbl)
    doc.build(story)
    buf.seek(0)
    return buf.read()


def _build_statements_zip(df_out: pd.DataFrame) -> bytes:
    """Build a zip of one PDF per stylist."""
    dates = pd.to_datetime(df_out["Date"], errors="coerce").dropna()
    date_from = dates.min().date() if len(dates) else None
    date_to   = dates.max().date() if len(dates) else None

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for stylist in sorted(df_out["Stylist"].dropna().unique()):
            rows = df_out[df_out["Stylist"] == stylist].copy()
            pdf_bytes = _build_stylist_statement_pdf(stylist, rows, date_from, date_to)
            safe_name = stylist.replace("/", "-").replace("\\", "-")
            zf.writestr(f"{safe_name} Statement.pdf", pdf_bytes)
    zip_buf.seek(0)
    return zip_buf.read()


# ─────────────────────────────────────────────
#  Sidebar — file upload only
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
st.markdown("---")

if till_audit_file is None or till_report_file is None:
    st.info("👈  Upload **TillAudit** and **Till Audit Report** in the sidebar to begin.", icon="📂")
    st.stop()

# ── Process files ──
with st.spinner("Reading and processing files…"):
    try:
        df_main   = process_till_audit(till_audit_file.read())
        df_report = process_till_audit_report(till_report_file.read())
    except Exception as e:
        st.error(f"❌  Error processing files: {e}")
        import traceback; st.code(traceback.format_exc())
        st.stop()

# Notify user if extra deposit columns were detected
_extra_cols = df_main.attrs.get("extra_deposit_cols", [])
if _extra_cols:
    st.info(
        f"ℹ️  Extra deposit column(s) detected and added to Deposits: "
        f"**{', '.join(_extra_cols)}**",
        icon="💳",
    )

all_stylists = sorted(df_main["Stylist"].dropna().unique())

# ═══════════════════════════════════════════════════════════════
#  SECTION 1 — Scenario Controls
# ═══════════════════════════════════════════════════════════════
st.markdown("### ⚙️ Scenario Controls")

sc1, sc2 = st.columns(2)
with sc1:
    cash_rate_pct = st.number_input(
        "Cash Rate — Services to Salon (%)",
        min_value=0.0, max_value=100.0, value=30.0, step=0.5,
        help="Percentage of Cash (Net of Retail) charged to salon. Default 30%.",
        key="cash_rate_pct",
    )
with sc2:
    deposit_rate_pct = st.number_input(
        "Deposit Rebate Rate (%)",
        min_value=0.0, max_value=100.0, value=70.0, step=0.5,
        help="Percentage of deposit ex-VAT (÷1.2) paid as rebate. Default 70%.",
        key="deposit_rate_pct",
    )

cash_rate    = cash_rate_pct    / 100.0
deposit_rate = deposit_rate_pct / 100.0

st.markdown("---")

# ═══════════════════════════════════════════════════════════════
#  SECTION 2 — Chair Rent
# ═══════════════════════════════════════════════════════════════
st.markdown("### 🪑 Chair Rent")

rent_col1, _ = st.columns([1, 3])
with rent_col1:
    daily_rate = st.number_input(
        "Daily Rate (£)", min_value=0.0, value=0.0, step=1.0,
        key="daily_rate",
    )

# Per-stylist days — 4 per row
st.markdown("**Days worked per stylist**")
cols_per_row = 4
rows_of_stylists = [all_stylists[i:i+cols_per_row] for i in range(0, len(all_stylists), cols_per_row)]

rent_days: dict[str, float] = {}
for row_group in rows_of_stylists:
    cols = st.columns(cols_per_row)
    for col, stylist in zip(cols, row_group):
        safe_key = f"rent_days_{stylist.replace(' ', '_').replace('/', '_')}"
        rent_days[stylist] = col.number_input(
            stylist, min_value=0.0, value=0.0, step=0.5,
            key=safe_key, label_visibility="visible",
        )

# Build chair rent lookup: stylist -> total rent
rent_lookup = {s: round(daily_rate * d, 2) for s, d in rent_days.items()}

st.markdown("---")

# ═══════════════════════════════════════════════════════════════
#  Build output with current rates
# ═══════════════════════════════════════════════════════════════
try:
    df_out = build_output(df_main, df_report, cash_rate=cash_rate, deposit_rate=deposit_rate)
except Exception as e:
    st.error(f"❌  Error building output: {e}")
    import traceback; st.code(traceback.format_exc())
    st.stop()

# ═══════════════════════════════════════════════════════════════
#  SECTION 3 — Summary KPIs
# ═══════════════════════════════════════════════════════════════
st.markdown("### 📊 Summary")

total_cash_net  = df_out["Cash (Net of Retail)"].sum()
total_deposits  = df_out["Deposits"].sum()
total_svc       = df_out["Services to Salon"].sum()
total_dep_reb   = df_out["Deposit Rebate"].sum()
total_chair     = sum(rent_lookup.values())
n_stylists      = df_out["Stylist"].nunique()
n_clients       = len(df_out)

k1,k2,k3,k4 = st.columns(4)
k1.metric("Transactions",        f"{n_clients:,}")
k2.metric("Stylists",            f"{n_stylists:,}")
k3.metric("Cash (Net Retail)",   f"£{total_cash_net:,.2f}")
k4.metric("Total Deposits",      f"£{total_deposits:,.2f}")

k5,k6,k7,k8 = st.columns(4)
k5.metric(f"Services to Salon ({cash_rate_pct:.1f}%)",     f"£{total_svc:,.2f}")
k6.metric(f"Deposit Rebate ({deposit_rate_pct:.1f}%)",     f"£{total_dep_reb:,.2f}")
k7.metric("Total Chair Rent",    f"£{total_chair:,.2f}")
total_salon = total_svc + total_chair - total_dep_reb
k8.metric("Net Salon Income",    f"£{total_salon:,.2f}")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════
#  SECTION 4 — Stylist Summary
#  Columns: Stylist | Transactions | Cash (Net Retail) | Deposits
#           | Services to Salon | Deposit Rebate | Chair Rent | Total
#  Total = ((Cash + Deposits) - (Services to Salon + Chair Rent)) - Deposit Rebate
# ═══════════════════════════════════════════════════════════════
st.markdown("### 👤 Stylist Summary")

summ = df_out.groupby("Stylist", as_index=False).agg(
    Transactions      = ("Client",               "count"),
    Cash_Net_Retail   = ("Cash (Net of Retail)",  "sum"),
    Deposits          = ("Deposits",               "sum"),
    Services_to_Salon = ("Services to Salon",      "sum"),
    Deposit_Rebate    = ("Deposit Rebate",          "sum"),
).round(2)

summ["Chair Rent"] = summ["Stylist"].map(rent_lookup).fillna(0.0)

summ["Total"] = (
    (summ["Cash_Net_Retail"] + summ["Deposits"])
    - (summ["Services_to_Salon"] + summ["Chair Rent"])
    - summ["Deposit_Rebate"]
).round(2)

summ.columns = [
    "Stylist", "Transactions",
    "Cash (Net Retail)", "Deposits",
    "Services to Salon", "Deposit Rebate",
    "Chair Rent", "Total",
]

currency_cols = ["Cash (Net Retail)", "Deposits", "Services to Salon",
                 "Deposit Rebate", "Chair Rent", "Total"]

def _colour_total(val):
    if isinstance(val, (int, float)):
        color = "#166534" if val >= 0 else "#991b1b"
        bg    = "#dcfce7"  if val >= 0 else "#fee2e2"
        return f"color:{color}; background-color:{bg}; font-weight:600;"
    return ""

styled_summ = (
    summ.style
    .format({c: "£{:,.2f}" for c in currency_cols})
    .applymap(_colour_total, subset=["Total"])
)

st.dataframe(
    styled_summ,
    use_container_width=True,
    hide_index=True,
    height=min(600, 60 + len(summ)*38),
)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════
#  SECTION 5 — Full Detail
# ═══════════════════════════════════════════════════════════════
st.markdown("### 📋 Full Detail")

fa, fb = st.columns(2)
with fa:
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

currency_detail = ["Cash (Net of Retail)", "Deposits", "Services to Salon", "Deposit Rebate", "Total SIQ"]
st.dataframe(
    filtered.style.format({c: "£{:,.2f}" for c in currency_detail if c in filtered.columns}),
    use_container_width=True,
    height=450,
)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════
#  SECTION 6 — Downloads
# ═══════════════════════════════════════════════════════════════
st.markdown("### ⬇️ Downloads")

dl1, dl2, dl3, dl4 = st.columns(4)

# Full output workbook
out1 = io.BytesIO()
with pd.ExcelWriter(out1, engine="openpyxl") as writer:
    df_out.to_excel(writer, index=False, sheet_name="Till Audit Output")
    summ.to_excel(writer, index=False, sheet_name="Stylist Summary")
    df_main.to_excel(writer, index=False, sheet_name="TillAudit (cleaned)")
    df_report.to_excel(writer, index=False, sheet_name="Till Report (cleaned)")
out1.seek(0)
with dl1:
    st.download_button(
        "📥 Full Output Workbook", data=out1,
        file_name="Till Audit Output.xlsx",
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
        "📥 Filtered View", data=out2,
        file_name="Till Audit Filtered.xlsx",
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
        "📥 Stylist Summary", data=out3,
        file_name="Till Audit Stylist Summary.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

# Stylist statements zip
zip_bytes = _build_statements_zip(df_out)
with dl4:
    st.download_button(
        "📄 Client Statements (ZIP)", data=zip_bytes,
        file_name="Touche Stylist Statements.zip",
        mime="application/zip",
        use_container_width=True,
    )
