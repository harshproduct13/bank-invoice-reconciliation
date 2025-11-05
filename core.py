import os, sqlite3, pdfplumber, base64, json, re
from rapidfuzz import fuzz
from openai import OpenAI

DB_PATH = "reconcile.db"
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS transactions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, description TEXT, amount REAL, type TEXT,
        need_invoice TEXT DEFAULT 'Yes', has_invoice TEXT DEFAULT 'Unmatched', invoice_number TEXT
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS invoices(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id TEXT, business_name TEXT, description TEXT, gstin TEXT,
        taxable_amount REAL, sgst_amount REAL, cgst_amount REAL, igst_amount REAL, total_amount REAL
    );""")
    conn.commit(); conn.close()

def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# -------- Bank PDF parsing ----------
def parse_bank_pdf_and_insert_all(pdf_path):
    import pdfplumber
    conn = _db(); c = conn.cursor()
    lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for p in pdf.pages:
            t = p.extract_text() or ""
            for ln in t.splitlines():
                ln = ln.strip()
                if ln: lines.append(ln)

    inserted = 0
    for ln in lines:
        parsed = parse_transaction_line_with_openai(ln)
        if not parsed: continue
        amt = parsed.get("amount") or 0.0
        if parsed.get("type","").lower() != "debit" and "debit" not in ln.lower() and "dr" not in ln.lower(): 
            continue
        c.execute("""
    INSERT INTO transactions(date, description, amount, type, need_invoice, has_invoice)
    VALUES (?, ?, ?, ?, ?, ?)
""", (
    parsed.get("date", ""),
    parsed.get("description", ln),
    amt,
    "Debit",
    "Yes",
    "Unmatched"))
        inserted += 1
    conn.commit(); conn.close()
    return inserted

def parse_transaction_line_with_openai(line):
    sys = "Extract JSON with keys: date, description, amount, type (Debit/Credit). Return only JSON."
    user = f"Transaction line: {line}"
    try:
        resp = client.responses.create(model="gpt-4o",
                                       input=[{"role":"system","content":sys},{"role":"user","content":user}],
                                       max_output_tokens=200)
        txt = resp.output[0].content[0].text
        m = re.search(r"\{.*\}", txt, re.DOTALL)
        if not m: return None
        js = json.loads(m.group(0))
        amt = float(str(js.get("amount",0)).replace(",",""))
        return {"date":js.get("date",""),"description":js.get("description",""),"amount":amt,"type":js.get("type","")}
    except Exception as e:
        print("tx parse err",e); return None

# -------- Invoice parsing ----------
def parse_invoice_file_with_openai(path):
    with open(path,"rb") as f: data = f.read()
    b64 = base64.b64encode(data).decode("utf-8")
    mime = "application/pdf" if path.lower().endswith(".pdf") else "image/png"
    url = f"data:{mime};base64,{b64}"
    sys = "Extract JSON: invoice_id, business_name, description, gstin, taxable_amount, sgst_amount, cgst_amount, igst_amount, total_amount."
    try:
        resp = client.responses.create(model="gpt-4o",
            input=[{"role":"system","content":sys},
                   {"role":"user","content":"Extract invoice data in JSON"},
                   {"role":"user","content":{"type":"input_image","image_url":url}}],
            max_output_tokens=400)
        txt = resp.output[0].content[0].text
        m = re.search(r"\{.*\}", txt, re.DOTALL)
        if not m: return None
        js = json.loads(m.group(0))
        def num(x): 
            try: return float(str(x).replace(",",""))
            except: return 0.0
        return {
            "invoice_id":js.get("invoice_id",""),
            "business_name":js.get("business_name",""),
            "description":js.get("description",""),
            "gstin":js.get("gstin",""),
            "taxable_amount":num(js.get("taxable_amount",0)),
            "sgst_amount":num(js.get("sgst_amount",0)),
            "cgst_amount":num(js.get("cgst_amount",0)),
            "igst_amount":num(js.get("igst_amount",0)),
            "total_amount":num(js.get("total_amount",0))
        }
    except Exception as e:
        print("inv parse err",e); return None

def insert_invoice(parsed):
    conn=_db(); c=conn.cursor()
    c.execute("""INSERT INTO invoices(invoice_id,business_name,description,gstin,taxable_amount,sgst_amount,cgst_amount,igst_amount,total_amount)
                 VALUES(?,?,?,?,?,?,?,?,?)""",
              (parsed["invoice_id"],parsed["business_name"],parsed["description"],parsed["gstin"],
               parsed["taxable_amount"],parsed["sgst_amount"],parsed["cgst_amount"],parsed["igst_amount"],parsed["total_amount"]))
    conn.commit(); conn.close()

def get_all_transactions():
    conn=_db(); rows=conn.execute("SELECT * FROM transactions").fetchall(); conn.close(); return rows

def get_all_invoices():
    conn=_db(); rows=conn.execute("SELECT * FROM invoices").fetchall(); conn.close(); return rows

def run_reconciliation(tolerance=0.5, fuzzy_threshold=65):
    conn=_db(); c=conn.cursor()
    txs=c.execute("SELECT * FROM transactions WHERE need_invoice='Yes' AND has_invoice='Unmatched'").fetchall()
    invs=c.execute("SELECT * FROM invoices").fetchall()
    matched=0
    for t in txs:
        for inv in invs:
            if abs(t["amount"]-(inv["total_amount"] or 0))<=tolerance:
                name=(inv["business_name"] or "").lower(); desc=(t["description"] or "").lower()
                if not name: continue
                if name in desc or fuzz.partial_ratio(name,desc)>=fuzzy_threshold:
                    c.execute("UPDATE transactions SET has_invoice='Matched',invoice_number=? WHERE id=?",
                              (inv["invoice_id"],t["id"]))
                    matched+=1; break
    conn.commit(); conn.close()
    return matched
