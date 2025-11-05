import streamlit as st, os
from core import (
    init_db, parse_bank_pdf_and_insert_all,
    parse_invoice_file_with_openai, insert_invoice,
    get_all_transactions, get_all_invoices, run_reconciliation
)

os.makedirs("uploads", exist_ok=True)
init_db()

st.set_page_config(page_title="Bank ↔ Invoice Reconciliation", layout="wide")
st.title("🏦 Bank ↔ Invoice Reconciliation App")

page = st.sidebar.radio("Navigate", ["Upload Bank Statement", "Upload Invoice", "Transactions", "Invoices", "Reconcile"])

if page == "Upload Bank Statement":
    st.header("Upload Bank Statement PDF")
    pdf = st.file_uploader("Upload bank statement", type=["pdf"])
    if pdf:
        path = f"uploads/{pdf.name}"
        with open(path,"wb") as f: f.write(pdf.getbuffer())
        st.info("Parsing bank statement with pdfplumber + GPT...")
        count = parse_bank_pdf_and_insert_all(path)
        st.success(f"Inserted {count} debit transactions!")

elif page == "Upload Invoice":
    st.header("Upload Invoice (image/PDF)")
    inv = st.file_uploader("Upload invoice", type=["png","jpg","jpeg","pdf"])
    if inv:
        path = f"uploads/{inv.name}"
        with open(path,"wb") as f: f.write(inv.getbuffer())
        st.info("Parsing invoice with GPT...")
        parsed = parse_invoice_file_with_openai(path)
        if parsed:
            insert_invoice(parsed)
            st.success("Invoice parsed and saved!")
        else:
            st.error("Failed to parse invoice.")

elif page == "Transactions":
    st.header("Transactions Table")
    txs = get_all_transactions()
    if txs: st.dataframe(txs)
    else: st.info("No transactions yet.")

elif page == "Invoices":
    st.header("Invoices Table")
    invs = get_all_invoices()
    if invs: st.dataframe(invs)
    else: st.info("No invoices yet.")

elif page == "Reconcile":
    st.header("Reconciliation")
    if st.button("Run Reconciliation"):
        matched = run_reconciliation()
        st.success(f"Matched {matched} transactions.")
