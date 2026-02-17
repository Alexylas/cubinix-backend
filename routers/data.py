from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from firebase_admin import firestore
from dependencies import get_current_user
import csv, io, os, gspread, openai
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import pandas as pd
from services.sales_analytics import get_top_sales_reps

# -----------------------------
# Canonical Sales / CRM fields
# -----------------------------
CANONICAL_FIELDS = {
    "none": {
        "label": "— Not mapped —",
        "keywords": []
    },

    # Sales / CRM (highest business value)
    "sales_rep": {
        "label": "Sales Representative",
        "keywords": ["rep", "sales_rep", "agent", "salesperson", "owner"]
    },
    "customer_name": {
        "label": "Customer / Account Name",
        "keywords": ["customer", "client", "account", "company", "name"]
    },
    "deal_value": {
        "label": "Deal Value / Revenue",
        "keywords": ["amount", "value", "revenue", "price", "total"]
    },
    "deal_stage": {
        "label": "Deal Stage",
        "keywords": ["stage", "status", "pipeline", "won", "lost", "closed"]
    },
    "close_date": {
        "label": "Close Date",
        "keywords": ["close", "date", "signed", "order_date"]
    },

    # Location (future maps & geo analytics)
    "city": {
        "label": "City",
        "keywords": ["city", "town", "location"]
    },
    "region": {
        "label": "State / Province / Region",
        "keywords": ["state", "province", "region"]
    },
    "country": {
        "label": "Country",
        "keywords": ["country"]
    }
}


# ───────────────────────────────────────────────────────────
# Router WITHOUT global Depends – avoids 400 on CORS preflight
# ───────────────────────────────────────────────────────────
router = APIRouter(prefix="/data", tags=["Data"])

# ─────────────── Request models ───────────────
class GoogleSheetRequest(BaseModel):
    sheet_id: str

class AskQuestionRequest(BaseModel):
    question: str


# ───────────────────────────── CSV Upload ─────────────────────────────
@router.post("/upload_csv", dependencies=[Depends(get_current_user)])
async def upload_csv(
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
):
    content = await file.read()
    try:
        decoded = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(decoded))
        cleaned_data = [
            {k: str(v) if v is not None else "" for k, v in row.items()}
            for row in reader
        ]
        if not cleaned_data:
            raise HTTPException(400, "No valid rows found in CSV")
    except Exception as e:
        raise HTTPException(400, f"Invalid CSV format: {e}")

    firestore.client().collection("datasets").document(user["uid"]).set(
        {"data": cleaned_data}
    )
    return {"detail": "CSV uploaded successfully", "records": len(cleaned_data)}


# ───────────────────────── Google Sheet import ───────────────────────
@router.post("/import_google", dependencies=[Depends(get_current_user)])
async def import_google(
    sheet: GoogleSheetRequest, user: dict = Depends(get_current_user)
):
    try:
        gc = gspread.service_account(
            filename=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        )
        sh = gc.open_by_key(sheet.sheet_id)
        data = sh.get_worksheet(0).get_all_records()
    except Exception:
        raise HTTPException(400, "Failed to fetch Google Sheet")

    firestore.client().collection("datasets").document(user["uid"]).set({"data": data})
    return {"detail": "Google Sheet imported", "records": len(data)}


# ───────────────────────────── Fetch data ────────────────────────────
@router.get("/fetch", dependencies=[Depends(get_current_user)])
async def fetch_data(user: dict = Depends(get_current_user)):
    doc = (
        firestore.client()
        .collection("datasets")
        .document(user["uid"])
        .get()
    )
    if doc.exists:
        return {"data": doc.to_dict().get("data", [])}
    return {"data": []}


# ───────────────────────────── Summary ───────────────────────────────
@router.get("/summary", dependencies=[Depends(get_current_user)])
async def generate_summary(user: dict = Depends(get_current_user)):
    doc = (
        firestore.client()
        .collection("datasets")
        .document(user["uid"])
        .get()
    )
    if not doc.exists:
        raise HTTPException(404, "No data found")
    data = doc.to_dict().get("data", [])
    if not data:
        raise HTTPException(404, "No data to summarize")

    prompt_text = "\n".join(map(str, data))
    openai.api_key = os.getenv("OPENAI_API_KEY")
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful data analyst."},
            {
                "role": "user",
                "content": f"Summarize this dataset:\n{prompt_text}",
            },
        ],
    )
    return {"summary": response.choices[0].message["content"]}


# ─────────────────────── Ask-a-Question endpoint ─────────────────────
@router.post("/ask")
async def ask_question(request: AskQuestionRequest, user: dict = Depends(get_current_user)):
    db = firestore.client()
    doc = db.collection("datasets").document(user["uid"]).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="No data found for user")

    data = doc.to_dict().get("data", [])
    if not data:
        raise HTTPException(status_code=404, detail="No data available to query")

    df = pd.DataFrame(data)
    question = request.question.lower()

    # --- helpers ---
    def find_column(df_local, field_key: str):
        if "CANONICAL_FIELDS" not in globals():
            return None
        keywords = CANONICAL_FIELDS.get(field_key, {}).get("keywords", [])
        # exact match
        for c in df_local.columns:
            if c.lower() in [k.lower() for k in keywords]:
                return c
        # partial match
        for c in df_local.columns:
            for k in keywords:
                if k.lower() in c.lower():
                    return c
        return None

    try:
        # =========================
        # Existing logic you had
        # =========================
        for col in df.columns:
            if "how many" in question and "contain" in question and col.lower() in question:
                keyword = question.split("contain")[-1].strip().strip("'\" ")
                count = df[df[col].astype(str).str.lower().str.contains(keyword, na=False)].shape[0]
                return {"answer": f"🔒 Logic result: There are {count} rows in '{col}' that contain '{keyword}'."}

            if "list all unique" in question and col.lower() in question:
                unique_values = df[col].dropna().unique()
                return {"answer": f"🔒 Logic result: Unique values in '{col}': {', '.join(map(str, unique_values))}. Total: {len(unique_values)}"}

            if ("total" in question or "sum" in question) and col.lower() in question:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                total = df[col].sum()
                return {"answer": f"🔒 Logic result: Total of '{col}': {total}"}

            if "average" in question and col.lower() in question:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                avg = df[col].mean()
                return {"answer": f"🔒 Logic result: Average of '{col}': {avg}"}

        if "how many" in question and any(y in question for y in ["2022", "2023", "2024"]):
            for col in df.columns:
                dt = pd.to_datetime(df[col], errors="coerce")
                if dt.notna().sum() > 0:
                    year = int([y for y in ["2022", "2023", "2024"] if y in question][0])
                    count = df[dt.dt.year == year].shape[0]
                    return {"answer": f"🔒 Logic result: Rows in year {year}: {count}"}

        if "summary" in question:
            summary = df.describe(include='all').to_dict()
            return {"answer": f"🔒 Logic result: Basic dataset summary: {summary}"}

        # =========================
        # NEW: Pipeline + Forecast logic
        # (works only if CANONICAL_FIELDS exists)
        # =========================
        amount_col = find_column(df, "deal_value")
        stage_col = find_column(df, "deal_stage")
        close_col = find_column(df, "close_date")

        if (("pipeline" in question) or ("forecast" in question) or ("total pipeline" in question)) and amount_col:
            df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
            total_amt = df[amount_col].fillna(0).sum()
            return {"answer": f"🔒 Logic result: Total pipeline amount from '{amount_col}' is {total_amt:,.2f}."}

        if ("by stage" in question or "per stage" in question) and amount_col and stage_col:
            df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
            grouped = df.groupby(stage_col)[amount_col].sum().sort_values(ascending=False)
            lines = [f"{idx}: {val:,.2f}" for idx, val in grouped.items()]
            return {"answer": "🔒 Logic result: Pipeline amount by stage:\n" + "\n".join(lines)}

        if ("closing" in question or "close" in question) and ("this month" in question) and close_col:
            df[close_col] = pd.to_datetime(df[close_col], errors="coerce")
            now = pd.Timestamp.utcnow()
            count = df[(df[close_col].dt.year == now.year) & (df[close_col].dt.month == now.month)].shape[0]
            return {"answer": f"🔒 Logic result: {count} rows have '{close_col}' in this month."}
            
        # =========================
        # Close Rate Logic
        # =========================
        if ("close rate" in question or "win rate" in question or "conversion rate" in question):
            stage_col = find_column(df, "deal_stage")

            if not stage_col:
                return {"answer": "❌ Could not detect a Deal Stage column. Please map your stage field."}

            stages = df[stage_col].astype(str).str.lower()

            won = stages.str.contains("won").sum()
            lost = stages.str.contains("lost").sum()

            total_closed = won + lost

            if total_closed == 0:
                return {"answer": "🔒 Logic result: No closed deals found (won/lost)."}

            close_rate = (won / total_closed) * 100

            return {
                "answer": f"🔒 Logic result: Close rate is {close_rate:.2f}% ({won} won / {total_closed} closed deals)."
            }

        # =========================
        # Fallback: AI
        # =========================
        context = "\n".join([str(row) for row in data[:500]])
        prompt = f"Dataset:\n{context}\n\nUser Question:\n{request.question}"

        openai.api_key = os.getenv("OPENAI_API_KEY")
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a data assistant. Try your best to answer questions based on the dataset."},
                {"role": "user", "content": prompt}
            ]
        )
        ai_answer = response.choices[0].message['content']
        return {"answer": f"🤖 AI-predicted response:\n{ai_answer}"}

    except Exception as e:
        return {"answer": f"❌ Error occurred while processing your request: {str(e)}"}


# ───────────────────────────── CSV export ────────────────────────────
@router.get("/export_csv", dependencies=[Depends(get_current_user)])
async def export_csv(user: dict = Depends(get_current_user)):
    doc = (
        firestore.client()
        .collection("datasets")
        .document(user["uid"])
        .get()
    )
    if not doc.exists or not (data := doc.to_dict().get("data")):
        raise HTTPException(404, "No data to export")

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    out.seek(0)
    return StreamingResponse(
        out,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=data_export.csv"},
    )

# ───────────────────── Top Sales Reps Ranking ─────────────────────
@router.get("/top-sales-reps", dependencies=[Depends(get_current_user)])
async def top_sales_reps(user: dict = Depends(get_current_user)):
    db = firestore.client()
    doc = db.collection("datasets").document(user["uid"]).get()

    if not doc.exists:
        raise HTTPException(status_code=404, detail="No data found")

    records = doc.to_dict().get("data", [])
    if not records:
        return {"top_sales_reps": [], "note": "No records found."}

    # ✅ ADD THIS GUARD RIGHT HERE
    # Check whether the dataset even looks like CRM data
    sample_keys = set()
    for r in records[:25]:
        sample_keys.update([str(k).lower() for k in r.keys()])

    rep_like = any(x in "".join(sample_keys) for x in ["sales_rep", "sales rep", "rep", "owner", "agent", "salesperson"])
    value_like = any(x in "".join(sample_keys) for x in ["deal_value", "deal value", "revenue", "amount", "price", "total"])

    if not rep_like or not value_like:
        return {
            "top_sales_reps": [],
            "note": "This feature requires a CRM/Sales dataset with Sales Rep/Owner + Deal Value/Revenue fields. Upload a CRM export or map your columns to canonical fields (sales_rep, deal_value).",
            "detected_headers_sample": sorted(list(sample_keys))[:20]
        }

    # ✅ THEN run the actual ranking
    top = get_top_sales_reps(records)

    if not top:
        return {
            "top_sales_reps": [],
            "note": "No usable Sales Rep + Deal Value pairs found. Check that mapped values exist and Deal Value is numeric."
        }

    return {"top_sales_reps": top}

# ───────────────────── Google Sheets export ───────────────────────────
@router.get("/export_google", dependencies=[Depends(get_current_user)])
async def export_google(user: dict = Depends(get_current_user)):
    doc = (
        firestore.client()
        .collection("datasets")
        .document(user["uid"])
        .get()
    )
    if not doc.exists or not (data := doc.to_dict().get("data")):
        raise HTTPException(404, "No data to export")

    try:
        gc = gspread.service_account(
            filename=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        )
        sh = gc.create(f"Cubinix Export - {user['email']}")
        sh.share(user["email"], perm_type="user", role="writer")

        ws = sh.sheet1
        ws.update([list(data[0].keys())] + [list(r.values()) for r in data])
        return {"detail": f"Data exported. Check Google Sheets ({user['email']})."}
    except Exception as e:
        raise HTTPException(500, f"Failed to export to Google Sheets: {e}")
