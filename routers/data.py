from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from firebase_admin import firestore
from dependencies import get_current_user
import csv, io, os, gspread, openai
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import pandas as pd

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
@router.post("/ask", dependencies=[Depends(get_current_user)])
async def ask_question(
    request: AskQuestionRequest, user: dict = Depends(get_current_user)
):
    doc = (
        firestore.client()
        .collection("datasets")
        .document(user["uid"])
        .get()
    )
    if not doc.exists:
        raise HTTPException(404, "No data found for user")

    data = doc.to_dict().get("data", [])
    if not data:
        raise HTTPException(404, "No data available to query")

    df = pd.DataFrame(data)
    q = request.question.lower()

    # ───── Logic rules ─────
    for col in df.columns:
        if "how many" in q and "contain" in q and col.lower() in q:
            kw = q.split("contain")[-1].strip(" '\"")
            count = df[df[col].str.lower().str.contains(kw, na=False)].shape[0]
            return {
                "answer": f"🔒 Logic result: {count} rows in '{col}' contain '{kw}'."
            }

        if "list all unique" in q and col.lower() in q:
            uniq = df[col].dropna().unique()
            return {
                "answer": f"🔒 Logic result: Unique values in '{col}': "
                f"{', '.join(map(str, uniq))} (Total {len(uniq)})"
            }

        if any(word in q for word in ["total", "sum"]) and col.lower() in q:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            return {"answer": f"🔒 Logic result: Total of '{col}': {df[col].sum()}"}

        if "average" in q and col.lower() in q:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            return {"answer": f"🔒 Logic result: Average of '{col}': {df[col].mean()}"}

    # Year filter
    if "how many" in q and any(y in q for y in ("2022", "2023", "2024")):
        year = int([y for y in ("2022", "2023", "2024") if y in q][0])
        for col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                count = df[df[col].dt.year == year].shape[0]
                return {"answer": f"🔒 Logic result: {count} rows in year {year}."}
            except Exception:
                pass

    # Dataset summary
    if "summary" in q:
        return {
            "answer": f"🔒 Logic result: {df.describe(include='all').to_dict()}"
        }

    # ───── Fallback to AI ─────
    context = "\n".join(map(str, data[:500]))
    prompt = f"Dataset:\n{context}\n\nUser Question:\n{request.question}"
    openai.api_key = os.getenv("OPENAI_API_KEY")
    ai = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system",
                "content": "You are a data assistant. Answer based on the dataset.",
            },
            {"role": "user", "content": prompt},
        ],
    )
    return {"answer": f"🤖 AI-predicted response:\n{ai.choices[0].message['content']}"}


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
