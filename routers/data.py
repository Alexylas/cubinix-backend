from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from firebase_admin import firestore
from dependencies import get_current_user
import csv, io, os, gspread, openai
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import pandas as pd

router = APIRouter(prefix="/data", tags=["Data"], dependencies=[Depends(get_current_user)])

class GoogleSheetRequest(BaseModel):
    sheet_id: str

class AskQuestionRequest(BaseModel):
    question: str

@router.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    content = await file.read()
    try:
        decoded = content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(decoded))
        cleaned_data = []

        for row in reader:
            cleaned_row = {k: str(v) if v is not None else "" for k, v in row.items()}
            cleaned_data.append(cleaned_row)

        if not cleaned_data:
            raise HTTPException(status_code=400, detail="No valid rows found in CSV")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")

    db = firestore.client()
    db.collection("datasets").document(user["uid"]).set({"data": cleaned_data})
    return {"detail": "CSV uploaded successfully", "records": len(cleaned_data)}

@router.post("/import_google")
async def import_google(sheet: GoogleSheetRequest, user: dict = Depends(get_current_user)):
    try:
        gc = gspread.service_account(filename=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        sh = gc.open_by_key(sheet.sheet_id)
        data = sh.get_worksheet(0).get_all_records()
    except Exception:
        raise HTTPException(status_code=400, detail="Failed to fetch Google Sheet")

    db = firestore.client()
    db.collection("datasets").document(user["uid"]).set({"data": data})
    return {"detail": "Google Sheet imported", "records": len(data)}

@router.get("/fetch")
async def fetch_data(user: dict = Depends(get_current_user)):
    db = firestore.client()
    doc = db.collection("datasets").document(user["uid"]).get()
    if doc.exists:
        return {"data": doc.to_dict().get("data", [])}
    return {"data": []}

@router.get("/summary")
async def generate_summary(user: dict = Depends(get_current_user)):
    db = firestore.client()
    doc = db.collection("datasets").document(user["uid"]).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="No data found")

    data = doc.to_dict().get("data", [])
    if not data:
        raise HTTPException(status_code=404, detail="No data to summarize")

    text = "\n".join([str(row) for row in data])

    openai.api_key = os.getenv("OPENAI_API_KEY")
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful data analyst."},
            {"role": "user", "content": f"Summarize this dataset:\n{text}"}
        ]
    )

    summary = response["choices"][0]["message"]["content"]
    return {"summary": summary}

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

    try:
        for col in df.columns:
            if "how many" in question and "contain" in question and col.lower() in question:
                keyword = question.split("contain")[-1].strip().strip("'\" ")
                count = df[df[col].str.lower().str.contains(keyword, na=False)].shape[0]
                return {"answer": f"🔒 Logic result: There are {count} rows in '{col}' that contain '{keyword}'."}

            if "list all unique" in question and col.lower() in question:
                unique_values = df[col].dropna().unique()
                return {
                    "answer": f"🔒 Logic result: Unique values in '{col}': {', '.join(map(str, unique_values))}. Total: {len(unique_values)}"
                }
            if ("total" in question or "sum" in question) and col.lower() in question:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    total = df[col].sum()
                    return {"answer": f"🔒 Logic result: Total of '{col}': {total}"}
                except:
                    continue

            if "average" in question and col.lower() in question:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    avg = df[col].mean()
                    return {"answer": f"🔒 Logic result: Average of '{col}': {avg}"}
                except:
                    continue

        if "how many" in question and any(kw in question for kw in ["2023", "2024", "2022"]):
            for col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    year = int([y for y in ["2023", "2024", "2022"] if y in question][0])
                    count = df[df[col].dt.year == year].shape[0]
                    return {"answer": f"🔒 Logic result: Rows in year {year}: {count}"}
                except:
                    continue

        if "summary" in question:
            summary = df.describe(include='all').to_dict()
            return {"answer": f"🔒 Logic result: Basic dataset summary: {summary}"}

        # Fallback: Use AI with disclaimer
        context = "\n".join([str(row) for row in data[:500]])  # Limit rows for prompt length
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
        return {
            "answer": f"🤖 AI-predicted response:\n{ai_answer}"
        }

    except Exception as e:
        return {"answer": f"❌ Error occurred while processing your request: {str(e)}"}

@router.get("/export_csv")
async def export_csv(user: dict = Depends(get_current_user)):
    db = firestore.client()
    doc = db.collection("datasets").document(user["uid"]).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="No data to export")

    data = doc.to_dict().get("data", [])
    if not data:
        raise HTTPException(status_code=404, detail="No data available")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=data_export.csv"})

@router.get("/export_google")
async def export_google(user: dict = Depends(get_current_user)):
    try:
        db = firestore.client()
        doc = db.collection("datasets").document(user["uid"]).get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail="No data to export")

        data = doc.to_dict().get("data", [])
        if not data:
            raise HTTPException(status_code=404, detail="No data available")

        gc = gspread.service_account(filename=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        sh = gc.create(f"CubitAI Export - {user['email']}")
        sh.share(user["email"], perm_type='user', role='writer')

        worksheet = sh.sheet1

        headers = list(data[0].keys())
        values = [list(row.values()) for row in data]

        worksheet.update([headers] + values)

        return {"detail": f"Data exported successfully. Check your Google Sheets account ({user['email']})."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export to Google Sheets: {str(e)}")
