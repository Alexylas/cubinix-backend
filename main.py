from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from firebase_admin import credentials, initialize_app
import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Initialize Firebase Admin
cred = credentials.Certificate(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
initialize_app(cred)

# Create FastAPI app
app = FastAPI()

# Dynamically load allowed origins from .env or Render dashboard
origins = os.getenv("FRONTEND_URL", "").split(",") + [
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]

# CORS Middleware - allow production + dev frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in origins if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("ALLOWED ORIGINS:", [o.strip() for o in origins if o.strip()])

# Import your routers
from routers import data
app.include_router(data.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to CubitAI backend!"}
