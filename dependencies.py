from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth as firebase_auth

auth_scheme = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    try:
        decoded_token = firebase_auth.verify_id_token(credentials.credentials)
        return decoded_token  # includes uid, email, etc.
    except Exception:
        raise HTTPException(status_code=401, detail="Unauthorized")
