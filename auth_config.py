from typing import Optional
from pydantic_settings import BaseSettings

class AuthSettings(BaseSettings):
    CONNECT_BASE_URL: str = "https://connect.bracu.ac.bd"
    CONNECT_LOGIN_URL: str = "https://connect.bracu.ac.bd/login"
    CONNECT_AUTH_INIT_URL: str = "https://connect.bracu.ac.bd/api/auth/init"
    CONNECT_USER_INFO_URL: str = "https://connect.bracu.ac.bd/api/adv/v1/advising/student/info"
    CONNECT_SCHEDULE_URL: str = "https://connect.bracu.ac.bd/api/adv/v1/advising/sections/student/{student_id}/schedules"
    FRONTEND_URL: str = "https://connapi.vercel.app"
    BACKEND_URL: str = "https://connapi.vercel.app"
    SECRET_PASSWORD: str = "default-not-used"

settings = AuthSettings() 