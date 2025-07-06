import sqlalchemy
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

# === POSTGRESQL ===
DATABASE_URL = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres_poc:5432/{os.getenv('POSTGRES_DB')}"
engine = sqlalchemy.create_engine(DATABASE_URL)

# === AWS ===
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

BUCKET_NAME = "p12-sport-data-solution"
PREFIX_RH = "donnees-rh/"
PREFIX_SPORT = "donnees-sportives/"
PREFIX_CLEAN = "clean_data/"

# === GOOGLE MAP API ===
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# === SLACK ===
SLACK_BOT_TOKEN_COACH = os.getenv("SLACK_BOT_TOKEN_COACH")
SLACK_CHANNEL_ID_COACH = os.getenv("SLACK_CHANNEL_ID_COACH")