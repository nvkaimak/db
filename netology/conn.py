from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv(".env"))

DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")