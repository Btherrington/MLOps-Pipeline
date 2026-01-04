from sqlalchemy import create_engine, Column, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker



DATABASE_URL = 'postgresql+psycopg2://admin:password@db:5432/riot_data'
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class RawMatch(Base):
    __tablename__ = "raw_matches"
    match_id = Column(String, primary_key=True)
    data = Column(JSONB)
def init_db():
    Base.metadata.create_all(bind=engine)







