from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# Load environmental variables from .env
load_dotenv('/.env')

# Setup database using SQLAlchemy
DATABASE_URL = f"redshift+psycopg2://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PWD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Tables definition using SQLAlchemy
class PopulationData(Base):
    __tablename__ = 'population_data'
    id_event = Column(Integer, primary_key=True)
    females = Column(Integer)
    country = Column(String(250))
    age = Column(Integer)
    males = Column(Integer)
    year = Column(Integer)
    total = Column(Integer)
    timestamp = Column(DateTime, default=func.now())

class WeatherData(Base):
    __tablename__ = 'weather_data'
    id_event = Column(Integer, primary_key=True)
    date = Column(Date)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)
    wind_speed_10m_max = Column(Float)
    daylight_duration = Column(Float)
    apparent_temperature_max = Column(Float)
    apparent_temperature_min = Column(Float)
    precipitation_sum = Column(Float)
    timestamp = Column(DateTime, default=func.now())

class PopulationWeatherRelation(Base):
    __tablename__ = 'population_weather_relation'
    id_event = Column(Integer, primary_key=True)
    population_change = Column(Float)
    average_temperature_change = Column(Float)
    location = Column(String(100))
    year = Column(Integer)

def create_tables():
    Base.metadata.create_all(engine)

def drop_old_data():
    Base.metadata.drop_all(engine)

def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()