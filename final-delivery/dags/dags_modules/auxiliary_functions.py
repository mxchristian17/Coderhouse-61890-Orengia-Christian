from sqlalchemy import func
from sqlalchemy.orm import Session
from dags_modules.table_management import PopulationData

def get_max_year_population(session: Session):
    # Obtener el año máximo de la tabla PopulationData
    max_year_population = session.query(func.max(PopulationData.year)).scalar()
    return max_year_population if max_year_population is not None else 1990