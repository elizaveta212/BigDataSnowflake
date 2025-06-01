import psycopg2
import pandas as pd
from tabulate import tabulate
import os

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "pet_store"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", " "),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5434")
}

def execute_query(query, description):
    """Выполнение запроса и вывод результатов"""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            df = pd.read_sql_query(query, conn)

            print(f"\n{description}")
            print("=" * 80)
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
            print("\n")
            
            return df
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None

def main():
    with open('analysis_queries.sql', 'r') as file:
        queries = file.read().split(';')
    
    descriptions = [
        "Количество записей в таблицах",
        "Анализ продаж по категориям продуктов",
        "Топ-10 самых продаваемых продуктов",
        "Анализ продаж по типам домашних животных",
        "Топ-5 магазинов по выручке",
        "Анализ сезонности продаж",
        "Средний чек по странам"
    ]
    
    for query, description in zip(queries, descriptions):
        if query.strip():
            execute_query(query, description)

if __name__ == "__main__":
    main() 
