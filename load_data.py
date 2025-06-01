import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import glob
import os
import time

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "pet_store"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", " "),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5434")  
}

def connect_to_db():
    """Установка соединения с базой данных с повторными попытками"""
    max_retries = 5
    retry_delay = 5  

    for attempt in range(max_retries):
        try:
            print(f"Попытка подключения к базе данных {attempt + 1}/{max_retries}")
            conn = psycopg2.connect(**DB_PARAMS)
            print("Успешное подключение к базе данных")
            return conn
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")
            if attempt < max_retries - 1:
                print(f"Повторная попытка через {retry_delay} секунд")
                time.sleep(retry_delay)
            else:
                print("Достигнуто максимальное количество попыток подключения")
                return None

def load_csv_files():
    """Загрузка всех CSV файлов"""
    csv_files = sorted(glob.glob("/data/MOCK_DATA*.csv"))
    
    if not csv_files:
        raise Exception("CSV файлы не найдены в директории /data/")
    
    print(f"Найдено файлов для загрузки: {len(csv_files)}")
    
    df = pd.read_csv(csv_files[0])
    print(f"Загружен файл: {csv_files[0]}")
    print(f"Колонки в файле: {', '.join(df.columns)}")
    
    date_columns = ['product_release_date', 'product_expiry_date', 'sale_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
    
    all_data = {
        'customers': df[['customer_first_name', 'customer_last_name', 'customer_age', 
                        'customer_email', 'customer_country', 'customer_postal_code']],
        'pets': df[['customer_pet_type', 'customer_pet_breed']],
        'products': df[['product_name', 'product_category', 'product_price', 'product_weight',
                       'product_color', 'product_size', 'product_brand', 'product_material',
                       'product_description', 'product_rating', 'product_reviews',
                       'product_release_date', 'product_expiry_date',
                       'supplier_name', 'supplier_contact', 'supplier_email',
                       'supplier_phone', 'supplier_address', 'supplier_city',
                       'supplier_country']],
        'sales': df[['sale_date', 'customer_email', 'seller_email', 'product_name',
                     'store_email', 'customer_pet_type', 'customer_pet_breed',
                     'sale_quantity', 'sale_total_price',
                     'seller_first_name', 'seller_last_name', 'seller_country', 'seller_postal_code']],
        'stores': df[['store_name', 'store_location', 'store_city', 'store_state',
                      'store_country', 'store_phone', 'store_email']]
    }
    
    for file_path in csv_files[1:]:
        try:
            print(f"Загрузка файла: {file_path}")
            df = pd.read_csv(file_path)
            
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            
            for key in all_data:
                all_data[key] = pd.concat([all_data[key], df[all_data[key].columns]], ignore_index=True)
            
            print(f"Успешно загружен файл {file_path}")
        except Exception as e:
            print(f"Ошибка при загрузке файла {file_path}: {e}")
    
    for key in all_data:
        all_data[key] = all_data[key].drop_duplicates()
        print(f"Количество уникальных записей в {key}: {len(all_data[key])}")
    
    all_data['sellers'] = all_data['sales'][['seller_first_name', 'seller_last_name', 
                                            'seller_email', 'seller_country', 
                                            'seller_postal_code']].drop_duplicates()
    print(f"Количество уникальных записей в sellers: {len(all_data['sellers'])}")
    
    return all_data

def insert_dimension_data(conn, data):
    """Вставка данных в таблицы измерений"""
    cur = conn.cursor()
    
    try:
        if 'products' in data:
            unique_categories = data['products']['product_category'].unique()
            categories_data = [(cat,) for cat in unique_categories]
            execute_values(cur, 
                         "INSERT INTO dim_product_categories (category_name) VALUES %s ON CONFLICT (category_name) DO NOTHING RETURNING category_id",
                         categories_data)
            category_mapping = {cat: id for (id,), cat in zip(cur.fetchall(), unique_categories)}
        
        if 'customers' in data:
            customer_data = data['customers'][['customer_first_name', 'customer_last_name', 'customer_age', 
                                             'customer_email', 'customer_country', 'customer_postal_code']].drop_duplicates()
            execute_values(cur,
                         """INSERT INTO dim_customers 
                            (first_name, last_name, age, email, country, postal_code) 
                            VALUES %s ON CONFLICT (email) DO NOTHING""",
                         [tuple(row) for row in customer_data.values])
        
        if 'sellers' in data:
            seller_data = data['sellers'][['seller_first_name', 'seller_last_name', 'seller_email',
                                       'seller_country', 'seller_postal_code']].drop_duplicates()
            execute_values(cur,
                         """INSERT INTO dim_sellers 
                            (first_name, last_name, email, country, postal_code) 
                            VALUES %s ON CONFLICT (email) DO NOTHING""",
                         [tuple(row) for row in seller_data.values])
        
        if 'products' in data:
            supplier_data = data['products'][['supplier_name', 'supplier_contact', 'supplier_email',
                                            'supplier_phone', 'supplier_address', 'supplier_city',
                                            'supplier_country']].drop_duplicates()
            execute_values(cur,
                         """INSERT INTO dim_suppliers 
                            (supplier_name, contact_name, email, phone, address, city, country) 
                            VALUES %s ON CONFLICT (email) DO NOTHING""",
                         [tuple(row) for row in supplier_data.values])
        
        if 'stores' in data:
            store_data = data['stores'][['store_name', 'store_location', 'store_city', 'store_state',
                                       'store_country', 'store_phone', 'store_email']].drop_duplicates()
            execute_values(cur,
                         """INSERT INTO dim_stores 
                            (store_name, location, city, state, country, phone, email) 
                            VALUES %s ON CONFLICT (email) DO NOTHING""",
                         [tuple(row) for row in store_data.values])
        
        if 'pets' in data:
            pet_data = data['pets'][['customer_pet_type', 'customer_pet_breed']].drop_duplicates()
            execute_values(cur,
                         """INSERT INTO dim_pets 
                            (pet_type, pet_breed) 
                            VALUES %s ON CONFLICT (pet_type, pet_breed) DO NOTHING""",
                         [tuple(row) for row in pet_data.values])
        
        if 'products' in data:
            product_records = []
            for _, row in data['products'].iterrows():
                try:
                    product_records.append((
                        row['product_name'],
                        category_mapping.get(row['product_category']),
                        float(row['product_price']),
                        float(row['product_weight']),
                        row['product_color'],
                        row['product_size'],
                        row['product_brand'],
                        row['product_material'],
                        row['product_description'],
                        float(row['product_rating']),
                        int(row['product_reviews']),
                        row['product_release_date'],
                        row['product_expiry_date']
                    ))
                except (ValueError, TypeError) as e:
                    print(f"Пропущена запись продукта из-за ошибки: {e}")
                    continue
            
            if product_records:
                execute_values(cur,
                             """INSERT INTO dim_products 
                                (product_name, category_id, price, weight, color, size, brand,
                                 material, description, rating, reviews, release_date, expiry_date) 
                                VALUES %s ON CONFLICT (product_name) DO NOTHING""",
                             product_records)
        
        conn.commit()
        print("Данные успешно вставлены в таблицы измерений")
        
    except Exception as e:
        print(f"Ошибка при вставке данных в таблицы измерений: {e}")
        conn.rollback()
        raise

def insert_fact_data(conn, data):
    """Вставка данных в таблицу фактов"""
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT email, customer_id FROM dim_customers")
        customer_mapping = {email: id for email, id in cur.fetchall()}
        
        cur.execute("SELECT email, seller_id FROM dim_sellers")
        seller_mapping = {email: id for email, id in cur.fetchall()}
        
        cur.execute("SELECT product_name, product_id FROM dim_products")
        product_mapping = {name: id for name, id in cur.fetchall()}
        
        cur.execute("SELECT email, store_id FROM dim_stores")
        store_mapping = {email: id for email, id in cur.fetchall()}
        
        cur.execute("SELECT pet_type, pet_breed, pet_id FROM dim_pets")
        pet_mapping = {(type, breed): id for type, breed, id in cur.fetchall()}
        
        sales_records = []
        for _, row in data['sales'].iterrows():
            try:
                sales_records.append((
                    datetime.strptime(row['sale_date'], '%Y-%m-%d').date(),
                    customer_mapping[row['customer_email']],
                    seller_mapping[row['seller_email']],
                    product_mapping[row['product_name']],
                    store_mapping[row['store_email']],
                    pet_mapping[(row['customer_pet_type'], row['customer_pet_breed'])],
                    int(row['sale_quantity']),
                    float(row['sale_total_price'])
                ))
            except KeyError as e:
                print(f"Пропущена запись из-за отсутствия ключа: {e}")
                continue
        
        if sales_records:
            execute_values(cur,
                         """INSERT INTO fact_sales 
                            (sale_date, customer_id, seller_id, product_id, store_id,
                             pet_id, quantity, total_price) 
                            VALUES %s ON CONFLICT DO NOTHING""",
                         sales_records)
            
            conn.commit()
            print("Данные успешно вставлены в таблицу фактов")
        else:
            print("Нет данных для вставки в таблицу фактов")
        
    except Exception as e:
        print(f"Ошибка при вставке данных в таблицу фактов: {e}")
        conn.rollback()
        raise

def main():
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        print("Загрузка данных из CSV файлов...")
        data = load_csv_files()
        
        print("Вставка данных в таблицы измерений...")
        insert_dimension_data(conn, data)
        
        print("Вставка данных в таблицу фактов...")
        insert_fact_data(conn, data)
        
        print("Загрузка данных завершена успешно!")
        
        with open('/tmp/data_loaded', 'w') as f:
            f.write('success')
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main() 
