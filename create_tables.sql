CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    age INTEGER,
    email VARCHAR(100) UNIQUE NOT NULL,
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_product_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) UNIQUE NOT NULL,
    category_id INTEGER REFERENCES dim_product_categories(category_id),
    price DECIMAL(10,2),
    weight DECIMAL(10,2),
    color VARCHAR(50),
    size VARCHAR(20),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating DECIMAL(3,2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(100),
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    location VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_pets (
    pet_id SERIAL PRIMARY KEY,
    pet_type VARCHAR(50) NOT NULL,
    pet_breed VARCHAR(50) NOT NULL,
    UNIQUE (pet_type, pet_breed)
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    seller_id INTEGER REFERENCES dim_sellers(seller_id),
    product_id INTEGER REFERENCES dim_products(product_id),
    store_id INTEGER REFERENCES dim_stores(store_id),
    pet_id INTEGER REFERENCES dim_pets(pet_id),
    quantity INTEGER NOT NULL,
    total_price DECIMAL(10,2) NOT NULL
); 
