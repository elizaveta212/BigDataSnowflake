SELECT 'customers' as table_name, COUNT(*) as row_count FROM dim_customers
UNION ALL
SELECT 'sellers', COUNT(*) FROM dim_sellers
UNION ALL
SELECT 'products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'product_categories', COUNT(*) FROM dim_product_categories
UNION ALL
SELECT 'suppliers', COUNT(*) FROM dim_suppliers
UNION ALL
SELECT 'stores', COUNT(*) FROM dim_stores
UNION ALL
SELECT 'pets', COUNT(*) FROM dim_pets
UNION ALL
SELECT 'sales', COUNT(*) FROM fact_sales;

SELECT 
    pc.category_name,
    COUNT(fs.sale_id) as total_sales,
    SUM(fs.quantity) as total_quantity,
    ROUND(SUM(fs.total_price)::numeric, 2) as total_revenue
FROM fact_sales fs
JOIN dim_products p ON fs.product_id = p.product_id
JOIN dim_product_categories pc ON p.category_id = pc.category_id
GROUP BY pc.category_name
ORDER BY total_revenue DESC;

SELECT 
    p.product_name,
    COUNT(fs.sale_id) as sale_count,
    SUM(fs.quantity) as total_quantity,
    ROUND(SUM(fs.total_price)::numeric, 2) as total_revenue
FROM fact_sales fs
JOIN dim_products p ON fs.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_quantity DESC
LIMIT 10;

SELECT 
    p.pet_type,
    p.pet_breed,
    COUNT(fs.sale_id) as sale_count,
    ROUND(SUM(fs.total_price)::numeric, 2) as total_revenue
FROM fact_sales fs
JOIN dim_pets p ON fs.pet_id = p.pet_id
GROUP BY p.pet_type, p.pet_breed
ORDER BY total_revenue DESC;

SELECT 
    s.store_name,
    s.city,
    s.country,
    COUNT(fs.sale_id) as sale_count,
    ROUND(SUM(fs.total_price)::numeric, 2) as total_revenue
FROM fact_sales fs
JOIN dim_stores s ON fs.store_id = s.store_id
GROUP BY s.store_name, s.city, s.country
ORDER BY total_revenue DESC
LIMIT 5;

SELECT 
    EXTRACT(MONTH FROM sale_date) as month,
    COUNT(sale_id) as sale_count,
    ROUND(SUM(total_price)::numeric, 2) as total_revenue
FROM fact_sales
GROUP BY EXTRACT(MONTH FROM sale_date)
ORDER BY month;

SELECT 
    c.country,
    COUNT(fs.sale_id) as sale_count,
    ROUND(AVG(fs.total_price)::numeric, 2) as avg_check
FROM fact_sales fs
JOIN dim_customers c ON fs.customer_id = c.customer_id
GROUP BY c.country
ORDER BY avg_check DESC; 
