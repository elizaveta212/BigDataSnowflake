База данных использует схему "снежинка" со следующими таблицами:

### Таблицы измерений:
- dim_customers (Покупатели)
- dim_sellers (Продавцы)
- dim_product_categories (Категории продуктов)
- dim_products (Продукты)
- dim_suppliers (Поставщики)
- dim_stores (Магазины)
- dim_pets (Домашние животные)

### Таблица фактов:
- fact_sales (Продажи)

## Примеры запросов

1. Топ-10 самых продаваемых продуктов:
```sql
SELECT p.product_name, SUM(f.quantity) as total_sold
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sold DESC
LIMIT 10;
```

2. Средний чек по магазинам:
```sql
SELECT s.store_name, AVG(f.total_price) as avg_check
FROM fact_sales f
JOIN dim_stores s ON f.store_id = s.store_id
GROUP BY s.store_name
ORDER BY avg_check DESC;
```

3. Продажи по категориям продуктов:
```sql
SELECT pc.category_name, SUM(f.total_price) as total_sales
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
JOIN dim_product_categories pc ON p.category_id = pc.category_id
GROUP BY pc.category_name
ORDER BY total_sales DESC;
``` 
