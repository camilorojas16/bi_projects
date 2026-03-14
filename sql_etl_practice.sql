-- ============================================================
-- SQL / ETL / MySQL — EJERCICIOS DE PREPARACIÓN CODERBYTE
-- ============================================================
-- Estructura: CONCEPTO → CASUÍSTICA → REQUERIMIENTO → SOLUCIÓN
-- Base de datos asumida:
--   customers (customer_id, name, email, country, signup_date)
--   orders (order_id, customer_id, order_date, status, channel)
--   order_items (item_id, order_id, product_id, quantity, unit_price)
--   products (product_id, product_name, category, cost_price)
--   employees (employee_id, name, department, salary, hire_date, manager_id)
--   web_events (event_id, user_id, event_type, page, event_timestamp)
--   inventory (product_id, warehouse, stock_qty, last_updated)
-- ============================================================


-- ************************************************************
-- BLOQUE 1: JOINS Y FILTRADO
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 1.1
-- CONCEPTO: INNER JOIN + filtrado con fechas
-- CASUÍSTICA: El equipo de marketing quiere saber qué clientes
--   hicieron compras en el último trimestre para una campaña
--   de retención.
-- REQUERIMIENTO: Lista los clientes que hicieron al menos una
--   orden completada en los últimos 90 días. Mostrar name,
--   email, order_date.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT c.name,
       c.email,
       o.order_date
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
  AND o.order_date >= DATE('now', '-90 days')
ORDER BY o.order_date DESC;


-- ------------------------------------------------------------
-- EJERCICIO 1.2
-- CONCEPTO: LEFT JOIN + IS NULL (Anti-join)
-- CASUÍSTICA: Finanzas necesita identificar productos en el
--   catálogo que nunca se han vendido para evaluar su descontinuación.
-- REQUERIMIENTO: Encuentra todos los productos que nunca han
--   aparecido en una orden. Mostrar product_name y category.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT p.product_name,
       p.category
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE oi.item_id IS NULL
ORDER BY p.category, p.product_name;


-- ------------------------------------------------------------
-- EJERCICIO 1.3
-- CONCEPTO: SELF JOIN
-- CASUÍSTICA: RRHH necesita un reporte que muestre cada
--   empleado junto con el nombre de su manager directo.
-- REQUERIMIENTO: Lista employee name, department, salary, y
--   manager name. Incluir empleados sin manager (CEO).
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT e.name AS employee_name,
       e.department,
       e.salary,
       m.name AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id
ORDER BY m.name, e.name;


-- ------------------------------------------------------------
-- EJERCICIO 1.4
-- CONCEPTO: MULTIPLE JOINS + aggregation
-- CASUÍSTICA: El director comercial quiere un dashboard que
--   muestre el revenue por canal y categoría de producto.
-- REQUERIMIENTO: Revenue total por channel y category, solo
--   órdenes completadas. Ordenar por channel y revenue DESC.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT o.channel,
       p.category,
       SUM(oi.quantity * oi.unit_price) AS revenue
FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
WHERE o.status = 'completed'
GROUP BY o.channel, p.category
ORDER BY o.channel, revenue DESC;


-- ************************************************************
-- BLOQUE 2: AGREGACIONES Y GROUP BY / HAVING
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 2.1
-- CONCEPTO: COUNT + GROUP BY + HAVING
-- CASUÍSTICA: El equipo de fraude quiere detectar clientes
--   con un número inusualmente alto de órdenes canceladas.
-- REQUERIMIENTO: Clientes con más de 3 órdenes canceladas.
--   Mostrar name, email, cantidad de cancelaciones.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT c.name,
       c.email,
       COUNT(*) AS cancelled_orders
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
WHERE o.status = 'cancelled'
GROUP BY c.customer_id, c.name, c.email
HAVING COUNT(*) > 3
ORDER BY cancelled_orders DESC;


-- ------------------------------------------------------------
-- EJERCICIO 2.2
-- CONCEPTO: Múltiples agregaciones en un solo query
-- CASUÍSTICA: Se necesita un resumen ejecutivo mensual
--   con métricas clave de ventas.
-- REQUERIMIENTO: Por cada mes, mostrar: total de órdenes,
--   órdenes completadas, revenue total, ticket promedio,
--   y número de clientes únicos.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT strftime('%Y-%m', o.order_date) AS month,
       COUNT(DISTINCT o.order_id) AS total_orders,
       COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END) AS completed_orders,
       SUM(CASE WHEN o.status = 'completed' THEN oi.quantity * oi.unit_price ELSE 0 END) AS revenue,
       ROUND(
           SUM(CASE WHEN o.status = 'completed' THEN oi.quantity * oi.unit_price ELSE 0 END) * 1.0
           / NULLIF(COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END), 0),
           2
       ) AS avg_ticket,
       COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY strftime('%Y-%m', o.order_date)
ORDER BY month;

-- NOTA MySQL: usar DATE_FORMAT(o.order_date, '%Y-%m') en lugar de strftime


-- ------------------------------------------------------------
-- EJERCICIO 2.3
-- CONCEPTO: HAVING con expresiones complejas
-- CASUÍSTICA: El equipo de pricing quiere identificar
--   categorías donde el margen promedio es menor al 20%.
-- REQUERIMIENTO: Categorías con margen promedio < 20%.
--   Margen = (unit_price - cost_price) / unit_price.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT p.category,
       ROUND(AVG((oi.unit_price - p.cost_price) * 1.0 / oi.unit_price) * 100, 2) AS avg_margin_pct
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category
HAVING AVG((oi.unit_price - p.cost_price) * 1.0 / oi.unit_price) < 0.20
ORDER BY avg_margin_pct ASC;


-- ************************************************************
-- BLOQUE 3: WINDOW FUNCTIONS
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 3.1
-- CONCEPTO: ROW_NUMBER() — Deduplicación
-- CASUÍSTICA: La tabla web_events tiene eventos duplicados.
--   Se necesita quedarse solo con el primer evento por usuario
--   y página.
-- REQUERIMIENTO: Para cada usuario y página, obtener solo el
--   primer evento (por timestamp). Mostrar user_id, page,
--   event_type, event_timestamp.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT user_id, page, event_type, event_timestamp
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY user_id, page
               ORDER BY event_timestamp ASC
           ) AS rn
    FROM web_events
) sub
WHERE rn = 1;


-- ------------------------------------------------------------
-- EJERCICIO 3.2
-- CONCEPTO: RANK() — Top N por grupo
-- CASUÍSTICA: El equipo de ventas quiere premiar a los 3
--   clientes que más gastan por cada país.
-- REQUERIMIENTO: Top 3 clientes por revenue en cada país.
--   Mostrar country, name, revenue, rank.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT country, name, revenue, rnk
FROM (
    SELECT c.country,
           c.name,
           SUM(oi.quantity * oi.unit_price) AS revenue,
           RANK() OVER (
               PARTITION BY c.country
               ORDER BY SUM(oi.quantity * oi.unit_price) DESC
           ) AS rnk
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY c.country, c.customer_id, c.name
) sub
WHERE rnk <= 3
ORDER BY country, rnk;


-- ------------------------------------------------------------
-- EJERCICIO 3.3
-- CONCEPTO: LAG() — Comparación con periodo anterior
-- CASUÍSTICA: Se necesita comparar el revenue de cada mes
--   con el mes anterior para detectar caídas.
-- REQUERIMIENTO: Mostrar mes, revenue, revenue del mes
--   anterior, y el porcentaje de cambio MoM.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT month,
       revenue,
       prev_revenue,
       ROUND((revenue - prev_revenue) * 100.0 / prev_revenue, 2) AS mom_change_pct
FROM (
    SELECT strftime('%Y-%m', o.order_date) AS month,
           SUM(oi.quantity * oi.unit_price) AS revenue,
           LAG(SUM(oi.quantity * oi.unit_price)) OVER (ORDER BY strftime('%Y-%m', o.order_date)) AS prev_revenue
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY strftime('%Y-%m', o.order_date)
) sub
ORDER BY month;


-- ------------------------------------------------------------
-- EJERCICIO 3.4
-- CONCEPTO: SUM() OVER() — Running total + ROWS BETWEEN
-- CASUÍSTICA: Finanzas quiere ver el revenue acumulado del
--   año y también un promedio móvil de 3 meses.
-- REQUERIMIENTO: Por mes, mostrar revenue, running total
--   YTD, y promedio móvil de 3 meses.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT month,
       revenue,
       SUM(revenue) OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS ytd_revenue,
       ROUND(AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3m
FROM (
    SELECT strftime('%Y-%m', o.order_date) AS month,
           SUM(oi.quantity * oi.unit_price) AS revenue
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY strftime('%Y-%m', o.order_date)
) sub
ORDER BY month;


-- ------------------------------------------------------------
-- EJERCICIO 3.5
-- CONCEPTO: NTILE() — Segmentación en cuartiles
-- CASUÍSTICA: Marketing quiere segmentar clientes en 4
--   niveles según su gasto total para campañas diferenciadas.
-- REQUERIMIENTO: Asignar a cada cliente un cuartil (1-4) basado
--   en su revenue total. Mostrar name, revenue, cuartil.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT name,
       revenue,
       NTILE(4) OVER (ORDER BY revenue DESC) AS quartile
FROM (
    SELECT c.name,
           c.customer_id,
           SUM(oi.quantity * oi.unit_price) AS revenue
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY c.customer_id, c.name
) sub
ORDER BY revenue DESC;


-- ************************************************************
-- BLOQUE 4: SUBQUERIES Y CTEs
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 4.1
-- CONCEPTO: CTE + lógica de cohortes
-- CASUÍSTICA: El PM quiere analizar la retención: cuántos
--   clientes que compraron por primera vez en un mes dado
--   volvieron a comprar el mes siguiente.
-- REQUERIMIENTO: Para cada cohorte (mes de primera compra),
--   mostrar total de clientes nuevos y cuántos compraron
--   de nuevo dentro de los 30 días siguientes.
-- ------------------------------------------------------------

-- SOLUCIÓN:
WITH first_purchase AS (
    SELECT customer_id,
           MIN(order_date) AS first_order_date,
           strftime('%Y-%m', MIN(order_date)) AS cohort_month
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
),
repeat_purchase AS (
    SELECT fp.customer_id,
           fp.cohort_month
    FROM first_purchase fp
    INNER JOIN orders o ON fp.customer_id = o.customer_id
    WHERE o.status = 'completed'
      AND o.order_date > fp.first_order_date
      AND o.order_date <= DATE(fp.first_order_date, '+30 days')
)
SELECT fp.cohort_month,
       COUNT(DISTINCT fp.customer_id) AS new_customers,
       COUNT(DISTINCT rp.customer_id) AS retained_customers,
       ROUND(COUNT(DISTINCT rp.customer_id) * 100.0 / COUNT(DISTINCT fp.customer_id), 2) AS retention_rate
FROM first_purchase fp
LEFT JOIN repeat_purchase rp ON fp.customer_id = rp.customer_id
GROUP BY fp.cohort_month
ORDER BY fp.cohort_month;


-- ------------------------------------------------------------
-- EJERCICIO 4.2
-- CONCEPTO: Subquery correlacionada
-- CASUÍSTICA: Se quiere identificar órdenes cuyo revenue
--   está por encima del promedio de su mismo canal.
-- REQUERIMIENTO: Órdenes completadas cuyo revenue supera el
--   promedio del canal al que pertenecen.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT o.order_id,
       o.channel,
       order_revenue
FROM (
    SELECT o.order_id,
           o.channel,
           SUM(oi.quantity * oi.unit_price) AS order_revenue
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY o.order_id, o.channel
) o
WHERE order_revenue > (
    SELECT AVG(sub.rev)
    FROM (
        SELECT o2.order_id,
               SUM(oi2.quantity * oi2.unit_price) AS rev
        FROM orders o2
        INNER JOIN order_items oi2 ON o2.order_id = oi2.order_id
        WHERE o2.status = 'completed'
          AND o2.channel = o.channel
        GROUP BY o2.order_id
    ) sub
)
ORDER BY o.channel, order_revenue DESC;

-- ALTERNATIVA MÁS LIMPIA CON CTE:
WITH order_rev AS (
    SELECT o.order_id,
           o.channel,
           SUM(oi.quantity * oi.unit_price) AS revenue
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY o.order_id, o.channel
),
channel_avg AS (
    SELECT channel,
           AVG(revenue) AS avg_revenue
    FROM order_rev
    GROUP BY channel
)
SELECT r.order_id,
       r.channel,
       r.revenue,
       a.avg_revenue
FROM order_rev r
INNER JOIN channel_avg a ON r.channel = a.channel
WHERE r.revenue > a.avg_revenue
ORDER BY r.channel, r.revenue DESC;


-- ------------------------------------------------------------
-- EJERCICIO 4.3
-- CONCEPTO: CTE recursivo (MySQL 8+)
-- CASUÍSTICA: Se necesita mostrar la jerarquía organizacional
--   completa desde el CEO hasta cada empleado.
-- REQUERIMIENTO: Mostrar employee_name, manager_name, y el
--   nivel jerárquico (CEO = nivel 1).
-- ------------------------------------------------------------

-- SOLUCIÓN (MySQL 8+ / SQLite con recursive):
WITH RECURSIVE org_tree AS (
    -- Caso base: CEO (sin manager)
    SELECT employee_id,
           name,
           manager_id,
           1 AS level,
           name AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Caso recursivo
    SELECT e.employee_id,
           e.name,
           e.manager_id,
           ot.level + 1,
           ot.path || ' > ' || e.name
    FROM employees e
    INNER JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT name,
       level,
       path
FROM org_tree
ORDER BY path;


-- ************************************************************
-- BLOQUE 5: CASE WHEN + LÓGICA CONDICIONAL
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 5.1
-- CONCEPTO: CASE WHEN para segmentación
-- CASUÍSTICA: Comercial necesita clasificar clientes por
--   volumen de gasto para ofrecer beneficios diferenciados.
-- REQUERIMIENTO: Clasificar cada cliente como 'VIP' (>5000),
--   'Regular' (1000-5000), o 'Low' (<1000) según su revenue
--   total. Mostrar name, revenue, segment.
-- ------------------------------------------------------------

-- SOLUCIÓN:
SELECT c.name,
       SUM(oi.quantity * oi.unit_price) AS revenue,
       CASE
           WHEN SUM(oi.quantity * oi.unit_price) > 5000 THEN 'VIP'
           WHEN SUM(oi.quantity * oi.unit_price) >= 1000 THEN 'Regular'
           ELSE 'Low'
       END AS segment
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.name
ORDER BY revenue DESC;


-- ------------------------------------------------------------
-- EJERCICIO 5.2
-- CONCEPTO: Pivot con CASE WHEN
-- CASUÍSTICA: Se necesita ver el revenue mensual de cada
--   canal en columnas para un reporte ejecutivo.
-- REQUERIMIENTO: Crear una tabla pivoteada con channels como
--   filas y meses como columnas con el revenue correspondiente.
-- ------------------------------------------------------------

-- SOLUCIÓN (últimos 3 meses como ejemplo):
SELECT o.channel,
       SUM(CASE WHEN strftime('%Y-%m', o.order_date) = '2026-01' THEN oi.quantity * oi.unit_price ELSE 0 END) AS jan_2026,
       SUM(CASE WHEN strftime('%Y-%m', o.order_date) = '2026-02' THEN oi.quantity * oi.unit_price ELSE 0 END) AS feb_2026,
       SUM(CASE WHEN strftime('%Y-%m', o.order_date) = '2026-03' THEN oi.quantity * oi.unit_price ELSE 0 END) AS mar_2026
FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'completed'
  AND o.order_date >= '2026-01-01'
GROUP BY o.channel
ORDER BY o.channel;


-- ************************************************************
-- BLOQUE 6: MySQL ESPECÍFICO
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 6.1
-- CONCEPTO: Funciones de fecha MySQL
-- CASUÍSTICA: Se necesita un reporte de órdenes agrupadas
--   por día de la semana para optimizar staffing.
-- REQUERIMIENTO: Cantidad de órdenes y revenue por día de
--   la semana (Lunes a Domingo). Solo completadas.
-- ------------------------------------------------------------

-- SOLUCIÓN (MySQL):
-- SELECT DAYNAME(o.order_date) AS day_of_week,
--        DAYOFWEEK(o.order_date) AS day_num,
--        COUNT(DISTINCT o.order_id) AS total_orders,
--        SUM(oi.quantity * oi.unit_price) AS revenue
-- FROM orders o
-- INNER JOIN order_items oi ON o.order_id = oi.order_id
-- WHERE o.status = 'completed'
-- GROUP BY DAYNAME(o.order_date), DAYOFWEEK(o.order_date)
-- ORDER BY day_num;

-- NOTA: DAYOFWEEK() en MySQL: 1=Domingo, 2=Lunes, ..., 7=Sábado


-- ------------------------------------------------------------
-- EJERCICIO 6.2
-- CONCEPTO: GROUP_CONCAT (MySQL) / GROUP_CONCAT (SQLite)
-- CASUÍSTICA: El equipo de soporte quiere ver un resumen
--   rápido de los productos comprados por cada cliente.
-- REQUERIMIENTO: Por cada cliente, mostrar su nombre y una
--   lista separada por comas de las categorías distintas
--   que ha comprado.
-- ------------------------------------------------------------

-- SOLUCIÓN (MySQL):
-- SELECT c.name,
--        GROUP_CONCAT(DISTINCT p.category ORDER BY p.category SEPARATOR ', ') AS categories_purchased
-- FROM customers c
-- INNER JOIN orders o ON c.customer_id = o.customer_id
-- INNER JOIN order_items oi ON o.order_id = oi.order_id
-- INNER JOIN products p ON oi.product_id = p.product_id
-- WHERE o.status = 'completed'
-- GROUP BY c.customer_id, c.name
-- ORDER BY c.name;

-- SOLUCIÓN (SQLite):
SELECT c.name,
       GROUP_CONCAT(DISTINCT p.category) AS categories_purchased
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.name
ORDER BY c.name;


-- ------------------------------------------------------------
-- EJERCICIO 6.3
-- CONCEPTO: INDEX y EXPLAIN (MySQL)
-- CASUÍSTICA: Una query de reporte tarda demasiado.
--   Se necesita analizar y optimizar.
-- REQUERIMIENTO: Dado el query lento abajo, escribir los
--   índices que mejorarían su rendimiento y explicar por qué.
-- ------------------------------------------------------------

-- QUERY LENTO:
-- SELECT c.country, SUM(oi.quantity * oi.unit_price) AS revenue
-- FROM customers c
-- JOIN orders o ON c.customer_id = o.customer_id
-- JOIN order_items oi ON o.order_id = oi.order_id
-- WHERE o.status = 'completed'
--   AND o.order_date BETWEEN '2025-01-01' AND '2025-12-31'
-- GROUP BY c.country;

-- SOLUCIÓN — índices recomendados:
-- CREATE INDEX idx_orders_status_date ON orders(status, order_date);
-- CREATE INDEX idx_orders_customer_id ON orders(customer_id);
-- CREATE INDEX idx_order_items_order_id ON order_items(order_id);
-- CREATE INDEX idx_customers_customer_id_country ON customers(customer_id, country);

-- EXPLICACIÓN:
-- 1. idx_orders_status_date: Índice compuesto que cubre el WHERE (status + order_date).
--    El motor puede filtrar directamente por status='completed' y luego rangear por fecha.
-- 2. idx_orders_customer_id: Acelera el JOIN entre orders y customers.
-- 3. idx_order_items_order_id: Acelera el JOIN entre orders y order_items.
-- 4. idx_customers_customer_id_country: Covering index para customers,
--    evita acceder a la tabla porque ya tiene customer_id y country.
--
-- Para verificar: EXPLAIN SELECT ... (revisar que se usen los índices)


-- ************************************************************
-- BLOQUE 7: ETL CONCEPTUAL + SQL
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 7.1
-- CONCEPTO: Deduplicación en ETL
-- CASUÍSTICA: La tabla de staging tiene registros duplicados
--   porque el mismo archivo CSV se cargó dos veces.
--   Los duplicados tienen exactamente el mismo customer_id,
--   order_id y order_date.
-- REQUERIMIENTO: Crear una tabla limpia eliminando duplicados,
--   quedándose con el registro de menor item_id (el primero
--   insertado).
-- ------------------------------------------------------------

-- SOLUCIÓN:
CREATE TABLE order_items_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, product_id
               ORDER BY item_id ASC
           ) AS rn
    FROM order_items_staging
) sub
WHERE rn = 1;

-- En MySQL puedes usar DELETE para limpiar in-place:
-- DELETE oi FROM order_items_staging oi
-- INNER JOIN (
--     SELECT item_id,
--            ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY item_id) AS rn
--     FROM order_items_staging
-- ) ranked ON oi.item_id = ranked.item_id
-- WHERE ranked.rn > 1;


-- ------------------------------------------------------------
-- EJERCICIO 7.2
-- CONCEPTO: SCD Tipo 2 (Slowly Changing Dimension)
-- CASUÍSTICA: La tabla de productos puede cambiar de precio.
--   Se necesita mantener un historial de precios para poder
--   calcular revenue con el precio vigente al momento de la venta.
-- REQUERIMIENTO: Diseñar la tabla de dimensión con SCD Tipo 2
--   y escribir el query de merge/upsert para nuevos precios.
-- ------------------------------------------------------------

-- SOLUCIÓN — Estructura de la tabla:
-- CREATE TABLE dim_products (
--     surrogate_key INT AUTO_INCREMENT PRIMARY KEY,
--     product_id INT,
--     product_name VARCHAR(255),
--     category VARCHAR(100),
--     unit_price DECIMAL(10,2),
--     effective_date DATE,
--     end_date DATE,        -- NULL si es el registro vigente
--     is_current BOOLEAN
-- );

-- PASO 1: Cerrar el registro vigente del producto que cambió
-- UPDATE dim_products
-- SET end_date = CURRENT_DATE - INTERVAL 1 DAY,
--     is_current = FALSE
-- WHERE product_id = 101
--   AND is_current = TRUE;

-- PASO 2: Insertar el nuevo registro vigente
-- INSERT INTO dim_products (product_id, product_name, category, unit_price, effective_date, end_date, is_current)
-- VALUES (101, 'Widget Pro', 'Electronics', 29.99, CURRENT_DATE, NULL, TRUE);

-- Para calcular revenue histórico correcto:
-- SELECT o.order_id,
--        dp.product_name,
--        dp.unit_price AS price_at_sale,
--        oi.quantity,
--        oi.quantity * dp.unit_price AS revenue
-- FROM order_items oi
-- INNER JOIN orders o ON oi.order_id = o.order_id
-- INNER JOIN dim_products dp ON oi.product_id = dp.product_id
--     AND o.order_date BETWEEN dp.effective_date AND COALESCE(dp.end_date, '9999-12-31');


-- ------------------------------------------------------------
-- EJERCICIO 7.3
-- CONCEPTO: Data Quality checks en ETL
-- CASUÍSTICA: Después de una carga ETL, se necesitan validar
--   los datos antes de moverlos a producción.
-- REQUERIMIENTO: Escribir queries de validación que detecten:
--   a) Órdenes sin items
--   b) Items con cantidad o precio <= 0
--   c) Órdenes con order_date en el futuro
--   d) Clientes con email duplicado
-- ------------------------------------------------------------

-- SOLUCIÓN:

-- a) Órdenes sin items (integridad referencial)
SELECT o.order_id, o.order_date
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE oi.item_id IS NULL;

-- b) Items con valores inválidos
SELECT item_id, order_id, quantity, unit_price
FROM order_items
WHERE quantity <= 0 OR unit_price <= 0;

-- c) Órdenes con fecha futura
SELECT order_id, order_date
FROM orders
WHERE order_date > DATE('now');
-- MySQL: WHERE order_date > CURDATE()

-- d) Emails duplicados
SELECT email, COUNT(*) AS cnt
FROM customers
WHERE email IS NOT NULL
GROUP BY email
HAVING COUNT(*) > 1;


-- ------------------------------------------------------------
-- EJERCICIO 7.4
-- CONCEPTO: Incremental Load (carga incremental)
-- CASUÍSTICA: El data warehouse tiene una tabla fact_orders
--   que se actualiza diariamente. No se puede recargar toda
--   la tabla, solo los registros nuevos o modificados.
-- REQUERIMIENTO: Escribir el query de carga incremental usando
--   un campo last_modified. La última carga fue '2026-03-13'.
-- ------------------------------------------------------------

-- SOLUCIÓN:

-- Insertar registros nuevos (no existen en el DW)
INSERT INTO fact_orders (order_id, customer_id, order_date, status, channel, revenue)
SELECT o.order_id,
       o.customer_id,
       o.order_date,
       o.status,
       o.channel,
       SUM(oi.quantity * oi.unit_price) AS revenue
FROM staging_orders o
INNER JOIN staging_order_items oi ON o.order_id = oi.order_id
WHERE o.last_modified > '2026-03-13 00:00:00'
  AND o.order_id NOT IN (SELECT order_id FROM fact_orders)
GROUP BY o.order_id, o.customer_id, o.order_date, o.status, o.channel;

-- Actualizar registros existentes que cambiaron (ej: status update)
-- MySQL:
-- UPDATE fact_orders f
-- INNER JOIN (
--     SELECT o.order_id, o.status,
--            SUM(oi.quantity * oi.unit_price) AS revenue
--     FROM staging_orders o
--     INNER JOIN staging_order_items oi ON o.order_id = oi.order_id
--     WHERE o.last_modified > '2026-03-13 00:00:00'
--     GROUP BY o.order_id, o.status
-- ) s ON f.order_id = s.order_id
-- SET f.status = s.status,
--     f.revenue = s.revenue;


-- ************************************************************
-- BLOQUE 8: ESCENARIOS AVANZADOS (COMBINACIONES)
-- ************************************************************

-- ------------------------------------------------------------
-- EJERCICIO 8.1
-- CONCEPTO: Funnel analysis
-- CASUÍSTICA: Producto quiere entender el funnel de conversión
--   del sitio: visit → add_to_cart → checkout → purchase.
-- REQUERIMIENTO: Por cada etapa del funnel, mostrar cantidad
--   de usuarios únicos y el drop-off % respecto a la etapa
--   anterior.
-- ------------------------------------------------------------

-- SOLUCIÓN:
WITH funnel AS (
    SELECT 'visit' AS step, 1 AS step_order,
           COUNT(DISTINCT user_id) AS users
    FROM web_events WHERE event_type = 'visit'
    UNION ALL
    SELECT 'add_to_cart', 2,
           COUNT(DISTINCT user_id)
    FROM web_events WHERE event_type = 'add_to_cart'
    UNION ALL
    SELECT 'checkout', 3,
           COUNT(DISTINCT user_id)
    FROM web_events WHERE event_type = 'checkout'
    UNION ALL
    SELECT 'purchase', 4,
           COUNT(DISTINCT user_id)
    FROM web_events WHERE event_type = 'purchase'
)
SELECT step,
       users,
       LAG(users) OVER (ORDER BY step_order) AS prev_step_users,
       ROUND(users * 100.0 / LAG(users) OVER (ORDER BY step_order), 2) AS conversion_pct,
       ROUND((1.0 - users * 1.0 / LAG(users) OVER (ORDER BY step_order)) * 100, 2) AS dropoff_pct
FROM funnel
ORDER BY step_order;


-- ------------------------------------------------------------
-- EJERCICIO 8.2
-- CONCEPTO: Year-over-Year comparison
-- CASUÍSTICA: El CFO quiere comparar el revenue de cada mes
--   con el mismo mes del año anterior.
-- REQUERIMIENTO: Mostrar año, mes, revenue, revenue del mismo
--   mes el año anterior, y crecimiento YoY %.
-- ------------------------------------------------------------

-- SOLUCIÓN:
WITH monthly_rev AS (
    SELECT strftime('%Y', o.order_date) AS year,
           strftime('%m', o.order_date) AS month,
           SUM(oi.quantity * oi.unit_price) AS revenue
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status = 'completed'
    GROUP BY strftime('%Y', o.order_date), strftime('%m', o.order_date)
)
SELECT curr.year,
       curr.month,
       curr.revenue AS current_revenue,
       prev.revenue AS prev_year_revenue,
       ROUND((curr.revenue - prev.revenue) * 100.0 / prev.revenue, 2) AS yoy_growth_pct
FROM monthly_rev curr
LEFT JOIN monthly_rev prev
    ON curr.month = prev.month
    AND CAST(curr.year AS INT) = CAST(prev.year AS INT) + 1
ORDER BY curr.year, curr.month;


-- ------------------------------------------------------------
-- EJERCICIO 8.3
-- CONCEPTO: Gaps and Islands / Días consecutivos
-- CASUÍSTICA: Marketing quiere identificar clientes que
--   compraron durante 3 o más días consecutivos (super fans).
-- REQUERIMIENTO: Encontrar clientes con rachas de al menos
--   3 días consecutivos con órdenes completadas.
-- ------------------------------------------------------------

-- SOLUCIÓN:
WITH daily_orders AS (
    SELECT DISTINCT customer_id,
           DATE(order_date) AS order_day
    FROM orders
    WHERE status = 'completed'
),
islands AS (
    SELECT customer_id,
           order_day,
           DATE(order_day, '-' || ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_day) || ' days') AS grp
    FROM daily_orders
)
SELECT i.customer_id,
       c.name,
       MIN(i.order_day) AS streak_start,
       MAX(i.order_day) AS streak_end,
       COUNT(*) AS consecutive_days
FROM islands i
INNER JOIN customers c ON i.customer_id = c.customer_id
GROUP BY i.customer_id, c.name, i.grp
HAVING COUNT(*) >= 3
ORDER BY consecutive_days DESC;


-- ------------------------------------------------------------
-- EJERCICIO 8.4
-- CONCEPTO: Inventory turnover + stock alerts
-- CASUÍSTICA: Supply chain necesita saber qué productos en
--   cada warehouse se están quedando sin stock basándose en
--   la velocidad de venta promedio de los últimos 30 días.
-- REQUERIMIENTO: Calcular días de stock restante por producto
--   y warehouse. Alertar si quedan menos de 7 días.
-- ------------------------------------------------------------

-- SOLUCIÓN:
WITH daily_sales AS (
    SELECT oi.product_id,
           SUM(oi.quantity) * 1.0 / 30 AS avg_daily_sales
    FROM order_items oi
    INNER JOIN orders o ON oi.order_id = o.order_id
    WHERE o.status = 'completed'
      AND o.order_date >= DATE('now', '-30 days')
    GROUP BY oi.product_id
)
SELECT inv.warehouse,
       p.product_name,
       inv.stock_qty,
       ROUND(ds.avg_daily_sales, 2) AS avg_daily_sales,
       CASE
           WHEN ds.avg_daily_sales > 0
           THEN ROUND(inv.stock_qty / ds.avg_daily_sales, 1)
           ELSE NULL
       END AS days_of_stock,
       CASE
           WHEN ds.avg_daily_sales > 0
                AND inv.stock_qty / ds.avg_daily_sales < 7
           THEN 'ALERT: LOW STOCK'
           ELSE 'OK'
       END AS stock_status
FROM inventory inv
INNER JOIN products p ON inv.product_id = p.product_id
LEFT JOIN daily_sales ds ON inv.product_id = ds.product_id
ORDER BY days_of_stock ASC;


-- ************************************************************
-- BLOQUE 9: PREGUNTAS TEÓRICAS FRECUENTES
-- ************************************************************

-- ------------------------------------------------------------
-- P: ¿Cuál es la diferencia entre WHERE y HAVING?
-- R: WHERE filtra filas ANTES de GROUP BY (opera sobre datos crudos).
--    HAVING filtra grupos DESPUÉS de GROUP BY (opera sobre agregaciones).
--    No puedes usar funciones de agregación en WHERE.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: ¿Cuál es la diferencia entre RANK(), DENSE_RANK() y ROW_NUMBER()?
-- R: Con valores [100, 100, 90, 80]:
--    ROW_NUMBER: 1, 2, 3, 4 (siempre único, arbitrario en empates)
--    RANK:       1, 1, 3, 4 (empata y salta posiciones)
--    DENSE_RANK: 1, 1, 2, 3 (empata pero NO salta posiciones)
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: ¿Qué es un covering index?
-- R: Un índice que contiene TODAS las columnas que el query
--    necesita, evitando ir a la tabla. Ejemplo:
--    CREATE INDEX idx ON orders(status, order_date, customer_id);
--    El query solo necesita status, order_date y customer_id,
--    así que el motor lee todo del índice sin tocar la tabla.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: ¿Diferencia entre DELETE, TRUNCATE y DROP?
-- R: DELETE: borra filas (puede tener WHERE), se puede hacer rollback,
--    dispara triggers, lento con muchas filas.
--    TRUNCATE: borra TODAS las filas, resetea auto_increment,
--    NO dispara triggers, mucho más rápido.
--    DROP: elimina la tabla completa (estructura + datos).
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: ¿Qué tipos de JOIN existen?
-- R: INNER JOIN: solo filas con match en ambas tablas.
--    LEFT JOIN: todas las filas de la izquierda + match de la derecha (NULL si no hay).
--    RIGHT JOIN: todas las filas de la derecha + match de la izquierda.
--    FULL OUTER JOIN: todas las filas de ambas (MySQL no lo soporta nativamente).
--    CROSS JOIN: producto cartesiano (cada fila con cada fila).
--    SELF JOIN: una tabla contra sí misma.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: ¿Qué es un deadlock y cómo se previene?
-- R: Ocurre cuando dos transacciones se bloquean mutuamente
--    esperando locks que la otra tiene.
--    Prevención: siempre acceder tablas en el mismo orden,
--    mantener transacciones cortas, usar niveles de aislamiento
--    apropiados, usar SELECT ... FOR UPDATE con cuidado.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: Orden de ejecución de un query SQL:
-- R: 1. FROM / JOINs (determina las tablas)
--    2. WHERE (filtra filas)
--    3. GROUP BY (agrupa)
--    4. HAVING (filtra grupos)
--    5. SELECT (calcula expresiones y aliases)
--    6. DISTINCT (elimina duplicados)
--    7. ORDER BY (ordena)
--    8. LIMIT / OFFSET (pagina)
--    Por eso no puedes usar un alias del SELECT en el WHERE.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: ¿Cuál es la diferencia entre ETL y ELT?
-- R: ETL (Extract, Transform, Load):
--    - Transforma los datos ANTES de cargarlos al destino.
--    - Usa un servidor de transformación intermedio.
--    - Tradicional (Informatica, Talend, SSIS).
--
--    ELT (Extract, Load, Transform):
--    - Carga los datos CRUDOS al destino primero.
--    - Transforma DENTRO del data warehouse usando SQL.
--    - Moderno (dbt, BigQuery, Snowflake, Redshift).
--    - Aprovecha el poder de cómputo del DW.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- P: ¿Qué es una tabla de hechos vs dimensión?
-- R: Hecho (fact): contiene métricas numéricas y claves foráneas.
--    Ejemplo: fact_orders (order_id, customer_key, date_key, revenue, qty).
--    Dimensión (dim): contiene atributos descriptivos.
--    Ejemplo: dim_customer (customer_key, name, country, segment).
--    Esquema estrella: 1 fact rodeada de dims.
--    Esquema copo de nieve: dims normalizadas (dim → sub-dim).
-- ------------------------------------------------------------


-- ============================================================
-- FIN DE EJERCICIOS
-- ============================================================
-- RESUMEN:
-- Bloque 1: JOINs y filtrado (4 ejercicios)
-- Bloque 2: Agregaciones, GROUP BY, HAVING (3 ejercicios)
-- Bloque 3: Window Functions (5 ejercicios)
-- Bloque 4: Subqueries y CTEs (3 ejercicios)
-- Bloque 5: CASE WHEN (2 ejercicios)
-- Bloque 6: MySQL específico (3 ejercicios)
-- Bloque 7: ETL conceptual + SQL (4 ejercicios)
-- Bloque 8: Escenarios avanzados (4 ejercicios)
-- Bloque 9: Preguntas teóricas (9 preguntas)
-- TOTAL: 28 ejercicios + 9 preguntas teóricas
-- ============================================================
