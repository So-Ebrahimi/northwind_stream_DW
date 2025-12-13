/* ======================================
   DELETE FIRST (Safe FK Order)
====================================== */

-- ORDER DETAILS
DELETE FROM order_details WHERE order_id = 20001 AND product_id = 78;

-- ORDERS
DELETE FROM orders WHERE order_id = 20001;

-- EMPLOYEE TERRITORIES
DELETE FROM employee_territories WHERE employee_id = 10 AND territory_id = '98105';

-- EMPLOYEES
DELETE FROM employees WHERE employee_id = 10;

-- PRODUCTS
DELETE FROM products WHERE product_id = 78;

-- CUSTOMERS
DELETE FROM customers WHERE customer_id = 'TESTC';

-- SUPPLIERS
DELETE FROM suppliers WHERE supplier_id = 30;

-- SHIPPERS
DELETE FROM shippers WHERE shipper_id = 7;

-- CATEGORIES
DELETE FROM categories WHERE category_id = 9;

-- US STATES
DELETE FROM us_states WHERE state_id = 52;

-- TERRITORIES
DELETE FROM territories WHERE territory_id = '98105';

-- REGION
DELETE FROM region WHERE region_id = 5;



/* ======================================
   INSERT AFTER DELETE
====================================== */

-- REGION
INSERT INTO region (region_id, region_description) VALUES
  (5, 'test_region');

-- TERRITORIES
INSERT INTO territories (territory_id, territory_description, region_id) VALUES
  ('98105', 'test_territory', 5);

-- US STATES
INSERT INTO us_states (state_id, state_name, state_abbr, state_region) VALUES
  (52, 'test_state', 'TS', 'test_region');

-- CATEGORIES
INSERT INTO categories (category_id, category_name, description, picture) VALUES
  (9, 'test_category', 'test_description', 'test_picture');

-- SHIPPERS
INSERT INTO shippers (shipper_id, company_name, phone) VALUES
  (7, 'test_shipper', '(111) 111-1111');

-- SUPPLIERS
INSERT INTO suppliers (
  supplier_id, company_name, contact_name, contact_title, address, city, region,
  postal_code, country, phone, fax, homepage
) VALUES
  (30, 'test_supplier', 'test_contact_name', 'test_contact_title', 'test_address',
   'test_city', 'test_region', '11111', 'test_country', '(111) 111-1111', NULL, NULL);

-- EMPLOYEES
INSERT INTO employees (
  employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date,
  address, city, region, postal_code, country, home_phone, extension, photo, notes,
  reports_to, photo_path
) VALUES
  (10, 'test_l', 'test_f', 'test_t', 'test',
   '1980-05-12', '2015-04-01', 'test_address', 'test_city', 'test_region', '11111',
   'test_country', '(111) 111-1111', 'test', '\x', 'test_notes', 2, 'test');

-- EMPLOYEE TERRITORIES
INSERT INTO employee_territories (employee_id, territory_id) VALUES
  (10, '98105');

-- CUSTOMERS
INSERT INTO customers (
  customer_id, company_name, contact_name, contact_title, address, city, region,
  postal_code, country, phone, fax
) VALUES
  ('TESTC', 'test_company', 'test_contact', 'test_c', 'test_address',
   'test_city', 'test_region', '11111', 'test_country', '(111) 111-1111', NULL);

-- PRODUCTS
INSERT INTO products (
  product_id, product_name, supplier_id, category_id, quantity_per_unit,
  unit_price, units_in_stock, units_on_order, reorder_level, discontinued
) VALUES
  (78, 'test_product', 30, 9, '1 unit', 10.00, 50, 0, 10, 0);

-- ORDERS
INSERT INTO orders (
  order_id, customer_id, employee_id, order_date, required_date, shipped_date,
  ship_via, freight, ship_name, ship_address, ship_city, ship_region,
  ship_postal_code, ship_country
) VALUES
  (20001, 'TESTC', 10, '2024-01-12', '2024-01-20', '2024-01-15', 7, 25.50,
   'test_sh', 'test_address', 'test_city', 'test_region',
   '11111', 'test_country');

-- ORDER DETAILS
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES
  (20001, 78, 10.00, 5, 0.00);
