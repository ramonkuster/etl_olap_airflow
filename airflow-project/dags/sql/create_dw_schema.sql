CREATE TABLE IF NOT EXISTS public.dimdate (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    month_of_year INT NOT NULL,
    calendar_quarter INT NOT NULL,
    calendar_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dimproduct (
    product_key SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    product_number VARCHAR(25) NOT NULL,
    standard_cost NUMERIC(10, 2),
    list_price NUMERIC(10, 2),
    category_name VARCHAR(50),
    subcategory_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS public.dimcustomer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    territory_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(101),
    email_address VARCHAR(50),
    sales_person_id INT,
    gender VARCHAR(10),
    birth_date DATE
);

CREATE TABLE IF NOT EXISTS public.dimgeography (
    geography_key SERIAL PRIMARY KEY,
    territory_id INT,
    country_region_code CHAR(3),
    name VARCHAR(50),
    group_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS public.dimpromotion (
    promotion_key SERIAL PRIMARY KEY,
    promotion_id INT NOT NULL,
    description VARCHAR(255) NOT NULL,
    discount_pct NUMERIC(5, 2),
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS public.factsales (
    sales_order_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES public.dimdate(date_key),
    product_key INT NOT NULL REFERENCES public.dimproduct(product_key),
    customer_key INT NOT NULL REFERENCES public.dimcustomer(customer_key),
    geography_key INT NOT NULL REFERENCES public.dimgeography(geography_key),
    promotion_key INT NOT NULL REFERENCES public.dimpromotion(promotion_key),
    order_qty INT NOT NULL,
    unit_price NUMERIC(19, 4) NOT NULL,
    unit_price_discount NUMERIC(19, 4) NOT NULL,
    line_total NUMERIC(19, 4) NOT NULL,
    freight NUMERIC(19, 4),
    tax_amt NUMERIC(19, 4),
    sales_order_id INT NOT NULL,
    sales_order_detail_id INT NOT NULL,
    ship_date DATE,
    return_reason VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_factsales_date_key ON public.factsales(date_key);
CREATE INDEX IF NOT EXISTS idx_factsales_product_key ON public.factsales(product_key);
CREATE INDEX IF NOT EXISTS idx_factsales_customer_key ON public.factsales(customer_key);
