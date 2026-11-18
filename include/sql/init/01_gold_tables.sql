-- Gold tables (Assignment 2.1 model). Nothing is dropped, so it is safe to run again.


CREATE TABLE IF NOT EXISTS dim_department (
    department_key      integer AUTOINCREMENT,
    department_id       varchar(20)  NOT NULL,
    department_nm       varchar(100) NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_department PRIMARY KEY (department_key),
    CONSTRAINT uq_dim_department_id UNIQUE (department_id)
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key        integer AUTOINCREMENT,
    category_id         varchar(20)  NOT NULL,
    category_nm         varchar(100) NOT NULL,
    department_key      integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_category PRIMARY KEY (category_key),
    CONSTRAINT uq_dim_category_id_start UNIQUE (category_id, effective_start_dt),
    CONSTRAINT fk_dim_category_department FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
);

CREATE TABLE IF NOT EXISTS dim_subclass (
    subclass_key        integer AUTOINCREMENT,
    subclass_id         varchar(20)  NOT NULL,
    subclass_nm         varchar(100) NOT NULL,
    category_key        integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_subclass PRIMARY KEY (subclass_key),
    CONSTRAINT uq_dim_subclass_id_start UNIQUE (subclass_id, effective_start_dt),
    CONSTRAINT fk_dim_subclass_category FOREIGN KEY (category_key) REFERENCES dim_category(category_key)
);

CREATE TABLE IF NOT EXISTS dim_brand (
    brand_key           integer AUTOINCREMENT,
    brand_id            varchar(20)  NOT NULL,
    brand_nm            varchar(100) NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_brand PRIMARY KEY (brand_key),
    CONSTRAINT uq_dim_brand_id_start UNIQUE (brand_id, effective_start_dt)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key        integer AUTOINCREMENT,
    supplier_id         varchar(20)  NOT NULL,
    supplier_nm         varchar(100) NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_supplier PRIMARY KEY (supplier_key),
    CONSTRAINT uq_dim_supplier_id UNIQUE (supplier_id)
);

CREATE TABLE IF NOT EXISTS dim_item (
    item_key            integer AUTOINCREMENT,
    item_id             varchar(20)  NOT NULL,
    item_nm             varchar(100) NOT NULL,
    item_desc           varchar(500) NOT NULL,
    unit_of_measure_cd  varchar(10)  NOT NULL,
    subclass_key        integer      NOT NULL,
    brand_key           integer      NOT NULL,
    supplier_key        integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_item PRIMARY KEY (item_key),
    CONSTRAINT uq_dim_item_id_start UNIQUE (item_id, effective_start_dt),
    CONSTRAINT fk_dim_item_subclass FOREIGN KEY (subclass_key) REFERENCES dim_subclass(subclass_key),
    CONSTRAINT fk_dim_item_brand FOREIGN KEY (brand_key) REFERENCES dim_brand(brand_key),
    CONSTRAINT fk_dim_item_supplier FOREIGN KEY (supplier_key) REFERENCES dim_supplier(supplier_key)
);

CREATE TABLE IF NOT EXISTS dim_chain (
    chain_key           integer AUTOINCREMENT,
    chain_id            varchar(20)  NOT NULL,
    chain_nm            varchar(100) NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_chain PRIMARY KEY (chain_key),
    CONSTRAINT uq_dim_chain_id UNIQUE (chain_id)
);

CREATE TABLE IF NOT EXISTS dim_area (
    area_key            integer AUTOINCREMENT,
    area_id             varchar(20)  NOT NULL,
    area_nm             varchar(100) NOT NULL,
    chain_key           integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_area PRIMARY KEY (area_key),
    CONSTRAINT uq_dim_area_id UNIQUE (area_id),
    CONSTRAINT fk_dim_area_chain FOREIGN KEY (chain_key) REFERENCES dim_chain (chain_key)
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_key          integer AUTOINCREMENT,
    region_id           varchar(20)  NOT NULL,
    region_nm           varchar(100) NOT NULL,
    area_key         integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_region PRIMARY KEY (region_key),
    CONSTRAINT uq_dim_region_id UNIQUE (region_id),
    CONSTRAINT fk_dim_region_area FOREIGN KEY (area_key) REFERENCES dim_area(area_key)
);

CREATE TABLE IF NOT EXISTS dim_district (
    district_key        integer AUTOINCREMENT,
    district_id         varchar(20)  NOT NULL,
    district_nm         varchar(100) NOT NULL,
    region_key          integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_district PRIMARY KEY (district_key),
    CONSTRAINT uq_dim_district_id UNIQUE (district_id),
    CONSTRAINT fk_dim_district_region FOREIGN KEY (region_key) REFERENCES dim_region(region_key)
);

CREATE TABLE IF NOT EXISTS dim_locn_type (
    locn_type_key   integer AUTOINCREMENT,
    locn_type_id    varchar(20)  NOT NULL,
    locn_type_nm    varchar(100) NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_locn_type PRIMARY KEY (locn_type_key),
    CONSTRAINT uq_dim_locn_type_id UNIQUE (locn_type_id)
);

CREATE TABLE IF NOT EXISTS dim_locn (
    locn_key        integer AUTOINCREMENT,
    locn_id         varchar(20)  NOT NULL,
    locn_nm         varchar(100) NOT NULL,
    district_key        integer      NOT NULL,
    locn_type_key   integer      NOT NULL,
    currency_cd         varchar(3)   NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_locn PRIMARY KEY (locn_key),
    CONSTRAINT uq_dim_locn_id_start UNIQUE (locn_id, effective_start_dt),
    CONSTRAINT fk_dim_locn_district FOREIGN KEY (district_key) REFERENCES dim_district(district_key),
    CONSTRAINT fk_dim_locn_locn_type FOREIGN KEY (locn_type_key) REFERENCES dim_locn_type(locn_type_key)
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_key            integer AUTOINCREMENT,
    year_id             varchar(4)   NOT NULL,
    year_no             integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_year PRIMARY KEY (year_key),
    CONSTRAINT uq_dim_year_id UNIQUE (year_id),
    CONSTRAINT uq_dim_year_no UNIQUE (year_no)
);

CREATE TABLE IF NOT EXISTS dim_week (
    week_key            integer AUTOINCREMENT,
    week_id             varchar(8)   NOT NULL,
    week_no             integer      NOT NULL,
    week_start_dt       date         NOT NULL,
    week_end_dt         date         NOT NULL,
    year_key            integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_week PRIMARY KEY (week_key),
    CONSTRAINT uq_dim_week_id UNIQUE (week_id),
    CONSTRAINT fk_dim_week_year FOREIGN KEY (year_key) REFERENCES dim_year (year_key)
);

CREATE TABLE IF NOT EXISTS dim_month (
    month_key           integer AUTOINCREMENT,
    month_id            varchar(6)   NOT NULL,
    month_no            integer      NOT NULL,
    month_nm            varchar(10)  NOT NULL,
    quarter_no          integer      NOT NULL,
    year_key            integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_month PRIMARY KEY (month_key),
    CONSTRAINT uq_dim_month_id UNIQUE (month_id),
    CONSTRAINT fk_dim_month_year FOREIGN KEY (year_key) REFERENCES dim_year (year_key)
);

CREATE TABLE IF NOT EXISTS dim_day (
    day_key             integer AUTOINCREMENT,
    day_id              varchar(8)   NOT NULL,
    day_dt              date         NOT NULL,
    weekday_nm          varchar(10)  NOT NULL,
    weekend_flg         boolean      NOT NULL,
    week_key            integer      NOT NULL,
    month_key           integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_day PRIMARY KEY (day_key),
    CONSTRAINT uq_dim_day_id UNIQUE (day_id),
    CONSTRAINT uq_dim_day_dt UNIQUE (day_dt),
    CONSTRAINT fk_dim_day_week FOREIGN KEY (week_key) REFERENCES dim_week (week_key),
    CONSTRAINT fk_dim_day_month FOREIGN KEY (month_key) REFERENCES dim_month (month_key)
);

CREATE TABLE IF NOT EXISTS dim_customer_segment (
    customer_segment_key integer AUTOINCREMENT,
    customer_segment_id  varchar(20)  NOT NULL,
    customer_segment_nm  varchar(100) NOT NULL,
    insert_ts            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg           boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt   date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt     date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_customer_segment PRIMARY KEY (customer_segment_key),
    CONSTRAINT uq_dim_customer_segment_id UNIQUE (customer_segment_id)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        integer AUTOINCREMENT,
    customer_id         varchar(20)  NOT NULL,
    customer_nm         varchar(100) NOT NULL,
    customer_segment_key integer      NOT NULL,
    insert_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_flg          boolean      NOT NULL DEFAULT TRUE,
    effective_start_dt  date         NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date         NOT NULL DEFAULT DATE '9999-12-31',
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key),
    CONSTRAINT uq_dim_customer_id UNIQUE (customer_id),
    CONSTRAINT fk_dim_customer_customer_segment FOREIGN KEY (customer_segment_key) REFERENCES dim_customer_segment(customer_segment_key)
);

CREATE TABLE IF NOT EXISTS fct_sales (
    sale_id             varchar(20)   NOT NULL,
    sale_line_no        integer       NOT NULL,
    item_key            integer       NOT NULL,
    locn_key            integer       NOT NULL,
    day_key             integer       NOT NULL,
    customer_key        integer,
    sale_qty            integer       NOT NULL,
    sale_amt            numeric(18,2) NOT NULL,
    discount_amt        numeric(18,2) NOT NULL DEFAULT 0,
    tax_amt             numeric(18,2) NOT NULL DEFAULT 0,
    insert_ts           timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fct_sales PRIMARY KEY (sale_id, sale_line_no),
    CONSTRAINT fk_fct_sales_item     FOREIGN KEY (item_key)     REFERENCES dim_item (item_key),
    CONSTRAINT fk_fct_sales_locn     FOREIGN KEY (locn_key)     REFERENCES dim_locn (locn_key),
    CONSTRAINT fk_fct_sales_day      FOREIGN KEY (day_key)      REFERENCES dim_day (day_key),
    CONSTRAINT fk_fct_sales_customer FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key)
);

CREATE TABLE IF NOT EXISTS fct_inventory (
    item_key            integer       NOT NULL,
    locn_key            integer       NOT NULL,
    day_key             integer       NOT NULL,
    on_hand_qty         integer       NOT NULL,
    on_order_qty        integer       NOT NULL DEFAULT 0,
    reorder_point_qty   integer       NOT NULL DEFAULT 0,
    effective_start_dt  date          NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date          NOT NULL DEFAULT DATE '9999-12-31',
    insert_ts           timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fct_inventory PRIMARY KEY (item_key, locn_key, day_key),
    CONSTRAINT fk_fct_inventory_item FOREIGN KEY (item_key) REFERENCES dim_item (item_key),
    CONSTRAINT fk_fct_inventory_locn FOREIGN KEY (locn_key) REFERENCES dim_locn (locn_key),
    CONSTRAINT fk_fct_inventory_day  FOREIGN KEY (day_key)  REFERENCES dim_day (day_key)
);

CREATE TABLE IF NOT EXISTS fct_price (
    item_key            integer       NOT NULL,
    locn_key            integer       NOT NULL,
    day_key             integer       NOT NULL,
    price_amt           numeric(18,2) NOT NULL,
    regular_price_amt   numeric(18,2) NOT NULL,
    effective_start_dt  date          NOT NULL DEFAULT CURRENT_DATE,
    effective_end_dt    date          NOT NULL DEFAULT DATE '9999-12-31',
    insert_ts           timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts           timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fct_price PRIMARY KEY (item_key, locn_key, day_key),
    CONSTRAINT fk_fct_price_item FOREIGN KEY (item_key) REFERENCES dim_item (item_key),
    CONSTRAINT fk_fct_price_locn FOREIGN KEY (locn_key) REFERENCES dim_locn (locn_key),
    CONSTRAINT fk_fct_price_day  FOREIGN KEY (day_key)  REFERENCES dim_day (day_key)
);
-- SHOW IMPORTED KEYS IN TABLE fct_sales;
