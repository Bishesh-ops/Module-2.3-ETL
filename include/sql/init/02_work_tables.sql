-- Tables the pipeline needs besides the Gold model. Safe to run again.
CREATE TABLE IF NOT EXISTS rej_row (          -- quarantine: every rejected row, with the reason
    run_date date NOT NULL, source varchar(40) NOT NULL, reject_cd varchar(30) NOT NULL,
    src_key varchar(300), amount numeric(18,2), insert_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS etl_run_log (      -- counts and reconciliation results per run date
    run_date date NOT NULL, subject varchar(80) NOT NULL, metric varchar(40) NOT NULL,
    value_num numeric(18,2), insert_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- Silver: typed, cleaned rows of the run date. reject_cd is NULL for good rows; action_cd says what Gold must do.
CREATE TABLE IF NOT EXISTS silver_item (
    run_date date NOT NULL, item_id varchar(500), item_nm varchar(500), item_desc varchar(500), uom_cd varchar(500),
    subclass_id varchar(500), subclass_nm varchar(500), src_row_no integer, reject_cd varchar(30), action_cd varchar(10)
);
CREATE TABLE IF NOT EXISTS silver_store (
    run_date date NOT NULL, store_id varchar(500), store_nm varchar(500), district_id varchar(500), district_nm varchar(500),
    currency_cd varchar(500), src_row_no integer, reject_cd varchar(30), action_cd varchar(10)
);
CREATE TABLE IF NOT EXISTS silver_sales (
    run_date date NOT NULL, sale_id varchar(500), sale_line_no integer, store_id varchar(500), item_id varchar(500),
    sale_qty numeric(18,4), sale_amt numeric(18,2), discount_amt numeric(18,2), tax_amt numeric(18,2),
    src_row_no integer, reject_cd varchar(30)
);
CREATE TABLE IF NOT EXISTS silver_inventory (
    run_date date NOT NULL, item_id varchar(500), store_id varchar(500), on_hand_qty numeric(18,4),
    on_order_qty numeric(18,4), reorder_point_qty numeric(18,4), src_row_no integer, reject_cd varchar(30), action_cd varchar(10)
);
