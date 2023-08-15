import argparse
import duckdb
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-t", "--table_name", type=str, action="store",
                        choices=["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", 
                                 "customer_address", "customer_demographics", "date_dim", "dbgen_version", 
                                 "household_demographics", "income_band", "inventory", "item", "promotion", 
                                 "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", 
                                 "warehouse", "web_page", "web_returns", "web_sales", "web_site"],
                        help="indicate the name of table that needs to be converted to parquet")
    
    parser.add_argument("-d", "--data_folder", type=str, action="store", required=True,
                        help="indicate the data source folder for conversion such as dataset/tpcds/dat-sf1>")
    
    parser.add_argument("-f", "--output_format", type=str, action="store", required=True,
                        help="indicate the output data format", choices=["csv", "parquet"])
    
    parser.add_argument("-rgs", "--row_group_size", type=int, action="store",
                        help="indicate scale factor of the dataset, such as 100000")
    
    args = parser.parse_args()

    dat_file = args.dat_file
    data_folder = args.data_folder
    output_format = args.output_format
    row_group_size = args.row_group_size

    if output_format == "parquet" and row_group_size is None:
        raise ValueError("Please indicate row group size for parquet")

    if dat_file is not None:
        table_list = [dat_file]
    else:
        table_list = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", 
                      "customer_address", "customer_demographics", "date_dim", "dbgen_version", 
                      "household_demographics", "income_band", "inventory", "item", "promotion", 
                      "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", 
                      "warehouse", "web_page", "web_returns", "web_sales", "web_site"]

    call_center = """(
        cc_call_center_sk INTEGER, 
        cc_call_center_id VARCHAR, 
        cc_rec_start_date DATE, 
        cc_rec_end_date DATE, 
        cc_closed_date_sk INTEGER, 
        cc_open_date_sk INTEGER, 
        cc_name VARCHAR, 
        cc_class VARCHAR, 
        cc_employees INTEGER, 
        cc_sq_ft INTEGER, 
        cc_hours VARCHAR, 
        cc_manager VARCHAR, 
        cc_mkt_id INTEGER, 
        cc_mkt_class VARCHAR, 
        cc_mkt_desc VARCHAR,
        cc_market_manager VARCHAR, 
        cc_division INTEGER, 
        cc_division_name VARCHAR, 
        cc_company INTEGER, 
        cc_company_name VARCHAR,
        cc_street_number VARCHAR, 
        cc_street_name VARCHAR, 
        cc_street_type VARCHAR, 
        cc_suite_number VARCHAR, 
        cc_city VARCHAR, 
        cc_county VARCHAR, 
        cc_state VARCHAR, 
        cc_zip VARCHAR, 
        cc_country VARCHAR, 
        cc_gmt_offset DOUBLE, 
        cc_tax_percentage DOUBLE
        )"""

    catalog_page = """(
        cp_catalog_page_sk INTEGER, 
        cp_catalog_page_id VARCHAR, 
        cp_start_date_sk INTEGER, 
        cp_end_date_sk INTEGER, 
        cp_department VARCHAR, 
        cp_catalog_number INTEGER, 
        cp_catalog_page_number INTEGER, 
        cp_description VARCHAR, 
        cp_type VARCHAR
        )"""

    catalog_returns = """(
        cr_returned_date_sk INTEGER, 
        cr_returned_time_sk INTEGER, 
        cr_item_sk INTEGER, 
        cr_refunded_customer_sk INTEGER, 
        cr_refunded_cdemo_sk INTEGER, 
        cr_refunded_hdemo_sk INTEGER, 
        cr_refunded_addr_sk INTEGER, 
        cr_returning_customer_sk INTEGER, 
        cr_returning_cdemo_sk INTEGER,
        cr_returning_hdemo_sk INTEGER,
        cr_returning_addr_sk INTEGER,
        cr_call_center_sk INTEGER,
        cr_catalog_page_sk INTEGER,
        cr_ship_mode_sk INTEGER,
        cr_warehouse_sk INTEGER,
        cr_reason_sk INTEGER,
        cr_order_number INTEGER,
        cr_return_quantity INTEGER,
        cr_return_amount DOUBLE,
        cr_return_tax DOUBLE,
        cr_return_amt_inc_tax DOUBLE,
        cr_fee DOUBLE,
        cr_return_ship_cost DOUBLE,
        cr_refunded_cash DOUBLE,
        cr_reversed_charge DOUBLE,
        cr_store_credit DOUBLE,
        cr_net_loss DOUBLE
        )"""

    catalog_sales = """(
        cs_sold_date_sk INTEGER,
        cs_sold_time_sk INTEGER,
        cs_ship_date_sk INTEGER,
        cs_bill_customer_sk INTEGER,
        cs_bill_cdemo_sk INTEGER,
        cs_bill_hdemo_sk INTEGER,
        cs_bill_addr_sk INTEGER,
        cs_ship_customer_sk INTEGER,
        cs_ship_cdemo_sk INTEGER,
        cs_ship_hdemo_sk INTEGER,
        cs_ship_addr_sk INTEGER,
        cs_call_center_sk INTEGER,
        cs_catalog_page_sk INTEGER,
        cs_ship_mode_sk INTEGER,
        cs_warehouse_sk INTEGER,
        cs_item_sk INTEGER,
        cs_promo_sk INTEGER,
        cs_order_number INTEGER,
        cs_quantity INTEGER,
        cs_wholesale_cost DOUBLE,
        cs_list_price DOUBLE,
        cs_sales_price DOUBLE,
        cs_ext_discount_amt DOUBLE,
        cs_ext_sales_price DOUBLE,
        cs_ext_wholesale_cost DOUBLE,
        cs_ext_list_price DOUBLE,
        cs_ext_tax DOUBLE,
        cs_coupon_amt DOUBLE,
        cs_ext_ship_cost DOUBLE,
        cs_net_paid DOUBLE,
        cs_net_paid_inc_tax DOUBLE,
        cs_net_paid_inc_ship DOUBLE,
        cs_net_paid_inc_ship_tax DOUBLE,
        cs_net_profit DOUBLE
        )"""

    customer = """(
        c_customer_sk INTEGER,
        c_customer_id VARCHAR,
        c_current_cdemo_sk INTEGER,
        c_current_hdemo_sk INTEGER,
        c_current_addr_sk INTEGER,
        c_first_shipto_date_sk INTEGER,
        c_first_sales_date_sk INTEGER,
        c_salutation VARCHAR,
        c_first_name VARCHAR,
        c_last_name VARCHAR,
        c_preferred_cust_flag VARCHAR,
        c_birth_day INTEGER,
        c_birth_month INTEGER,
        c_birth_year INTEGER,
        c_birth_country VARCHAR,
        c_login VARCHAR,
        c_email_address VARCHAR,
        c_last_review_date VARCHAR
        )"""

    customer_address = """()""", 
    customer_demographics = """()"""
    date_dim = """()"""
    dbgen_version = """()""" 
    household_demographics = """()"""
    income_band = """()"""
    inventory = """()"""
    item = """()""" 
    promotion = """()""" 
    reason = """()"""
    ship_mode = """()""" 
    store = """()""" 
    store_returns = """()""" 
    store_sales = """()""" 
    time_dim = """()""" 
    warehouse = """()""" 
    web_page = """()""" 
    web_returns = """()""" 
    web_sales = """()"""
    web_site = """()"""

    db_conn = duckdb.connect(database=':memory:')

    for table in table_list:
        print(f"Convert {table}.tbl to parquet...")
        table_schema = table + "_schema"
        db_conn.execute(f"CREATE TABLE {table} {locals()[table_schema]};")
        db_conn.execute(f"COPY {table} FROM '{data_folder}/{table}.tbl' ( DELIMITER '|' );")
        if output_format == "parquet":
            db_conn.execute(f"COPY {table} TO '{table}.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE {row_group_size});")
            parquet_file = pq.ParquetFile(f"{table}.parquet")
            print("Number of row groups in {}.parquet: {}".format(table, parquet_file.num_row_groups))
        elif output_format == "csv":
            db_conn.execute(f"COPY {table} TO '{table}.csv' WITH (HEADER 1, DELIMITER ',');;")


if __name__ == "__main__":
    main()
