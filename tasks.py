from common import fetch_all_data, insert_products, clean_old_data, log_comparison_to_db

def run_scrape_job(job_name=""):
    products = fetch_all_data()
    insert_products(products)
    clean_old_data()
    log_comparison_to_db()
