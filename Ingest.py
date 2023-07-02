from process.Extract import Extract
from process.Load import Load
import pandas as pd
from prefect import flow

@flow(log_prints=True)
def ingesta():
	load = Load()
	extract = Extract()

	print("Extraccion")

	categories_df = extract.read_mysql('retail_db', 'categories')
	customer_df = extract.read_mysql('retail_db', 'customer')
	departments_df = extract.read_mysql('retail_db', 'departments')
	order_items_df = extract.read_gcp('datadep', 'retail/Lmartinez/order_items')
	orders_df = extract.read_gcp('datadep', 'retail/Lmartinez/orders')
	products_df= extract.read_gcp('datadep', 'retail/Lmartinez/products')

	print("Fin extraccion")

	print("Carga landing")

	load.load_to_gcloud('datadep', 'landing/categories', categories_df)
	load.load_to_gcloud('datadep', 'landing/customer', customer_df)
	load.load_to_gcloud('datadep', 'landing/deparments', departments_df)
	load.load_to_gcloud('datadep', 'landing/order_items', order_items_df)
	load.load_to_gcloud('datadep', 'landing/orders', orders_df)
	load.load_to_gcloud('datadep', 'landing/products', products_df)

	print("Fin carga landing")

if __name__ == "main":
    ingesta()