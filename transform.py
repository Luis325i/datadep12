from process.Extract import Extract
from process.Load import Load
from process.Transform import Transform
import pandas as pd
from prefect import flow

@flow(log_prints=True)
def transformacion():
	#Definiendo funciones
	extract = Extract()
	load = Load()
	transform = Transform()

	#Lectura capa landing

	print("Lectura capa landing")

	categories_df = extract.read_gcp('datadep', 'landing/categories')
	customer_df = extract.read_gcp('datadep', 'landing/customer')
	departments_df = extract.read_gcp('datadep', 'landing/deparments')
	order_items_df = extract.read_gcp('datadep', 'landing/order_items')
	orders_df = extract.read_gcp('datadep', 'landing/orders')
	products_df = extract.read_gcp('datadep', 'landing/products')

	print("Fin lectura capa landing")

	print("transformaciones")

	df_enunciado1 = transform.enunciado1(customer_df, orders_df, order_items_df)
	df_enunciado2 = transform.enunciado2(order_items_df,products_df,categories_df)
	df_enunciado3 = transform.enunciado3(customer_df, orders_df, order_items_df, products_df, categories_df)
	df_enunciado4 = transform.enunciado4(customer_df, orders_df, order_items_df, products_df, categories_df)

	#df_enunciado1.head()
	#df_enunciado2.head()
	#df_enunciado3.head()
	#df_enunciado4.head()

	print("fin transformaciones")

	print("Carga a capa gold")

	load.load_to_gcloud('datadep','gold/df_enunciado1',df_enunciado1)
	load.load_to_gcloud('datadep','gold/df_enunciado2',df_enunciado2)
	load.load_to_gcloud('datadep','gold/df_enunciado3',df_enunciado3)
	load.load_to_gcloud('datadep','gold/df_enunciado4',df_enunciado4)

	print("fin carga a capa gold")

	print("Carga a MySQL local")

	load.load_to_mysql('gold','enunciado1',df_enunciado1)
	load.load_to_mysql('gold','enunciado2',df_enunciado2)
	load.load_to_mysql('gold','enunciado3',df_enunciado3)
	load.load_to_mysql('gold','enunciado4',df_enunciado4)

	print("Fin carga a MySQL local")

if __name__ == "main":
    transformacion()