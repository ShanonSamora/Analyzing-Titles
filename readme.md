Las variables de entorno necesarias para las credenciales de acceso deben estar configuradas en un .env antes de ejecutar el código del archivo s3_download.py. Esto fue hecho para no compremeter la seguridad de las credenciales. Las variables fueron definidas como ACCESS_KEY, SECRET_KEY, API_TMDB_KEY y DB_PASSWORD.

- s3_download.py descarga los archivos .csv iniciales.
- fill_missing_values.py contiene funciones para llenar campos en nulo de los dataframes trabajados en data_analysis.ipynb.
- load_data_to_postgres.py carga el archivo final (merge_titles.csv ubicado en la carpeta output) a la base de datos.