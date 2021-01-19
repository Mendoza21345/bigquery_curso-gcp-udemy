#BigQuery almacena en caché los resultados de las consultas. 
#Como resultado, las consultas posteriores toman menos tiempo. 
#Es posible deshabilitar el almacenamiento en caché con opciones de consulta. 
#BigQuery también realiza un seguimiento de las estadísticas sobre consultas, 
#como la hora de creación, la hora de finalización y el total de bytes procesados.

#En este paso, deshabilitará el almacenamiento en caché y también mostrará 
#estadísticas sobre las consultas.

from google.cloud import bigquery

client = bigquery.Client()

query = """
    SELECT subject AS subject, COUNT(*) AS num_duplicates
    FROM bigquery-public-data.github_repos.commits
    GROUP BY subject
    ORDER BY num_duplicates
    DESC LIMIT 10
"""
job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
results = client.query(query, job_config=job_config)

for row in results:
    subject = row['subject']
    num_duplicates = row['num_duplicates']
    print(f'{subject:<20} | {num_duplicates:>9,}')

print('-'*60)
print(f'Created: {results.created}')
print(f'Ended:   {results.ended}')
print(f'Bytes:   {results.total_bytes_processed:,}')