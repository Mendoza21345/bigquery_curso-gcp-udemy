from google.cloud import bigquery

def ejecutar_consulta():
    # Configura tu proyecto y crea un cliente de BigQuery
    proyecto = "prueba-415603"  # Reemplaza con tu ID de proyecto
    client = bigquery.Client(project=proyecto)

    # Especifica tu consulta SQL
    consulta = """
        SELECT family, species, COUNT(*) AS cantidad
        FROM `bigquery-public-data.gbif.occurrences`
        WHERE family IS NOT NULL AND species IS NOT NULL
        GROUP BY family, species
        HAVING COUNT(*) > 0;
    """

    # Ejecuta la consulta
    resultados = client.query(consulta)

    # Imprime los resultados
    for fila in resultados:
        print(fila)

if __name__ == "__main__":
    ejecutar_consulta()
