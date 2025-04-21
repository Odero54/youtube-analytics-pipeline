from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="postgres_youtube_analytics")
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT version();")
print(cursor.fetchone())
