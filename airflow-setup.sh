docker-compose run airflow-webserver airflow db init

docker-compose run airflow-webserver airflow users create \
  --username admin \
  --firstname John \
  --lastname Wee \
  --role Admin \
  --email john@example.com \
  --password admin
