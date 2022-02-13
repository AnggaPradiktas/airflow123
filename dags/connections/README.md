## Create Airflow Connection

## Docker
Mounting /secret ke dalam docker-compose.yml

```
 - ./secret:/opt/airflow/secret
```

Jika menggunakan BigQuery, kita harus membuat akun Google Cloud Platform Free Tier terlebih dahulu (https://cloud.google.com/free/docs/gcp-free-tier), dapat mengikuti langkah berikut https://www.storagetutorials.com/create-free-trial-google-cloud-platform/


## BigQuery Service Account

Untuk dapat melakukan koneksi dengan akun GCP (BigQuery, GCS, dll) yang telah kita buat, kita dapat men-generate service account dengan mengikuti langkah berikut ini. https://github.com/alexvanboxel/airflow-gcp-examples


## Membuat Koneksi dari Airflow ke GCP

Untuk dapat terhubung dengan akun GCP yang telah kita buat, kita dapat mengikuti langkah berikut untuk dapat terhubung - https://cloud.google.com/composer/docs/how-to/managing/connections#creating_new_airflow_connections
