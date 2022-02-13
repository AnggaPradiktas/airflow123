# airflow-docker


## Running Airflow in Docker

Dokumen ini berisi panduan dasar untuk menginstal Airflow pada komputer Anda menggunakan Docker, airflow akan dijalankan pada CeleryExecutor. Dokumen ini diperuntukkan untuk pengguna Airflow tahap awal untuk menjadi lebih familiar terhadap database system Airflow.

## Sebelum Dimulai

Ikuti langkah-langkah ini untuk menginstal dependencies yang diperlukan.

- Instal [Docker Community Edition (CE)](https://docs.docker.com/engine/install/) di workstation Anda. Bergantung pada OS, kamu mungkin perlu mengonfigurasi instance Docker untuk menggunakan memori 4,00 GB agar semua container berjalan dengan benar. Silakan merujuk ke Resources section jika menggunakan [Docker for Windows](https://docs.docker.com/desktop/windows/) atau [Docker for Mac](https://docs.docker.com/desktop/mac/) untuk informasi selengkapnya.
- Instal [Docker Compose v1.29.1](https://docs.docker.com/compose/install/) atau yang lebih update di workstation kamu.

## docker-compose.yaml

Untuk men-deploy Airflow di Docker Compose, Anda harus mengunduh [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml). Atau menggunakan command curl berikut. Sebagai contoh: Buatlah folder airflow-docker pada komputermu, dan eksekusi command dibawah ini didalam direktori tersebut
```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.2/docker-compose.yaml'
```

File ini berisi beberapa definisi service seperti:

- **airflow-scheduler** - Scheduler unruk memantau semua tasks dan DAG, lalu menginisiasi worker instance ketika semua dependensinya terpenuhi.

- **airflow-webserver** - Server web tersedia di http://localhost:8080.

- **airflow-worker** - Worker yang menjalankan tugas yang diberikan oleh scheduler.

- **airflow-init** - Layanan inisialisasi airflow.

- **flower** - Aplikasi flower untuk memantau airflow environment. Ini tersedia di http://localhost:5555.

- **postgres** - Backend metadata dari airflow.

- **redis** - broker yang meneruskan pesan dari scheduler ke worker.

Semua services ini memungkinkan kamu untuk menjalankan Airflow dengan [CeleryExecutor](https://airflow.apache.org/docs/apache-airflow/stable/executor/celery.html).

Beberapa direktori dalam container telah di-mount, yang berarti bahwa isinya disinkronkan antara komputer dan container.
- ./dags - kamu dapat meletakkan file DAG di sini.
- ./logs - berisi log dari eksekusi worker dan scheduler.
- ./plugins - kamu dapat meletakkan plugin khusus Anda di sini.


## Initializing Environment

Sebelum memulai Airflow untuk pertama kalinya, kamu perlu menyiapkan environemt terlebih dahulu, yaitu membuat file, direktori, dan menginisialisasi database yang diperlukan.

Didalam folder yang telah dibuat sebelumnya diatas, eksekusi command dibawah ini.

```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## Initialize the database

Untuk semua sistem operasi, kamu perlu menjalankan melakikan migrasi database dan membuat akun pengguna pertama. Untuk melakukannya, lakukan command berikut.

```docker-compose up airflow-init```

Lalu akan muncul log seperti berikut ketika command berhasil dijalankan

```
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.2.2
start_airflow-init_1 exited with code 0
```

Akun yang dibuat memiliki login **airflow** dan password **airflow**.


## Running Airflow

Sekarang kamu dapat memulai semua service dengan command dibawah ini:

```docker-compose up```



Note: untuk meng-clone repo ini pada local computer, 
 ```
 1. cd ke folder installasi Airflow, misal airflow-docker/
 2. git init
 3. git checkout -b main
 4. git remote add origin https://github.com/AnggaPradiktas/airflow-docker.git
 5. git checkout --track origin/main
 6. git pull
 ```
 
 
 Github archive data - https://console.cloud.google.com/bigquery?project=githubarchive&page=project

