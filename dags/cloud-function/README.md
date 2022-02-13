# External Trigger 

Untuk membuat external trigger, salah satu cara yang bisa kita gunakan adalah menggunakan Cloud Function di GCP atau Lambda jika di AWS. Pada dokumen ini kita akan menggunakan Cloud Function di GCP untuk men-trigger salah satu Airflow DAGs yang kita punya.

## Cloud Function

Google Cloud Functions merupakan sebuah model tanpa server untuk membangun aplikasi cloud. Cloud Functions bersifat serverless (Tanpa Server) seperti Google App Engine, namun Cloud Functions memiliki ukuran lebih kecil karena yang kita deploy adalah kode logika yang berada di dalam fungsi, bukan aplikasi seperti pada App Engine. 
Kita tidak perlu melakukan pengaturan terhadap infrastruktur atau server yang menjalankan Cloud Functions. Layanan seperti ini bisa disebut juga Function as a Service (FaaS).


## Ngrok

Jika kita menggunakan localhost untuk testing Airflow kita, kita perlu meng-ekspos endpoint kita melalui tunnel yang secure, salah satunya adalah [ngrok](https://ngrok.com/).
ngrok memungkinkan kita untuk dapat mengakses localhost dari suatu remote server. Install ngrok disini https://ngrok.com/download 
