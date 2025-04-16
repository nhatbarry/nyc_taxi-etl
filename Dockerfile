docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="ny_taxi" \
-p 5432:5432 \
-v d:/de_bootcamp/ny_taxi_postgres_data:/var/lib/postgresql/data \
postgres:13