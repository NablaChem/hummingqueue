devserver:
	docker volume create dev-mongodb
	docker volume create dev-minio
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml -p dev up --build
