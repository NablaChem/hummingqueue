devserver:
	docker volume create dev-mongodb
	docker volume create dev-minio
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml -p dev --env-file env.dev.yml up --build
