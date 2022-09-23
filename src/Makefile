devserver:
	docker volume create dev-mongodb
	docker volume create dev-minio
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml -p dev --env-file env.dev.yml up --build -d
testserver:
	docker exec dev_api_1 pytest --cov=./
testuser:
	docker build -t dev-user user
	docker run --network="host" dev-user
runuser:
	docker build -q -t dev-user user 
	docker run --network="host" --rm -w="/" dev-user hmq $(ARGS)
