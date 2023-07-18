devserver:
	docker volume create dev-mongodb
	#docker volume create dev-minio
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml -p dev --env-file env.dev.yml up --build -d
prodserver:
	docker volume create prod-mongodb
	#docker volume create prod-minio
	docker volume create prod-redis
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml -p prod --env-file env.prod.yml up --build -d
testserver:
	docker exec dev_api_1 pytest --cov=./
testuser:
	docker build -t dev-user user
	docker run --network="host" dev-user
runuser:
	docker build -q -t dev-user user 
	docker run -t --network="host" --rm -w="/root" -v "$(shell pwd)/.devhome:/root/" dev-user hmq $(ARGS)
worker:
	cd computenode; PYTHONPATH="../user:${PYTHONPATH}" rq worker -c config -w worker.CachedWorker --max-idle-time 60 --quiet --burst
director:
	cd computenode; PYTHONPATH="../user:${PYTHONPATH}" python director.py cluster-configs/localhost
