devserver:
	docker volume create dev-mongodb
	docker volume create prod-redis
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml -p dev --env-file env.dev.yml up --build -d
prodserver:
	docker volume create prod-mongodb
	docker volume create prod-redis
	cp VERSION server/app/
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml -p prod --env-file env.prod.yml up --build -d
worker:
	cd computenode; PYTHONPATH="../user:${PYTHONPATH}" rq worker -c config -w hmq.CachedWorker --max-idle-time 60  --burst
director:
	cd computenode; PYTHONPATH="../user:${PYTHONPATH}" python director.py cluster-configs/localhost
pypi:
	cd user; sed -i "s/VERSION/$(cat ../VERSION)/" setup.py; rm dist/*; python setup.py sdist; twine upload dist/*; git checkout setup.py
