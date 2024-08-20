image: .built
.built:
	docker build -t pyscfad:0.1.7 .
	touch .built
clean:
	docker rmi -f $$(docker images -q pyscfad:0.1.7)
	rm .built 2> /dev/null
example: .built
	docker run --rm -v ${CURDIR}/../:/wrk pyscfad:0.1.7 python /wrk/example.py
