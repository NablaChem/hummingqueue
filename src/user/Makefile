devenv:
	@echo -e '# run this in your current shell\n'
	@echo 'HUMMING_QUEUE="user-311e6015-b0fb-4ad0-a5b6-54ba1addec37@api.lvh.me/project-0415bf24-f25b-43e3-8404-75d92e7410e7'
	@echo 'HUMMING_QUEUE_S3="access:password@s3.lvh.me/databucket"'
	@wget -q -nc https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc || true
	@chmod +x /tmp/mc
	@echo 'alias mc="/tmp/mc"'
	@echo 'mc alias set devenv "http://s3.lvh.me" root password'
	@echo 'mc admin user add devenv access password'
	@echo 'mc mb devenv databucket'

