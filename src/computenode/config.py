import hmq
REDIS_HOST = 'redis.nablachem.org'
REDIS_PORT = '443'
REDIS_PASSWORD = hmq.api._get_message_secret()
REDIS_SSL = True             # Default: False
REDIS_SSL_CA_CERTS = None    # Default: None
REDIS_SSL_CERT_REQS = None # only for debug
REDIS_DB = 1                 # Default: 0
