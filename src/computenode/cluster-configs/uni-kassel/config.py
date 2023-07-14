import hmq
import tunnel
tunnel.use_tunnel("141.51.204.30", "nablachem.org")


REDIS_HOST = 'redis.nablachem.org'
REDIS_PORT = '42424'
REDIS_SSL = True             # Default: False
REDIS_SSL_CA_CERTS = None    # Default: None
REDIS_SSL_CERT_REQS = None # only for debug
REDIS_DB = 1                 # Default: 0
