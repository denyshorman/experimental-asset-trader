# Trading robot
Main trading application

## How to run the application

To run the application, environment variables or secrets have to be provided:

 - **DEPLOY_ENV** - Deploy environment dev/test/qa/prod.
 - **HTTP_API_PORT**** - Port to start HTTP API server.


 - **POLONIEX_API_KEY** - API key for Poloniex exchange
 - **POLONIEX_API_SECRET** - API secret for Poloniex exchange


 - **POSTGRES_HOST** - Postgres host
 - **POSTGRES_PORT** - Postgres port
 - **POSTGRES_USER** - Postgres user
 - **POSTGRES_PASSWORD** - Postgres password
 - **POSTGRES_DB** - Postgres database


 - **ENABLE_SOUND_SIGNAL** - Define this variable if you want to hear sound signal when trade is occurred


 - **HTTP_PROXY_HOST** - Proxy host
 - **HTTP_PROXY_PORT** - Proxy port
 - **HTTP_PROXY_TYPE** - Proxy type. Can be **http** or **socks5**
 - **HTTP_PROXY_ENABLED** - Define this variable if you want to enable proxy
 - **HTTP_CERT_TRUST_ALL** - Define this variable if you want to trust all certificates
