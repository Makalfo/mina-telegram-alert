docker run -d \
	 --restart=always \
	--name mina-alerts \
    -e GOOGLE_APPLICATION_CREDENTIALS="/bigquery-api.json" \
	--mount "type=bind,source=/git/mina-telegram-alert/bigquery-api.json,dst=/bigquery-api.json,readonly" \
	--mount "type=bind,source=/git/mina-telegram-alert/config.ini,dst=/mina-telegram-alert/config.ini" \
	makalfe/mina-telegram-alerts
