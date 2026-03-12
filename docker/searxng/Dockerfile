FROM searxng/searxng:latest

COPY searxng/settings.yml /usr/local/share/searxng/settings.yml
COPY docker/searxng/entrypoint.sh /usr/local/bin/searxng-entrypoint.sh

RUN chmod +x /usr/local/bin/searxng-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/searxng-entrypoint.sh"]
