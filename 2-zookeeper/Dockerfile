FROM confluentinc/cp-zookeeper:7.5.1

HEALTHCHECK --interval=20s --timeout=5s --retries=5 \
  CMD echo ruok | nc localhost 2181 | grep imok || exit 1
