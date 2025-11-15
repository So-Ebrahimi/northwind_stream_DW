FROM confluentinc/cp-kafka:7.5.1

HEALTHCHECK --interval=30s --timeout=10s --retries=5 \
  CMD kafka-topics --bootstrap-server localhost:9092 --list || exit 1
