FROM grafana/grafana:latest

RUN grafana-cli plugins install grafana-clickhouse-datasource

EXPOSE 3000
ENTRYPOINT [ "/run.sh" ]



