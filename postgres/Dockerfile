FROM postgres:latest
ENV POSTGRES_DB=northwind
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
COPY northwind.sql /docker-entrypoint-initdb.d/northwind.sql
COPY postgresql.conf /etc/postgresql/postgresql.conf
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
