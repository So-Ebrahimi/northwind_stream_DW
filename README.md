# Data-pipeline-for-northwind
docker network create ProjectHost
docker-compose -f ./postgres/docker-compose.yml up -d --buld
docker-compose -f ./kafka/docker-compose.yml up -d
docker-compose -f ./debezium/docker-compose.yml up -d
docker-compose -f ./clickhouse/docker-compose.yml up -d

INSERT INTO orders VALUES (11079, 'RATTC', 1, '1998-05-06', '1998-06-03', NULL, 2, 8.52999973, 'Rattlesnake Canyon Grocery', '2817 Milton Dr.', 'Albuquerque', 'NM', '87110', 'USA');
INSERT INTO order_details VALUES (11079, 7, 30, 1, 0.0500000007);
INSERT INTO order_details VALUES (11079, 8, 40, 2, 0.100000001);
INSERT INTO order_details VALUES (11079, 10, 31, 1, 0);