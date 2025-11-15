FROM debezium/connect:2.6

USER root

# نصب curl با microdnf (UBI base)
RUN microdnf install -y curl && microdnf clean all

USER kafka

# اگر اسکریپت اضافه داری اینجا کپی کن
# COPY register.sh /register.sh
# RUN chmod +x /register.sh

EXPOSE 8083
