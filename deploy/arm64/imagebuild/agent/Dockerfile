#FROM scratch
FROM alpine:latest
COPY . /opt
WORKDIR /opt/agent
RUN chmod +x agent
ENTRYPOINT ["./agent"]
