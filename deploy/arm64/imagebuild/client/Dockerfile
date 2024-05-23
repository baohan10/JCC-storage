#FROM scratch
FROM alpine:latest
COPY . /opt
WORKDIR /opt/client
RUN chmod +x client
ENTRYPOINT ["./client","serve","http"]
