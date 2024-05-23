#FROM scratch
FROM alpine:latest
COPY . /opt
WORKDIR /opt/scanner
RUN apk add --no-cache tzdata
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN chmod +x scanner
ENTRYPOINT ["./scanner"]
