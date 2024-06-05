#FROM scratch
FROM alpine:latest
COPY . /opt
WORKDIR /opt/coordinator
RUN apk add --no-cache tzdata
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN chmod +x coordinator
#ENTRYPOINT ["tail","-f","/etc/hosts"]
#RUN ./coordinator &
#ENTRYPOINT ["tail","-f","log/coordinator.log"]
ENTRYPOINT ["./coordinator"]
