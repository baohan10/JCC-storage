FROM alpine:3.16
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add --no-cache ca-certificates bash fuse3 curl unzip tini

#RUN curl https://rclone.org/install.sh | bash
COPY rclone /usr/bin/rclone
RUN chmod +x /usr/bin/rclone
# Use pre-compiled version (with cirectory marker patch)
# https://github.com/rclone/rclone/pull/5323
# COPY bin/rclone /usr/bin/rclone
# RUN chmod 755 /usr/bin/rclone \
#     && chown root:root /usr/bin/rclone

COPY ./_output/csi-rclone-plugin /bin/csi-rclone-plugin
RUN chmod +x /bin/csi-rclone-plugin

ENTRYPOINT [ "/sbin/tini", "--"]
CMD ["/bin/csi-rclone-plugin"]
