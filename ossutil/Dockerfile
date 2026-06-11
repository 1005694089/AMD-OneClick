FROM alpine:3.20

ARG OSSUTIL_VERSION=1.7.19

RUN apk add --no-cache bash ca-certificates coreutils curl flock unzip util-linux \
    && curl -fsSL "https://gosspublic.alicdn.com/ossutil/${OSSUTIL_VERSION}/ossutil-v${OSSUTIL_VERSION}-linux-amd64.zip" -o /tmp/ossutil.zip \
    && unzip -p /tmp/ossutil.zip "ossutil-v${OSSUTIL_VERSION}-linux-amd64/ossutil64" > /usr/local/bin/ossutil \
    && chmod +x /usr/local/bin/ossutil \
    && rm -f /tmp/ossutil.zip \
    && /usr/local/bin/ossutil --version
