FROM golang:1.5.3

# This is where the node will look for public keys in case authentication is
# enabled (default: on).  When starting the container, this should be mounted
# as a volume (using the -v option to "docker run").
ENV PUBKEYS "/etc/exis/keys/public"

# Compile the node.  This produces a binary named "main".
COPY . /go/src/github.com/exis-io/node
RUN cd /go/src/github.com/exis-io/node && \
        go get -d -v . && \
        go build runner/main.go

EXPOSE 8000
USER daemon
WORKDIR /go/src/github.com/exis-io/node

CMD ["./main"]
