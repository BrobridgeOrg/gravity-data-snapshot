FROM golang:1.12.1-alpine AS builder
WORKDIR $GOPATH/src/github.com/BrobridgeOrg/gravity-data-snapshot/
COPY . .

RUN apk add --update git
RUN CGO_ENABLED=0 GO111MODULE=on go build -installsuffix cgo -o /gravity-data-snapshot

FROM alpine:3.8
COPY --from=builder /gravity-data-snapshot /usr/local/bin/gravity-data-snapshot
COPY ./config/config.toml /usr/local/bin/config.toml
EXPOSE 44446
CMD ["/usr/local/bin/gravity-data-snapshot"]
