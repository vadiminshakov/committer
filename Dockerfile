FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/committer ./cmd/committer

FROM alpine:3.22

RUN addgroup -S committer \
    && adduser -S -G committer committer \
    && mkdir /data \
    && chown committer:committer /data

COPY --from=build /out/committer /usr/local/bin/committer
USER committer
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/committer"]
