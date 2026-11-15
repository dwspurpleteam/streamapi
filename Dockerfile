FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine3.24 AS builder

RUN apk add --no-cache git
WORKDIR /src
ENV GOTOOLCHAIN=local

COPY . .

ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -p 2 -trimpath -ldflags='-s -w' -o /out/streamapi ./cmd/...

FROM alpine:3.24.1

RUN apk add --no-cache ca-certificates
COPY --from=builder /out/streamapi /streamapi

USER 65532:65532
ENTRYPOINT ["/streamapi"]
