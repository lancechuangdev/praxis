FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/outbox-relay ./cmd/outbox-relay

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/outbox-relay /outbox-relay
EXPOSE 8082
ENTRYPOINT ["/outbox-relay"]
