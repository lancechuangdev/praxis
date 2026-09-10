FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /ledger-service ./cmd/ledger-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ledger-service /ledger-service
EXPOSE 8081 9091
ENTRYPOINT ["/ledger-service"]
