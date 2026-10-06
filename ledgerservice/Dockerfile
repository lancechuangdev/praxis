FROM golang:1.25 AS build
WORKDIR /src
COPY api/go.mod api/go.sum ./api/
COPY ledgerservice/go.mod ledgerservice/go.sum ./ledgerservice/
RUN cd ledgerservice && go mod download
COPY api ./api
COPY ledgerservice ./ledgerservice
WORKDIR /src/ledgerservice
RUN CGO_ENABLED=0 go build -trimpath -o /ledger-service ./cmd/ledger-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ledger-service /ledger-service
EXPOSE 8081 9091
ENTRYPOINT ["/ledger-service"]
