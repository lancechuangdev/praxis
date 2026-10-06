FROM golang:1.25-alpine AS build
WORKDIR /src
COPY api/go.mod api/go.sum ./api/
COPY orderservice/go.mod orderservice/go.sum ./orderservice/
RUN cd orderservice && go mod download
COPY api ./api
COPY orderservice ./orderservice
WORKDIR /src/orderservice
RUN CGO_ENABLED=0 go build -trimpath -o /out/order-service ./cmd/order-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/order-service /order-service
EXPOSE 8083
ENTRYPOINT ["/order-service"]
