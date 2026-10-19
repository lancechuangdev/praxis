FROM golang:1.25-alpine AS build
WORKDIR /src
COPY api/go.mod api/go.sum ./api/
COPY orderservice/go.mod orderservice/go.sum ./orderservice/
RUN cd orderservice && go mod download
COPY api ./api
COPY orderservice ./orderservice
WORKDIR /src/orderservice
RUN CGO_ENABLED=0 go build -trimpath -o /out/order-service ./cmd/order-service

RUN CGO_ENABLED=0 go build -trimpath -o /workflow-export ./cmd/workflow-export

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/order-service /order-service
COPY --from=build /workflow-export /workflow-export
EXPOSE 8083
ENTRYPOINT ["/order-service"]
