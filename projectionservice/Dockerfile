FROM golang:1.25 AS build
WORKDIR /src
COPY api/go.mod api/go.sum ./api/
COPY projectionservice/go.mod projectionservice/go.sum ./projectionservice/
RUN cd projectionservice && go mod download
COPY api ./api
COPY projectionservice ./projectionservice
WORKDIR /src/projectionservice
RUN CGO_ENABLED=0 go build -trimpath -o /projection-service ./cmd/projection-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /projection-service /projection-service
EXPOSE 8086
ENTRYPOINT ["/projection-service"]
