FROM golang:1.25-alpine AS build
WORKDIR /src
COPY api/go.mod api/go.sum ./api/
COPY matchingengine/go.mod matchingengine/go.sum ./matchingengine/
RUN cd matchingengine && go mod download
COPY api ./api
COPY matchingengine ./matchingengine
WORKDIR /src/matchingengine
RUN CGO_ENABLED=0 go build -trimpath -o /out/matching-engine ./cmd/matching-engine

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/matching-engine /matching-engine
EXPOSE 9092 8084
ENTRYPOINT ["/matching-engine"]
