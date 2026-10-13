FROM golang:1.25-alpine AS build
WORKDIR /src
COPY api/go.mod api/go.sum ./api/
COPY riskservice/go.mod riskservice/go.sum ./riskservice/
RUN cd riskservice && go mod download
COPY api ./api
COPY riskservice ./riskservice
WORKDIR /src/riskservice
RUN CGO_ENABLED=0 go build -trimpath -o /out/risk-service ./cmd/risk-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/risk-service /risk-service
EXPOSE 9095 8085
ENTRYPOINT ["/risk-service"]
