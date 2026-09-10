FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/matching-engine ./cmd/matching-engine

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/matching-engine /matching-engine
EXPOSE 9092 8084
ENTRYPOINT ["/matching-engine"]
