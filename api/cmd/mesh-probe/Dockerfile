# Build from repository root: docker build -f api/cmd/mesh-probe/Dockerfile .
FROM golang:1.25 AS build
WORKDIR /src/api
COPY api/go.mod api/go.sum ./
RUN go mod download
COPY api/cmd/mesh-probe ./cmd/mesh-probe
RUN CGO_ENABLED=0 go build -trimpath -o /mesh-probe ./cmd/mesh-probe
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /mesh-probe /mesh-probe
ENTRYPOINT ["/mesh-probe"]
