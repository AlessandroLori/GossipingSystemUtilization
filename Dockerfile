# Dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/node ./cmd/node

FROM alpine:3.20
WORKDIR /app
COPY --from=build /bin/node /app/node
COPY config.json /app/config.json
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Default env; possono essere sovrascritte da compose
ENV IS_SEED=false \
    PORT=9002

EXPOSE 9001 9002 9004 9006
ENTRYPOINT ["/app/entrypoint.sh"]
