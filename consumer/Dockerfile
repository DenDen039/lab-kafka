FROM golang:1.21 As builder

WORKDIR /app

COPY . .
RUN go mod tidy

RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o /consumer .

FROM debian:bookworm

RUN apt-get update && apt-get install -y \
    build-essential pkg-config git

COPY --from=builder /consumer /consumer

CMD ["/consumer"]