FROM golang:1.21 As builder

WORKDIR /app

COPY . .
RUN go mod tidy

RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o /producer .


FROM debian:bookworm

RUN apt-get update && apt-get install -y \
    build-essential pkg-config git
   

# Copy the binary from the previous stage
COPY --from=builder /producer /producer

CMD ["./producer"]