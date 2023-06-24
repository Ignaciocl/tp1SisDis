FROM golang:latest

WORKDIR /client
COPY . .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o client
ENTRYPOINT ["./client"]