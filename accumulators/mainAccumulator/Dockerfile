FROM golang
WORKDIR /app
COPY . .
RUN go get github.com/codegangsta/gin
RUN go install github.com/codegangsta/gin
RUN go mod download
ENTRYPOINT ["gin", "--immediate","run","/app/main.go"]
