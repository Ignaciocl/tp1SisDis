FROM golang
WORKDIR /app
COPY ./go.mod .
COPY ./go.sum .
RUN go get github.com/codegangsta/gin
RUN go install github.com/codegangsta/gin
RUN go mod download
ENTRYPOINT ["gin", "--immediate","run","/app/main.go"]
