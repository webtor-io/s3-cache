FROM alpine:latest as certs
RUN apk update && apk upgrade && apk add --no-cache ca-certificates

FROM golang:latest as build
WORKDIR /app
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux
RUN go build -ldflags '-w -s' -a -installsuffix cgo -o server

FROM scratch
COPY --from=build /app/server .
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 8080
CMD ["./server"]
