FROM golang:1.20-alpine as builder

WORKDIR /app
ENV CGO_ENABLED 1

RUN apk add --no-cache --update gcc musl-dev
COPY go.* ./
RUN go mod download

COPY . ./
RUN go build -v -o msgreplay

FROM alpine

COPY --from=builder /app/msgreplay /app/msgreplay

ENTRYPOINT ["/app/msgreplay"]
