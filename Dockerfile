FROM golang:1.23-bookworm
RUN apt update
RUN apt install ca-certificates
RUN update-ca-certificates -f
ENV CGO_ENABLED=1
ADD go.mod /src/go.mod
ADD go.sum /src/go.sum

WORKDIR /src
RUN go mod download -x

ADD . /src
RUN make build

FROM debian:bookworm
COPY --from=0 /src/feedgen /feedgen
COPY --from=0 /src/feedgen-admin /feedgen-admin
RUN apt update
RUN apt install -y ca-certificates
RUN update-ca-certificates -f
CMD ["/feedgen"]
