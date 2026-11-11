# The egress plugin: the credential injector Substrate calls on actor egress.
#
# Static binary on distroless, because this sits in the request path of every
# outbound call an actor makes and holds the model credential. The less that is
# in the image, the less there is to reach if something gets in.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
# CGO off so the result runs on distroless/static.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/plugin .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/plugin /plugin
USER nonroot:nonroot
EXPOSE 50051
ENTRYPOINT ["/plugin"]
