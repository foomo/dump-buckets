# -----------------------------------------------------------------------------
# Runtime
# -----------------------------------------------------------------------------
FROM --platform=$TARGETPLATFORM alpine:latest as runtime

ENV TZ=Europe/Zurich
ENV DEFAULT_TZ ${TZ}

# Add tools
RUN apk add -U --no-cache \
    tzdata \
    ca-certificates \
    curl \
    bash \
    mongodb-tools \
    npm \
  && rm -rf /var/cache/apk/*

RUN  cp /usr/share/zoneinfo/${DEFAULT_TZ} /etc/localtime \
     && echo ${DEFAULT_TZ} > /etc/timezone

# Install NodeJS dependencies

RUN npm install -g contentful-cli


# -----------------------------------------------------------------------------
# Builder Base
# -----------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1-alpine as build-env

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG GOPROXY="https://proxy.golang.org,direct"

WORKDIR /workdir

COPY ./go.mod ./go.sum ./

# Download all the dependencies
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
     GOPROXY=${GOPROXY} go mod download -x

# Import the code from the context.
COPY ./ ./

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    GOARCH=${TARGETARCH} GOOS=${TARGETOS} CGO_ENABLED=0  go build -o /dumpb main.go


# -----------------------------------------------------------------------------
# Application
# -----------------------------------------------------------------------------
FROM runtime

COPY --from=build-env /dumpb /dumpb

ENTRYPOINT "/dumpb"
