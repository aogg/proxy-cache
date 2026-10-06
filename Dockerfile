# syntax=docker/dockerfile:1

# ============================================================================
# Stage 1: builder —— 在 golang 官方镜像中静态编译
# 国内网络环境可构建时覆盖代理:
#   docker build --build-arg GOPROXY=https://goproxy.cn,direct -t proxy-cache .
# ============================================================================
FROM golang:1.23-alpine AS builder

ARG GOPROXY=https://proxy.golang.org,direct

ENV CGO_ENABLED=0 \
    GO111MODULE=on \
    GOPROXY=${GOPROXY}

WORKDIR /src

# 先拷贝依赖清单，充分利用 docker 层缓存
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/proxy-cache .

# ============================================================================
# Stage 2: runtime —— alpine 运行镜像，只含二进制 + 默认配置 + 缓存目录
# ============================================================================
FROM alpine:3.20

# ca-certificates: 回源 https 目标所需；tzdata: 时区
# app(uid=10001): 非 root 运行用户
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 10001 -S app \
    && adduser -u 10001 -S -G app -h /app app \
    && mkdir -p /app/cache-data \
    && chown -R app:app /app/cache-data

WORKDIR /app

# 只拷贝最终产物：静态编译的二进制 + 一份开箱可用的默认配置
COPY --from=builder /out/proxy-cache /usr/local/bin/proxy-cache
COPY config.example.yaml /app/config.yaml

USER app

# 配置路径（挂载自己的配置到 /app/config.yaml 即可覆盖）
ENV PROXY_CACHE_CONFIG=/app/config.yaml

# 服务默认监听 0.0.0.0:8080（可在配置中修改）
EXPOSE 8080

# 缓存目录建议挂载卷持久化：docker run -v $(pwd)/cache-data:/app/cache-data ...
VOLUME ["/app/cache-data"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/proxy-cache"]
CMD ["-c", "/app/config.yaml"]
