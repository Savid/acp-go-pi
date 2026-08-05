# The base is pinned to an immutable multi-architecture OCI index. The fixture
# traces the production adapter while its runtime network is disabled.
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

RUN apt-get update \
 && apt-get install --no-install-recommends --assume-yes \
      strace \
      sudo \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 native-canary \
 && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/native-canary \
      --shell /usr/sbin/nologin native-canary \
 && chmod 0700 /home/native-canary
