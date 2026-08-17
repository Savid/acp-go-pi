# The base is pinned to an immutable multi-architecture OCI index. Node is the
# runtime pi ships on, so the pinned pi release below is the real native binary
# the canary drives through the production adapter while its runtime network is
# disabled.
FROM node:22-bookworm-slim@sha256:d649c27dae7ba0137b3cef5dd75baa422c08dc3d9e3fc0c23dfb172dc3cc6436

# The release is verified by digest before it is installed, and the version is
# re-checked from the installed binary so a registry that served something else
# fails the build rather than the canary. Every transitive dependency is fixed
# by the npm-shrinkwrap.json the release itself ships.
ARG PI_VERSION=0.82.1
ARG PI_TARBALL_URL=https://registry.npmjs.org/@earendil-works/pi-coding-agent/-/pi-coding-agent-0.82.1.tgz
ARG PI_TARBALL_SHA256=8343ab95cbab5766f2f5d48844df8db13e772ead2e2976166cbb820a29dacb7d

RUN apt-get update \
 && apt-get install --no-install-recommends --assume-yes \
      ca-certificates \
      curl \
      strace \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 native-canary \
 && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/native-canary \
      --shell /usr/sbin/nologin native-canary \
 && chmod 0700 /home/native-canary \
 && curl --fail --silent --show-error --location "$PI_TARBALL_URL" --output /tmp/pi.tgz \
 && printf '%s  %s\n' "$PI_TARBALL_SHA256" /tmp/pi.tgz | sha256sum --check --strict - \
 && npm install --global --no-fund --no-audit /tmp/pi.tgz \
 && rm -f /tmp/pi.tgz \
 && test "$(pi --version)" = "$PI_VERSION"

# No browser launcher is installed. A launcher the adapter's shim did not
# create must therefore be one the adapter itself put on the child's PATH, which
# is what the canary's syscall trace decides.
