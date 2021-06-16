FROM ubuntu:20.04 as builder

ARG DEBIAN_FRONTEND=noninteractive
ARG BUILD_PROOF_OF_SPACE_STATICALLY=OFF

RUN apt-get update -y

RUN apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  cmake \
  git \
  python3 \
  python3-dev \
  python3-distutils \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN mkdir -p build

WORKDIR /app/build
RUN cmake -DBUILD_PROOF_OF_SPACE_STATICALLY=${BUILD_PROOF_OF_SPACE_STATICALLY} ../
RUN cmake --build . -- -j `nproc`


FROM ubuntu:20.04

COPY --from=builder /app/build /app

CMD ["/app/ProofOfSpace"]
