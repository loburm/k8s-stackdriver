# This file should be place in fluent-bit directory.
FROM fluent/fluent-bit:0.13.2 as builder

FROM gcr.io/google-containers/debian-base-amd64:0.3

RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install --no-install-recommends ca-certificates libssl1.0.2 -y \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get autoclean
COPY --from=builder /fluent-bit /fluent-bit
COPY conf/fluent-bit.conf conf/parsers.conf conf/parsers_java.conf /fluent-bit/etc/

EXPOSE 2020

COPY stackdriver.so /fluent-bit
# Entry point
CMD ["/fluent-bit/bin/fluent-bit", "-e", "/fluent-bit/stackdriver.so", "-c", "/fluent-bit/etc/fluent-bit.conf"]
