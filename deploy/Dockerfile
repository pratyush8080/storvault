# syntax=docker/dockerfile:1
# Multi-stage: build the C++ agent once, then ship it in a slim "agent" image or inside the orchestrator image.
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake ninja-build libssl-dev libcurl4-openssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY agent/ agent/
RUN cmake -S agent -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build \
 && ./build/sv_tests \
 && strip build/storvault-agent

# ---- target: agent (CLI only) ----
FROM ubuntu:24.04 AS agent
RUN apt-get update && apt-get install -y --no-install-recommends libssl3t64 libcurl4t64 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 10001 storvault
COPY --from=build /src/build/storvault-agent /usr/local/bin/storvault-agent
USER 10001
ENTRYPOINT ["storvault-agent"]

# ---- target: orchestrator (Python service + agent binary it spawns) ----
FROM ubuntu:24.04 AS orchestrator
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv libssl3t64 libcurl4t64 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 10001 -d /data storvault && mkdir -p /data /tmp/storvault /restore && chown 10001 /data /tmp/storvault /restore
COPY orchestrator/requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
COPY orchestrator/storvault /app/storvault
COPY --from=build /src/build/storvault-agent /usr/local/bin/storvault-agent
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1 SV_AGENT_BIN=/usr/local/bin/storvault-agent \
    SV_DB_URL=sqlite:////data/storvault.db
WORKDIR /app
USER 10001
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s CMD curl -fs http://localhost:8080/healthz || exit 1
CMD ["uvicorn", "storvault.api:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8080"]
