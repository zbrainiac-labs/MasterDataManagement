FROM python:3.13-slim

WORKDIR /app

# Debian security fixes for base-image packages (e.g. zlib) land before a new
# python:3.13-slim is published; CI's Trivy scan blocks on HIGH/CRITICAL.
RUN apt-get update && apt-get upgrade -y --no-install-recommends \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install dependencies (as root; files stay root-owned and read-only for the runtime user).
# requirements.txt pins every transitive dependency (pip-compile from pyproject.toml),
# so builds are reproducible and Trivy / Dependabot can see the exact versions.
COPY pyproject.toml requirements.txt ./
COPY src/ src/
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir --no-deps -e . \
 && python -m pip uninstall -y pip   # build-time only; its vendored libs (urllib3, msgpack, ...) carry CVEs

# Copy schema (for init scripts if needed)
COPY schema/ schema/
COPY config.yaml .
COPY app/ app/

# Run as an unprivileged user (all services listen on ports > 1024).
# A home directory is needed for Streamlit's ~/.streamlit state.
RUN useradd --create-home --uid 10001 --user-group mdm
USER mdm

# Entry point: run consumer
CMD ["python", "-m", "nrt_mdm"]
