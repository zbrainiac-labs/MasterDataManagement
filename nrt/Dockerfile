FROM python:3.13-slim

WORKDIR /app

# Install dependencies (as root; files stay root-owned and read-only for the runtime user)
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .

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
