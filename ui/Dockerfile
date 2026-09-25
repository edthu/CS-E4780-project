FROM python:3.12-slim

# Unlike the JVM services (whose images COPY a pre-built fat jar), this image
# builds from source, so no sbt step is needed before `docker compose build ui`.
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY .streamlit /app/.streamlit
COPY app.py /app/app.py

RUN useradd --create-home --uid 10001 appuser && chown -R appuser /app
USER appuser

EXPOSE 8501

HEALTHCHECK --interval=15s --timeout=5s --retries=5 --start-period=20s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=3).status==200 else 1)"

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
