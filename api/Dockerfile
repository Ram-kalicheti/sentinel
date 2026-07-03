FROM python:3.11-slim

WORKDIR /app

# libgomp1 is required by xgboost and is absent from the slim base
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*

COPY api/requirements.txt ./api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

# scorer imports ml and pipeline so both packages ship in the image
COPY pipeline ./pipeline
COPY ml ./ml
COPY api ./api

# model loads from this baked-in path so no registry auth is needed at runtime
ENV PYTHONPATH=/app
ENV MODEL_PATH=/app/api/model

EXPOSE 8000
CMD ["uvicorn", "api.scorer:app", "--host", "0.0.0.0", "--port", "8000"]