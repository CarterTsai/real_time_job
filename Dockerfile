FROM python:3.14.4-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml requirements.txt ./
COPY src ./src

RUN python -m pip install --upgrade pip \
    && python -m pip install .

CMD ["checkpoint-consumer"]
