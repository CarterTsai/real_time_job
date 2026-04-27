FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./

RUN python -m pip install --upgrade pip 
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "service.app"]