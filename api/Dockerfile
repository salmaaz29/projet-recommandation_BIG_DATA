FROM python:3.11-slim

WORKDIR /app

RUN pip install flask==3.0.0 flask-cors==4.0.0

COPY . .

EXPOSE 5000

CMD ["python", "app.py"]