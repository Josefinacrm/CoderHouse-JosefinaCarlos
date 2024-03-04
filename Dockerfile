FROM python:3.8

WORKDIR /app

COPY JosefinaCarlos.Entrega3.py .

CMD ["python", "-u", "JosefinaCarlos.Entrega3.py"]
