FROM jupyter/base-notebook

ENV PYTHONUNBUFFERED=1
ENV TERM=dumb
ENV NO_COLOR=True
RUN pip install --no-cache-dir fastapi uvicorn jupyter_client matplotlib
WORKDIR /app
COPY inside_server.py /app/server.py

EXPOSE 8000
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]