FROM python:3.8.0-slim

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app

# Copy the rest of the application code to /app
COPY . $APP_HOME

WORKDIR $APP_HOME

COPY . ./

# Copy the 'secrets' folder to /app/secrets in the container
COPY secrets $APP_HOME/secrets

ENV PORT 8080

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# As an example here we're running the web service with one worker on uvicorn.
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1