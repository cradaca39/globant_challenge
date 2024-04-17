FROM python:3.12

# Create app directory
WORKDIR /app
COPY . /app
# Install app dependencies

RUN pip install -r requirements.txt

# Bundle app source

EXPOSE 5000
CMD [ "python", "main.py" ]