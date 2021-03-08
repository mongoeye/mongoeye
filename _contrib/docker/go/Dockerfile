FROM library/golang:1.16

# Install packages
RUN apt-get update && apt-get install -y zip

# Update path
ENV PATH=/code/bin:$PATH
RUN mkdir /code
WORKDIR /code
