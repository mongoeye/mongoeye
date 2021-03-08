ARG MONGODB_VERSION
FROM library/mongo:${MONGODB_VERSION}

# Install package
ADD https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 /usr/local/bin/jq
RUN chmod +x /usr/local/bin/jq

# Copy test data
RUN mkdir   /init
ADD init.sh /init/init.sh
ADD dataset /init/dataset

# Import test data
RUN chmod +x /init/init.sh && /init/init.sh
