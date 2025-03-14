FROM apache/airflow:2.10.5

USER root

# Install Chrome dependencies and Xvfb
RUN apt-get update && apt-get install -y \
    xvfb \
    wget \
    gnupg \
    ca-certificates \
    apt-transport-https \
    curl \
    unzip \
    --no-install-recommends

# Install Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install a specific version of ChromeDriver that's compatible with Chrome
RUN wget -q "https://chromedriver.storage.googleapis.com/112.0.5615.49/chromedriver_linux64.zip" \
    && unzip chromedriver_linux64.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/chromedriver \
    && rm chromedriver_linux64.zip

# Make sure the chrome binary is accessible
ENV CHROME_PATH=/usr/bin/google-chrome
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver

USER airflow