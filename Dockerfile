FROM php:8.3-cli

# install dependencies
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libxml2-dev \
    git \
    zip \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# install docker-php extensions
RUN docker-php-ext-install curl xml

# workdir
WORKDIR /app
COPY . .

# install composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# composer install
RUN composer install --no-interaction --optimize-autoloader --no-dev

CMD ["tail", "-f", "/dev/null"]