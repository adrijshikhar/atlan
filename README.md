# Monte Carlo Webhook Receiver

## Setup

1. Create local configuration:

```bash
cp config.yml.template config.yml
```

2. Edit `config.yml` with your local settings

3. Run the application:
    - Using Maven:
      ```bash
      mvn clean package
      java -Dconfig.path=config.yml -jar target/webhook-receiver-1.0-SNAPSHOT-jar-with-dependencies.jar
      ```
    - Using IDE:
        - Add VM option: `-Dconfig.path=config.yml`
        - Run `com.atlan.montecarlo.Application`

## Docker Setup

```bash
cd docker
docker-compose up -d
```

## Development

- Configuration files:
    - `config.yml.template`: Template for local development
    - `config.yml`: Local configuration (not committed)
    - `docker/config/application.yml`: Docker environment configuration
