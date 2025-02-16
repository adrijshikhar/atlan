package com.atlan.montecarlo.config;

import java.io.File;
import java.io.InputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigurationLoader {
  private static final String DEFAULT_CONFIG_PATH = "config.yml";
  private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  public static ServerConfig loadConfiguration(String configPath) {
    try {
      String path = configPath != null ? configPath : DEFAULT_CONFIG_PATH;
      log.info("Loading configuration from: {}", path);

      // First try to load from external file
      File configFile = new File(path);
      if (configFile.exists()) {
        log.info("Loading configuration from external file: {}", configFile.getAbsolutePath());
        return mapper.readValue(configFile, ServerConfig.class);
      }

      // If external file not found, try to load from classpath
      try (InputStream is = ConfigurationLoader.class.getClassLoader().getResourceAsStream(path)) {
        if (is != null) {
          log.info("Loading configuration from classpath: {}", path);
          return mapper.readValue(is, ServerConfig.class);
        }
      }

      log.warn("No configuration file found, using default configuration");
      return ServerConfig.defaultConfig();
    } catch (Exception e) {
      log.error("Failed to load configuration", e);
      return ServerConfig.defaultConfig();
    }
  }
}
