/*
 * Copyright (c) 2025 Atlan Inc.
 */
package com.atlan.montecarlo.service;

import java.util.*;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.typedef.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasBuiltInTypes;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

@Slf4j
public class AtlasService {
  private final AtlasClientV2 atlasClient;

  public AtlasService(String atlasUrl, String username, String password) {
    try {
      Configuration configuration = new PropertiesConfiguration();
      configuration.setProperty("atlas.rest.address", atlasUrl);
      configuration.setProperty("atlas.authentication.method.kerberos", "false");
      configuration.setProperty("atlas.authentication.basic.username", username);
      configuration.setProperty("atlas.authentication.basic.password", password);
      configuration.setProperty("atlas.client.connectTimeoutMSecs", "60000");
      configuration.setProperty("atlas.client.readTimeoutMSecs", "60000");

      this.atlasClient =
          new AtlasClientV2(
              configuration, new String[] {atlasUrl}, new String[] {username, password});

      // Initialize required type definitions
      initializeTypeDefinitions();

      log.info("Successfully initialized Atlas client with URL: {}", atlasUrl);
    } catch (Exception e) {
      log.error("Failed to initialize Atlas client", e);
      throw new RuntimeException("Failed to initialize Atlas client", e);
    }
  }

  private void initializeTypeDefinitions() throws Exception {
    try {
      // Create entity type definitions
      List<AtlasEntityDef> entityDefs = new ArrayList<>();

      // Create Table type
      AtlasEntityDef tableDef = new AtlasEntityDef("Table");
      tableDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("name", AtlasBaseTypeDef.ATLAS_TYPE_STRING));

      AtlasStructDef.AtlasAttributeDef attrDef =
          AtlasTypeUtil.createRequiredAttrDef("qualifiedName", AtlasBaseTypeDef.ATLAS_TYPE_STRING);
      attrDef.setIsOptional(false);
      attrDef.setIsUnique(true);

      tableDef.addAttribute(attrDef);
      tableDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("description", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      tableDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("owner", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      tableDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("createTime", AtlasBaseTypeDef.ATLAS_TYPE_DATE));
      entityDefs.add(tableDef);

      // Create the types definition
      AtlasTypesDef typesDef = new AtlasTypesDef();
      typesDef.setEntityDefs(entityDefs);

      // Create Monte Carlo classification
      createMonteCarloClassificationType();

      // Create the type definitions in Atlas
      atlasClient.createAtlasTypeDefs(typesDef);
      log.info("Successfully created Atlas type definitions");
    } catch (Exception e) {
      // If types already exist, log and continue
      if (e.getMessage().contains("already exists")) {
        log.info("Atlas types already exist, continuing...");
      } else {
        log.error("Error creating Atlas type definitions", e);
        throw e;
      }
    }
  }

  public void updateTableMetadata(
      String tableId, String issueType, String severity, Map<String, Object> metadata) {
    try {
      if (tableId == null || tableId.trim().isEmpty()) {
        throw new IllegalArgumentException("tableId cannot be null or empty");
      }
      if (issueType == null || issueType.trim().isEmpty()) {
        throw new IllegalArgumentException("issueType cannot be null or empty");
      }
      if (severity == null || severity.trim().isEmpty()) {
        throw new IllegalArgumentException("severity cannot be null or empty");
      }

      // Create or update the table entity
      AtlasEntity.AtlasEntityWithExtInfo tableEntity = getOrCreateTableEntity(tableId);

      // Add Monte Carlo classification
      addMonteCarloClassification(tableEntity.getEntity().getGuid(), issueType, severity, metadata);

    } catch (Exception e) {
      log.error("Error updating Atlas metadata for table: {}", tableId, e);
      throw new RuntimeException("Failed to update Atlas metadata", e);
    }
  }

  private AtlasEntity.AtlasEntityWithExtInfo getOrCreateTableEntity(String tableId)
      throws Exception {
    try {
      // Debugging log
      log.info("Searching for Table entity with qualifiedName: {}", tableId);

      // Use a proper filter with a single attribute map
      Map<String, String> attributes = Collections.singletonMap("qualifiedName", tableId);
      AtlasEntity.AtlasEntityWithExtInfo entitiesInfo =
          atlasClient.getEntityByAttribute("Table", attributes);

      if (entitiesInfo != null && entitiesInfo.getEntity() != null) {
        log.info("Found Table entity with qualifiedName: {}", tableId);
        return entitiesInfo;
      }
    } catch (Exception ex) {
      log.warn(
          "Table entity not found for qualifiedName: {}. Proceeding to create a new one.", tableId);
    }

    try {
      // Create new entity if not found
      log.info("Creating new Table entity with qualifiedName: {}", tableId);
      AtlasEntity entity = new AtlasEntity("Table");
      entity.setAttribute("name", tableId);
      entity.setAttribute("qualifiedName", tableId);

      AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo =
          new AtlasEntity.AtlasEntityWithExtInfo(entity);
      EntityMutationResponse response = atlasClient.createEntity(entityWithExtInfo);
      AtlasEntityHeader entityHeader = response.getFirstEntityCreated();

      log.info("Successfully created Table entity with GUID: {}", entityHeader.getGuid());
      return atlasClient.getEntityByGuid(entityHeader.getGuid());
    } catch (AtlasServiceException e) {
      log.error("Error creating Table entity with qualifiedName: {}", tableId, e);
      throw new RuntimeException(e);
    }
  }

  private void addMonteCarloClassification(
      String entityGuid, String issueType, String severity, Map<String, Object> metadata)
      throws Exception {
    try {
      AtlasClassification classification = new AtlasClassification("MonteCarloIssue");

      Map<String, Object> attributes = new HashMap<>();
      attributes.put("issueType", issueType);
      attributes.put("severity", severity);
      attributes.put("lastUpdated", new Date());
      attributes.put("metadata", metadata);
      classification.setAttributes(attributes);

      List<AtlasClassification> classifications = Collections.singletonList(classification);
      atlasClient.addClassifications(entityGuid, classifications);
    } catch (Exception e) {
      log.error("Error adding classification to entity: {}", entityGuid, e);
      throw e;
    }
  }

  public void createMonteCarloClassificationType() throws Exception {
    try {
      // Check if the classification already exists
      AtlasClassificationDef existingDef =
          atlasClient.getClassificationDefByName("MonteCarloIssue");

      if (existingDef != null) {
        log.info("MonteCarloIssue classification type already exists. Skipping creation.");
        return;
      }
    } catch (Exception e) {
      log.info("MonteCarloIssue classification type does not exist. Proceeding with creation.");
    }

    try {
      AtlasClassificationDef classificationDef = new AtlasClassificationDef("MonteCarloIssue");

      List<AtlasStructDef.AtlasAttributeDef> attributeDefs = new ArrayList<>();

      attributeDefs.add(
          AtlasTypeUtil.createOptionalAttrDef(
              "issueType", new AtlasBuiltInTypes.AtlasStringType()));
      attributeDefs.add(
          AtlasTypeUtil.createOptionalAttrDef("severity", new AtlasBuiltInTypes.AtlasStringType()));
      attributeDefs.add(
          AtlasTypeUtil.createOptionalAttrDef(
              "lastUpdated", new AtlasBuiltInTypes.AtlasDateType()));
      attributeDefs.add(
          AtlasTypeUtil.createOptionalAttrDef(
              "metadata",
              new AtlasMapType(
                  new AtlasBuiltInTypes.AtlasStringType(),
                  new AtlasBuiltInTypes.AtlasStringType())));

      classificationDef.setAttributeDefs(attributeDefs);

      AtlasTypesDef typesDef = new AtlasTypesDef();
      typesDef.setClassificationDefs(Collections.singletonList(classificationDef));

      atlasClient.createAtlasTypeDefs(typesDef);

      log.info("Successfully created MonteCarloIssue classification type");
    } catch (Exception e) {
      log.error("Error creating MonteCarloIssue classification type", e);
      throw e;
    }
  }
}
