package com.atlan.montecarlo.service;

import java.util.*;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasBuiltInTypes;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.configuration.PropertiesConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AtlasService {
  private final AtlasClientV2 atlasClient;

  public AtlasService(String atlasUrl, String username, String password) {
    try {
      var configuration = new PropertiesConfiguration();
      configuration.setProperty("atlas.rest.address", atlasUrl);
      configuration.setProperty("atlas.authentication.method.kerberos", "false");
      configuration.setProperty("atlas.authentication.basic.username", username);
      configuration.setProperty("atlas.authentication.basic.password", password);
      configuration.setProperty("atlas.client.connectTimeoutMSecs", "60000");
      configuration.setProperty("atlas.client.readTimeoutMSecs", "60000");

      this.atlasClient =
          new AtlasClientV2(
              configuration, new String[] {atlasUrl}, new String[] {username, password});

      initializeTypeDefinitions();
      initializeClassifications();
      log.info("Successfully initialized Atlas client with URL: {}", atlasUrl);
    } catch (Exception e) {
      log.error("Failed to initialize Atlas client", e);
      throw new RuntimeException("Failed to initialize Atlas client", e);
    }
  }

  private void initializeTypeDefinitions() throws Exception {
    try {
      var entityDefs = new ArrayList<AtlasEntityDef>();

      // Create Table type
      var tableDef = new AtlasEntityDef("Table");
      tableDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("name", AtlasBaseTypeDef.ATLAS_TYPE_STRING));

      var qualifiedNameAttr =
          AtlasTypeUtil.createRequiredAttrDef("qualifiedName", AtlasBaseTypeDef.ATLAS_TYPE_STRING);
      qualifiedNameAttr.setIsUnique(true);
      tableDef.addAttribute(qualifiedNameAttr);

      entityDefs.add(tableDef);

      // Create MonteCarloAlert type
      var alertDef = new AtlasEntityDef("MonteCarloAlert");
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("qualifiedName", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("issueType", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("severity", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("createdTime", AtlasBaseTypeDef.ATLAS_TYPE_DATE));
      alertDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("status", AtlasBaseTypeDef.ATLAS_TYPE_STRING));

      var tableRef = new AtlasStructDef.AtlasAttributeDef();
      tableRef.setName("table");
      tableRef.setTypeName("Table");
      tableRef.setIsOptional(false);
      alertDef.addAttribute(tableRef);

      entityDefs.add(alertDef);

      var typesDef = new AtlasTypesDef();
      typesDef.setEntityDefs(entityDefs);
      atlasClient.createAtlasTypeDefs(typesDef);

      log.info("Successfully created Atlas type definitions");
    } catch (Exception e) {
      if (e.getMessage().contains("already exists")) {
        log.info("Atlas types already exist");
      } else {
        throw e;
      }
    }
  }

  private void initializeClassifications() throws Exception {
    try {
      var piiDef = new AtlasClassificationDef("PII");

      var dataElementsAttr =
          new AtlasStructDef.AtlasAttributeDef(
              "dataElements",
              new AtlasArrayType(new AtlasBuiltInTypes.AtlasStringType()).getTypeName());
      dataElementsAttr.setIsOptional(true);
      piiDef.addAttribute(dataElementsAttr);

      piiDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("level", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      piiDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("lastUpdated", AtlasBaseTypeDef.ATLAS_TYPE_DATE));

      var typesDef = new AtlasTypesDef();
      typesDef.setClassificationDefs(Collections.singletonList(piiDef));
      atlasClient.createAtlasTypeDefs(typesDef);

      log.info("Successfully created PII classification");
    } catch (Exception e) {
      if (e.getMessage().contains("already exists")) {
        log.info("PII classification already exists");
      } else {
        throw e;
      }
    }
  }

  public void createAlertEntity(
      String tableId, String issueType, String severity, Map<String, Object> metadata) {
    try {
      if (tableId == null || tableId.trim().isEmpty()) {
        throw new IllegalArgumentException("tableId cannot be null or empty");
      }

      var tableEntity = getOrCreateTableEntity(tableId);
      var alertEntity = new AtlasEntity("MonteCarloAlert");
      var alertQualifiedName =
          String.format("%s_%s_%d", tableId, issueType, System.currentTimeMillis());

      alertEntity.setAttribute("qualifiedName", alertQualifiedName);
      alertEntity.setAttribute("issueType", issueType);
      alertEntity.setAttribute("severity", severity);
      alertEntity.setAttribute("createdTime", new Date());
      alertEntity.setAttribute("status", "ACTIVE");
      alertEntity.setAttribute("table", AtlasTypeUtil.getAtlasObjectId(tableEntity.getEntity()));

      if (metadata != null) {
        alertEntity.setAttribute("metadata", metadata);
      }

      var alertEntityInfo = new AtlasEntity.AtlasEntityWithExtInfo(alertEntity);
      var response = atlasClient.createEntity(alertEntityInfo);

      log.info("Created alert entity with GUID: {}", response.getFirstEntityCreated().getGuid());
    } catch (Exception e) {
      log.error("Error creating alert for table: {}", tableId, e);
      throw new RuntimeException("Failed to create alert", e);
    }
  }

  public void applyPIIClassification(String tableId, List<String> dataElements, String level) {
    try {
      var tableEntity = getOrCreateTableEntity(tableId);
      var classification = new AtlasClassification("PII");

      var attributes = new HashMap<String, Object>();
      attributes.put("dataElements", dataElements);
      attributes.put("level", level);
      attributes.put("lastUpdated", new Date());
      classification.setAttributes(attributes);
      classification.setPropagate(true);

      atlasClient.addClassifications(
          tableEntity.getEntity().getGuid(), Collections.singletonList(classification));

      log.info("Applied PII classification to table: {}", tableId);
    } catch (Exception e) {
      log.error("Error applying PII classification to table: {}", tableId, e);
      throw new RuntimeException("Failed to apply PII classification", e);
    }
  }

  private AtlasEntity.AtlasEntityWithExtInfo getOrCreateTableEntity(String tableId)
      throws Exception {
    try {
      var attributes = Collections.singletonMap("qualifiedName", tableId);
      var entityInfo = atlasClient.getEntityByAttribute("Table", attributes);

      if (entityInfo != null && entityInfo.getEntity() != null) {
        return entityInfo;
      }
    } catch (Exception e) {
      log.debug("Table entity not found, creating new one: {}", tableId);
    }

    var entity = new AtlasEntity("Table");
    entity.setAttribute("name", tableId);
    entity.setAttribute("qualifiedName", tableId);
    entity.setAttribute("createTime", new Date());

    var entityInfo = new AtlasEntity.AtlasEntityWithExtInfo(entity);
    var response = atlasClient.createEntity(entityInfo);
    var guid = response.getFirstEntityCreated().getGuid();

    log.info("Created new Table entity with GUID: {}", guid);
    return atlasClient.getEntityByGuid(guid);
  }
}
