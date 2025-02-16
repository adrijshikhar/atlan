package com.atlan.montecarlo.service;

import java.util.*;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.typedef.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasBuiltInTypes;
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

      // Initialize required type definitions and classifications
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
      List<AtlasEntityDef> entityDefs = new ArrayList<>();

      // Create Table type
      AtlasEntityDef tableDef = new AtlasEntityDef("Table");
      tableDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("name", AtlasBaseTypeDef.ATLAS_TYPE_STRING));

      AtlasStructDef.AtlasAttributeDef attrDef =
          AtlasTypeUtil.createRequiredAttrDef("qualifiedName", AtlasBaseTypeDef.ATLAS_TYPE_STRING);
      attrDef.setIsUnique(true);

      tableDef.addAttribute(attrDef);
      tableDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("description", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      tableDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("owner", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      tableDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("createTime", AtlasBaseTypeDef.ATLAS_TYPE_DATE));
      entityDefs.add(tableDef);

      // Create MonteCarloAlert type
      AtlasEntityDef alertDef = new AtlasEntityDef("MonteCarloAlert");
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("qualifiedName", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("issueType", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("severity", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      alertDef.addAttribute(
          AtlasTypeUtil.createRequiredAttrDef("createdTime", AtlasBaseTypeDef.ATLAS_TYPE_DATE));
      alertDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("resolvedTime", AtlasBaseTypeDef.ATLAS_TYPE_DATE));
      alertDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("status", AtlasBaseTypeDef.ATLAS_TYPE_STRING));

      // Add relationship attribute to Table
      AtlasStructDef.AtlasAttributeDef tableRef = new AtlasStructDef.AtlasAttributeDef();
      tableRef.setName("table");
      tableRef.setTypeName("Table");
      tableRef.setIsOptional(false);
      alertDef.addAttribute(tableRef);

      entityDefs.add(alertDef);

      // Create the types definition
      AtlasTypesDef typesDef = new AtlasTypesDef();
      typesDef.setEntityDefs(entityDefs);

      // Create the type definitions in Atlas
      atlasClient.createAtlasTypeDefs(typesDef);
      log.info("Successfully created Atlas type definitions");
    } catch (Exception e) {
      if (e.getMessage().contains("already exists")) {
        log.info("Atlas types already exist, continuing...");
      } else {
        log.error("Error creating Atlas type definitions", e);
        throw e;
      }
    }
  }

  private void initializeClassifications() throws Exception {
    try {
      // Create PII Classification
      AtlasClassificationDef piiDef = new AtlasClassificationDef("PII");

      // Create array of strings attribute for data elements using AtlasArrayType
      AtlasStructDef.AtlasAttributeDef dataElementsAttr =
          new AtlasStructDef.AtlasAttributeDef(
              "dataElements", // attribute name
              new AtlasArrayType(new AtlasBuiltInTypes.AtlasStringType())
                  .getTypeName() // array of strings type
              );

      dataElementsAttr.setIsOptional(true);
      piiDef.addAttribute(dataElementsAttr);
      piiDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("level", AtlasBaseTypeDef.ATLAS_TYPE_STRING));
      piiDef.addAttribute(
          AtlasTypeUtil.createOptionalAttrDef("lastUpdated", AtlasBaseTypeDef.ATLAS_TYPE_DATE));

      AtlasTypesDef typesDef = new AtlasTypesDef();
      typesDef.setClassificationDefs(Collections.singletonList(piiDef));

      atlasClient.createAtlasTypeDefs(typesDef);
      log.info("Successfully created PII classification");
    } catch (Exception e) {
      if (e.getMessage().contains("already exists")) {
        log.info("PII classification already exists, continuing...");
      } else {
        log.error("Error creating PII classification", e);
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

      // Get or create the table entity first
      AtlasEntity.AtlasEntityWithExtInfo tableEntity = getOrCreateTableEntity(tableId);

      // Create alert entity
      AtlasEntity alertEntity = new AtlasEntity("MonteCarloAlert");

      // Generate a unique qualified name for the alert
      String alertQualifiedName =
          String.format("%s_%s_%d", tableId, issueType, System.currentTimeMillis());

      alertEntity.setAttribute("qualifiedName", alertQualifiedName);
      alertEntity.setAttribute("issueType", issueType);
      alertEntity.setAttribute("severity", severity);
      alertEntity.setAttribute("createdTime", new Date());
      alertEntity.setAttribute("status", "ACTIVE");
      alertEntity.setAttribute("table", AtlasTypeUtil.getAtlasObjectId(tableEntity.getEntity()));

      if (metadata != null) {
        alertEntity.setAttribute("metadata", metadata.toString());
      }

      // Create the alert entity
      AtlasEntity.AtlasEntityWithExtInfo alertEntityInfo =
          new AtlasEntity.AtlasEntityWithExtInfo(alertEntity);
      EntityMutationResponse response = atlasClient.createEntity(alertEntityInfo);

      log.info(
          "Created new MonteCarloAlert entity with GUID: {}",
          response.getFirstEntityCreated().getGuid());

    } catch (Exception e) {
      log.error("Error creating alert entity for table: {}", tableId, e);
      throw new RuntimeException("Failed to create alert entity", e);
    }
  }

  public void applyPIIClassification(String tableId, List<String> dataElements, String level) {
    try {
      // Get or create the table entity first
      AtlasEntity.AtlasEntityWithExtInfo tableEntity = getOrCreateTableEntity(tableId);

      AtlasClassification classification = new AtlasClassification("PII");

      Map<String, Object> attributes = new HashMap<>();
      attributes.put("dataElements", dataElements);
      attributes.put("level", level);
      attributes.put("lastUpdated", new Date());
      classification.setAttributes(attributes);

      // Enable propagation through lineage
      classification.setPropagate(true);

      List<AtlasClassification> classifications = Collections.singletonList(classification);
      atlasClient.addClassifications(tableEntity.getEntity().getGuid(), classifications);

      log.info("Applied PII classification to table: {}", tableId);
    } catch (Exception e) {
      log.error("Error applying PII classification to table: {}", tableId, e);
      throw new RuntimeException("Failed to apply PII classification", e);
    }
  }

  private AtlasEntity.AtlasEntityWithExtInfo getOrCreateTableEntity(String tableId)
      throws Exception {
    try {
      // Search for existing table entity
      Map<String, String> attributes = Collections.singletonMap("qualifiedName", tableId);
      AtlasEntity.AtlasEntityWithExtInfo entityInfo =
          atlasClient.getEntityByAttribute("Table", attributes);

      if (entityInfo != null && entityInfo.getEntity() != null) {
        log.info("Found existing Table entity with qualifiedName: {}", tableId);
        return entityInfo;
      }
    } catch (Exception e) {
      log.info("Table entity not found, will create new one: {}", tableId);
    }

    // Create new table entity if not found
    AtlasEntity entity = new AtlasEntity("Table");
    entity.setAttribute("name", tableId);
    entity.setAttribute("qualifiedName", tableId);
    entity.setAttribute("createTime", new Date());

    AtlasEntity.AtlasEntityWithExtInfo entityInfo = new AtlasEntity.AtlasEntityWithExtInfo(entity);
    EntityMutationResponse response = atlasClient.createEntity(entityInfo);

    String guid = response.getFirstEntityCreated().getGuid();
    log.info("Created new Table entity with GUID: {}", guid);

    return atlasClient.getEntityByGuid(guid);
  }

  public List<AtlasEntity> getTableAlerts(String tableId) throws AtlasServiceException {
    List<AtlasEntity> alerts = new ArrayList<>();

    try {
      // Get the table entity
      Map<String, String> attributes = Collections.singletonMap("qualifiedName", tableId);
      AtlasEntity.AtlasEntityWithExtInfo tableEntity =
          atlasClient.getEntityByAttribute("Table", attributes);

      if (tableEntity != null && tableEntity.getEntity() != null) {
        // Query for all alerts related to this table
        String dslQuery =
            String.format(
                "from MonteCarloAlert where table = '%s' order by createdTime desc",
                tableEntity.getEntity().getGuid());

        AtlasSearchResult result = atlasClient.dslSearch(dslQuery);

        // Get full entity details for each alert
        for (AtlasEntityHeader header : result.getEntities()) {
          AtlasEntity.AtlasEntityWithExtInfo alertEntity =
              atlasClient.getEntityByGuid(header.getGuid());
          alerts.add(alertEntity.getEntity());
        }
      }
    } catch (Exception e) {
      log.error("Error fetching alerts for table: {}", tableId, e);
      throw e;
    }

    return alerts;
  }

  public List<AtlasClassification> getTableClassifications(String tableId)
      throws AtlasServiceException {
    try {
      Map<String, String> attributes = Collections.singletonMap("qualifiedName", tableId);
      AtlasEntity.AtlasEntityWithExtInfo tableEntity =
          atlasClient.getEntityByAttribute("Table", attributes);

      if (tableEntity != null && tableEntity.getEntity() != null) {
        return atlasClient.getClassifications(tableEntity.getEntity().getGuid()).getList();
      }
      return Collections.emptyList();
    } catch (Exception e) {
      log.error("Error fetching classifications for table: {}", tableId, e);
      throw e;
    }
  }
}
