package com.atlan.montecarlo.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasBuiltInTypes;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeUtil;
import java.util.*;

@Slf4j
public class AtlasService {
  private final AtlasClientV2 atlasClient;

  public AtlasService(String atlasUrl, String username, String password) {
    this.atlasClient =
        new AtlasClientV2(new String[] {atlasUrl}, new String[] {username, password});
  }

  public void updateTableMetadata(
      String tableId, String issueType, String severity, Map<String, Object> metadata) {
    try {
      // Create or update the table entity
      AtlasEntity.AtlasEntityWithExtInfo tableEntity = getOrCreateTableEntity(tableId);

      // Add Monte Carlo classification
      addMonteCarloClassification(tableEntity.getEntity().getGuid(), issueType, severity, metadata);

      log.info("Successfully updated Atlas metadata for table: {}", tableId);
    } catch (Exception e) {
      log.error("Error updating Atlas metadata for table: {}", tableId, e);
      throw new RuntimeException("Failed to update Atlas metadata", e);
    }
  }

  private AtlasEntity.AtlasEntityWithExtInfo getOrCreateTableEntity(String tableId)
      throws Exception {
    try {
      // Try to find existing entity
      Map<String, String> attributes = new HashMap<>();
      attributes.put("qualifiedName", tableId);

      List<Map<String, String>> entityFilters = new ArrayList<>();
      entityFilters.add(attributes);

      List<AtlasEntity> entities =
          atlasClient.getEntitiesByAttribute("Table", entityFilters).getEntities();

      if (!entities.isEmpty()) {
        return atlasClient.getEntityByGuid(entities.get(0).getGuid());
      }

      // Create new entity if not found
      AtlasEntity entity = new AtlasEntity("Table");
      entity.setAttribute("qualifiedName", tableId);
      entity.setAttribute("name", tableId.substring(tableId.lastIndexOf('.') + 1));

      AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo =
          new AtlasEntity.AtlasEntityWithExtInfo(entity);

      EntityMutationResponse response = atlasClient.createEntity(entityWithExtInfo);
      AtlasEntityHeader entityHeader = response.getFirstEntityCreated();

      return atlasClient.getEntityByGuid(entityHeader.getGuid());

    } catch (Exception e) {
      log.error("Error getting/creating table entity: {}", tableId, e);
      throw e;
    }
  }

  private void addMonteCarloClassification(
      String entityGuid, String issueType, String severity, Map<String, Object> metadata)
      throws Exception {
    try {
      // Create classification instance
      AtlasClassification classification = new AtlasClassification("MonteCarloIssue");

      // Set attributes
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("issueType", issueType);
      attributes.put("severity", severity);
      attributes.put("lastUpdated", new Date());
      attributes.put("metadata", metadata);
      classification.setAttributes(attributes);

      // Associate classification with entity
      List<AtlasClassification> classifications = Collections.singletonList(classification);
      atlasClient.addClassifications(entityGuid, classifications);
    } catch (Exception e) {
      log.error("Error adding classification to entity: {}", entityGuid, e);
      throw e;
    }
  }

  public void createMonteCarloClassificationType() throws Exception {
    try {
      AtlasClassificationDef classificationDef = new AtlasClassificationDef("MonteCarloIssue");

      // Create attribute definitions
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

      // Create the classification type
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
