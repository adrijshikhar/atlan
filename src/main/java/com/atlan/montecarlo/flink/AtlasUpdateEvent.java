/*
                                * Copyright (c) 2025 Atlan Inc.
                                */
package com.atlan.montecarlo.flink;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AtlasUpdateEvent {
  private String tableId;
  private String issueType;
  private String severity;
  private String metadata;
}
