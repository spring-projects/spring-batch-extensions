package org.springframework.batch.extensions.notion;

import java.util.Map;

record Page(Map<String, PageProperty> properties) {
}
