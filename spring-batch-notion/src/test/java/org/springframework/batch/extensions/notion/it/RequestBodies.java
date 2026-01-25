/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.extensions.notion.it;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.batch.extensions.notion.Sort.Direction;
import org.springframework.batch.extensions.notion.Sort.Timestamp;

import java.util.UUID;

/**
 * @author Stefano Cordio
 */
public class RequestBodies {

	public static String queryRequest(int pageSize, JSONObject... sorts) {
		return queryRequest(null, pageSize, sorts);
	}

	public static String queryRequest(UUID startCursor, int pageSize, JSONObject... sorts) {
		try {
			JSONObject jsonObject = new JSONObject();

			if (sorts.length > 0) {
				jsonObject.put("sorts", new JSONArray(sorts));
			}

			return jsonObject //
				.put("page_size", pageSize)
				.putOpt("start_cursor", startCursor != null ? startCursor.toString() : null)
				.toString();
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static JSONObject sortByProperty(String property, Direction direction) {
		try {
			return new JSONObject() //
				.put("property", property)
				.put("direction", direction.name().toLowerCase());
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static JSONObject sortByTimestamp(String property, Timestamp timestamp) {
		try {
			return new JSONObject() //
				.put("property", property)
				.put("timestamp", timestamp.name().toLowerCase());
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

}
