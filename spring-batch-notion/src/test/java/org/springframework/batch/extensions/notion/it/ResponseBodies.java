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

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static java.util.UUID.randomUUID;

/**
 * @author Stefano Cordio
 */
public class ResponseBodies {

	public static String databaseInfoResponse(UUID databaseId, UUID dataSourceId) {
		try {
			return new JSONObject() //
				.put("object", "database")
				.put("id", databaseId.toString())
				.put("data_sources", new JSONArray() //
					.put(new JSONObject() //
						.put("id", dataSourceId.toString())
						.put("name", "default")))
				.toString();
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static String databaseInfoResponse(UUID databaseId, UUID firstDataSourceId, UUID secondDataSourceId) {
		try {
			return new JSONObject() //
				.put("object", "database")
				.put("id", databaseId.toString())
				.put("data_sources", new JSONArray() //
					.put(new JSONObject() //
						.put("id", firstDataSourceId.toString())
						.put("name", "default"))
					.put(new JSONObject() //
						.put("id", secondDataSourceId.toString())
						.put("name", "secondary")))
				.toString();
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static String datasourceQueryResponse(JSONObject... results) {
		return datasourceQueryResponse(null, results);
	}

	public static String datasourceQueryResponse(UUID nextCursor, JSONObject... results) {
		try {
			return new JSONObject() //
				.put("object", "list")
				.put("results", new JSONArray(results))
				.put("next_cursor", nextCursor != null ? nextCursor.toString() : null)
				.put("has_more", nextCursor != null)
				.put("type", "page")
				.put("page", new JSONObject())
				.toString();
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static JSONObject result(UUID id, UUID dataSourceId, Map<?, ?> properties) {
		try {
			Instant now = Instant.now();

			return new JSONObject() //
				.put("object", "page")
				.put("id", id.toString())
				.put("created_time", now.toString())
				.put("last_edited_time", now.toString())
				.put("created_by", new JSONObject())
				.put("last_edited_by", new JSONObject())
				.put("parent", new JSONObject() //
					.put("type", "data_source_id")
					.put("data_source_id", dataSourceId.toString())
					.put("database_id", randomUUID().toString()))
				.put("archived", false)
				.put("properties", new JSONObject(properties))
				.put("url", "https://www.notion.so/" + randomUUID().toString().replace("-", ""));
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static JSONObject title(String value) {
		try {
			JSONArray jsonArray = new JSONArray();

			if (value != null) {
				jsonArray.put(new JSONObject() //
					.put("type", "text")
					.put("text", new JSONObject() //
						.put("content", value))
					.put("annotations", new JSONObject() //
						.put("bold", false)
						.put("italic", false)
						.put("strikethrough", false)
						.put("underline", false)
						.put("code", false)
						.put("color", "default"))
					.put("plain_text", value));
			}

			return new JSONObject() //
				.put("id", "title")
				.put("type", "title")
				.put("title", jsonArray);
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static JSONObject richText(String value) {
		try {
			return new JSONObject() //
				.put("id", "JV%3B%3F")
				.put("type", "rich_text")
				.put("rich_text", new JSONArray() //
					.put(new JSONObject() //
						.put("type", "text")
						.put("text", new JSONObject() //
							.put("content", value))
						.put("annotations", new JSONObject() //
							.put("bold", false)
							.put("italic", false)
							.put("strikethrough", false)
							.put("underline", false)
							.put("code", false)
							.put("color", "default"))
						.put("plain_text", value)));
		}
		catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

}
