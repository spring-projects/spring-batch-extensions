/*
 * Copyright 2002-2025 the original author or authors.
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

	public static String queryResponse(JSONObject... results) {
		return queryResponse(null, results);
	}

	public static String queryResponse(UUID nextCursor, JSONObject... results) {
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

	public static JSONObject result(UUID id, UUID databaseId, Map<?, ?> properties) {
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
					.put("type", "database_id")
					.put("database_id", databaseId.toString()))
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
