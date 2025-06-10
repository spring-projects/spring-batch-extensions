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

package org.springframework.batch.extensions.bigquery.emulator.writer.base;

import com.github.tomakehurst.wiremock.extension.ResponseTransformerV2;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.springframework.batch.extensions.bigquery.emulator.base.BigQueryBaseDockerConfiguration;
import wiremock.com.google.common.net.HttpHeaders;

import java.util.List;
import java.util.function.Predicate;

public final class SpyResponseExtension implements ResponseTransformerV2 {

    private static final String BQ_DOCKER_URL_PREFIX = "http://0.0.0.0:";
    private static final String BQ_DOCKER_URL = BQ_DOCKER_URL_PREFIX + BigQueryBaseDockerConfiguration.PORT;

    private int wireMockPort;

    @Override
    public Response transform(Response response, ServeEvent serveEvent) {
        var originalHeaders = response.getHeaders();
        HttpHeader originalLocationHeader = originalHeaders.getHeader(HttpHeaders.LOCATION);

        List<String> locationHeaderValues = originalLocationHeader.getValues();
        boolean containsLocationHeader = locationHeaderValues.stream().anyMatch(s -> s.startsWith(BQ_DOCKER_URL));

        if (containsLocationHeader) {
            if (locationHeaderValues.size() > 1) {
                throw new IllegalStateException();
            }

            List<HttpHeader> headersWithoutLocation = originalHeaders
                    .all()
                    .stream()
                    .filter(Predicate.not(hh -> hh.keyEquals(HttpHeaders.LOCATION)))
                    .toList();

            HttpHeader updatedHeader = HttpHeader.httpHeader(
                    HttpHeaders.LOCATION, locationHeaderValues.get(0).replace(BQ_DOCKER_URL, BQ_DOCKER_URL_PREFIX + wireMockPort)
            );

            return Response.Builder
                    .like(response)
                    .but()
                    .headers(new com.github.tomakehurst.wiremock.http.HttpHeaders(headersWithoutLocation).plus(updatedHeader))
                    .build();
        }
        return response;
    }

    @Override
    public String getName() {
        return "spy-response-extension";
    }

    public void setWireMockPort(int wireMockPort) {
        this.wireMockPort = wireMockPort;
    }
}