package io.pkb.integration.wayfinder;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Google cloud function to proxy FHIR server requests to an upstream,
 * and enrich them with data to meet the Wayfinder integration spec.
 */
public class FhirProxyFunction implements HttpFunction {
    private static final String UPSTREAM_SCHEME = "https";
    private static final String UPSTREAM_HOST = "rcaggregated.fhir-api.sandbox.patientsknowbest.com";
    private static final int UPSTREAM_PORT = 443;
    // Copy of jdkinternal.net.http.common.Utils.DISALLOWED_HEADERS_SET
    private static final Set<String> BANNED_HEADERS = Set.of("connection", "content-length", "expect", "host", "upgrade");
    
    private final HttpClient httpClient;
    private final FhirContext r4FhirCtx;
    
    private final Map<Class<?>, ResourceEnricher<?>> enrichers;

    public FhirProxyFunction() throws Exception {
        this.httpClient = HttpClient.newHttpClient();
        this.r4FhirCtx = FhirContext.forR4Cached();
        this.enrichers = Stream.of(
                    new AppointmentEnricher(r4FhirCtx.newRestfulGenericClient(new URI(UPSTREAM_SCHEME, UPSTREAM_HOST,"/fhir", "").toString()))
                ).collect(Collectors.toMap(t -> t.type(), t -> t));
    }

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        var proxyRequest = createProxyRequest(httpRequest);
        var proxyResponse = httpClient.send(proxyRequest, BodyHandlers.ofInputStream());
        httpResponse.setStatusCode(proxyResponse.statusCode());
        headersStreamFromMap(proxyResponse.headers().map())
                // We're potentially modifying the content from upstream, don't pass on an invalid content-length.
                .filter(h -> !h.key.equalsIgnoreCase("content-length"))
                .forEach(h -> httpResponse.appendHeader(h.key, h.value));
        if (proxyResponse.statusCode() == HttpURLConnection.HTTP_OK) {
            try {
                var res = r4FhirCtx.newJsonParser().parseResource(proxyResponse.body());
                enrichResource(res);
                r4FhirCtx.newJsonParser().encodeResourceToWriter(res, httpResponse.getWriter());
            } catch (DataFormatException dfe) {
                httpResponse.setStatusCode(500);
                httpResponse.getWriter().write(dfe.toString());
            }
        } else {
            // Don't try to parse it or anything just pump it back.
            IOUtils.copy(proxyResponse.body(), httpResponse.getOutputStream());
        }
    }

    private void enrichResource(IBaseResource res) {
        if (res instanceof Bundle b) {
            b.getEntry().stream()
                    .map(Bundle.BundleEntryComponent::getResource)
                    .forEach(this::enrichResource);
        } else {
            var type = res.getClass();
            Optional.ofNullable(enrichers.get(type))
                    .ifPresent(e -> e.enrich(e.type().cast(res)));
        }
    }

    private java.net.http.HttpRequest createProxyRequest(HttpRequest googRequest) throws URISyntaxException, IOException {
        var builder = java.net.http.HttpRequest.newBuilder();
        builder = builder.method(googRequest.getMethod(), BodyPublishers.ofInputStream(() -> {
            try {
                return googRequest.getInputStream();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        // Translate the URI
        var originalUri = new URI(googRequest.getUri());
        builder = builder.uri(new URI(UPSTREAM_SCHEME,
                originalUri.getUserInfo(),
                UPSTREAM_HOST,
                UPSTREAM_PORT,
                originalUri.getPath(),
                originalUri.getQuery(),
                originalUri.getFragment()));

        // Copy over headers
        builder = headersStreamFromMap(googRequest.getHeaders())
                .filter(h -> !BANNED_HEADERS.contains(h.key.toLowerCase()))
                .reduce(builder,
                        (b, h) -> b.header(h.key, h.value),
                        (b, b2) -> b.headers(toStringArray(headersStreamFromMap(b2.build().headers().map()))));

        return builder.build();
    }

    /**
     * Handle the dissonance between com.google.cloud.functions.HttpRequest
     * and java.net.http.HttpRequest.
     */
    record Header(String key, String value) {}

    private Stream<Header> headersStreamFromMap(Map<String, List<String>> m) {
        return m.entrySet().stream().flatMap(e -> e.getValue().stream().map(v -> new Header(e.getKey(), v)));
    }

    private String[] toStringArray(Stream<Header> s) {
        return s.flatMap(h -> Stream.of(h.key, h.value))
                .toArray(String[]::new);
    }
}
