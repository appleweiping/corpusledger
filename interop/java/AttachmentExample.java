// Original pinned binary-read oracle, not a general CorpusLedger Java SDK.
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

public final class AttachmentExample {
    private static final int RAW_LIMIT = 4 * 1024 * 1024;
    private static final int WIRE_LIMIT = 16 * 1024 * 1024;
    private static final JsonFactory JSON = JsonFactory.builder()
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(40)
            .maxNumberLength(10000).maxStringLength(WIRE_LIMIT).build()).build();

    private AttachmentExample() { }

    private static String text(Object value) {
        if (!(value instanceof String result)) throw new IllegalArgumentException();
        for (int i = 0; i < result.length(); i++) {
            char c = result.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i == result.length() || !Character.isLowSurrogate(result.charAt(i)))
                    throw new IllegalArgumentException();
            } else if (Character.isLowSurrogate(c)) throw new IllegalArgumentException();
        }
        return result;
    }

    private static Map<?, ?> object(Object value) {
        if (!(value instanceof Map<?, ?> result)) throw new IllegalArgumentException();
        return result;
    }

    private static long integer(Object value) {
        if (!(value instanceof BigInteger number)) throw new IllegalArgumentException();
        return number.longValueExact();
    }

    private static Object readValue(JsonParser parser, int[] nodes) throws IOException {
        if (++nodes[0] > 2000000) throw new IOException();
        JsonToken token = parser.currentToken();
        if (token == JsonToken.START_OBJECT) {
            Map<String, Object> result = new LinkedHashMap<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) throw new IOException();
                String key = text(parser.currentName());
                parser.nextToken();
                result.put(key, readValue(parser, nodes));
            }
            return result;
        }
        if (token == JsonToken.START_ARRAY) {
            List<Object> result = new ArrayList<>();
            while (parser.nextToken() != JsonToken.END_ARRAY) result.add(readValue(parser, nodes));
            return result;
        }
        if (token == JsonToken.VALUE_STRING) return text(parser.getText());
        if (token == JsonToken.VALUE_NUMBER_INT) return parser.getBigIntegerValue();
        if (token == JsonToken.VALUE_NUMBER_FLOAT) return parser.getDecimalValue();
        if (token == JsonToken.VALUE_TRUE) return Boolean.TRUE;
        if (token == JsonToken.VALUE_FALSE) return Boolean.FALSE;
        if (token == JsonToken.VALUE_NULL) return null;
        throw new IOException();
    }

    private static Map<?, ?> parse(byte[] raw) throws IOException {
        // Jackson's byte parser alone can accept UTF-8 surrogate encodings.
        String decoded = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
        try (JsonParser parser = JSON.createParser(decoded)) {
            parser.nextToken();
            Map<?, ?> result = object(readValue(parser, new int[]{0}));
            if (parser.nextToken() != null) throw new IOException();
            return result;
        }
    }

    private static void writeValue(JsonGenerator generator, Object value) throws IOException {
        if (value instanceof Map<?, ?> map) {
            generator.writeStartObject();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                generator.writeFieldName(text(entry.getKey()));
                writeValue(generator, entry.getValue());
            }
            generator.writeEndObject();
        } else if (value instanceof String string) generator.writeString(string);
        else if (value instanceof Long number) generator.writeNumber(number);
        else if (value instanceof Integer number) generator.writeNumber(number);
        else if (value instanceof Boolean flag) generator.writeBoolean(flag);
        else throw new IOException();
    }

    private static byte[] encode(Map<String, Object> value) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (JsonGenerator generator = JSON.createGenerator(output)) { writeValue(generator, value); }
        return output.toByteArray();
    }

    private static byte[] bounded(InputStream stream, int maximum) throws IOException {
        byte[] raw = stream.readNBytes(maximum + 1);
        if (raw.length > maximum) throw new IOException();
        return raw;
    }

    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private Flow.Subscription subscription;
        public CompletionStage<byte[]> getBody() { return result; }
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            value.request(1);
        }
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > WIRE_LIMIT - bytes.size()) {
                    subscription.cancel();
                    result.completeExceptionally(new IOException());
                    return;
                }
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);
                bytes.writeBytes(data);
            }
            subscription.request(1);
        }
        public void onError(Throwable error) { result.completeExceptionally(new IOException()); }
        public void onComplete() { result.complete(bytes.toByteArray()); }
    }

    private static void run() throws Exception {
        Map<?, ?> config = parse(bounded(System.in, 65536));
        if (!config.keySet().equals(Set.of("endpoint", "event_id", "name", "payload_file", "revision", "digest")))
            throw new IllegalArgumentException();
        URI endpoint = URI.create(text(config.get("endpoint")));
        if (!"http".equals(endpoint.getScheme()) || !"127.0.0.1".equals(endpoint.getHost())
            || endpoint.getPort() < 1 || endpoint.getPort() > 65535 || endpoint.getRawUserInfo() != null
            || !endpoint.getRawPath().isEmpty() || endpoint.getRawQuery() != null || endpoint.getRawFragment() != null)
            throw new IllegalArgumentException();
        String token = System.getenv("CORPUSLEDGER_ATTACHMENT_ORACLE_TOKEN");
        if (token == null || !token.matches("[A-Za-z0-9_-]{32,128}")) throw new IllegalArgumentException();
        String eventId = text(config.get("event_id"));
        String name = text(config.get("name"));
        String digest = text(config.get("digest"));
        long revision = integer(config.get("revision"));
        if (revision < 1 || !digest.matches("[0-9a-f]{64}")) throw new IllegalArgumentException();
        Path file = Path.of(text(config.get("payload_file")));
        if (!Files.isRegularFile(file) || Files.size(file) > RAW_LIMIT) throw new IllegalArgumentException();
        byte[] expected;
        try (InputStream stream = Files.newInputStream(file)) { expected = bounded(stream, RAW_LIMIT); }
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(expected));
        byte[] requestBody = encode(Map.of("format", "corpusledger.event-command.v1",
            "command", "attachment_get", "arguments", Map.of("event_id", eventId, "name", name,
                "revision", revision, "expected_digest", digest)));
        if (requestBody.length > WIRE_LIMIT) throw new IllegalArgumentException();
        ProxySelector noProxy = new ProxySelector() {
            public List<Proxy> select(URI ignored) { return List.of(Proxy.NO_PROXY); }
            public void connectFailed(URI ignored, SocketAddress address, IOException error) { }
        };
        HttpResponse<byte[]> response;
        try (HttpClient client = HttpClient.newBuilder().proxy(noProxy)
                .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(5)).build()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + "/v1/events"))
                .timeout(Duration.ofSeconds(30)).header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody)).build();
            CompletableFuture<HttpResponse<byte[]>> pending = client.sendAsync(request, ignored -> new LimitedBody());
            try { response = pending.get(30, TimeUnit.SECONDS); }
            catch (Exception error) { pending.cancel(true); throw new IOException(); }
        }
        if (response.statusCode() != 200 || response.headers().firstValue("content-encoding").isPresent())
            throw new IOException();
        Map<?, ?> envelope = parse(response.body());
        if (!envelope.keySet().equals(Set.of("format", "command", "ok", "result"))
            || !"corpusledger.event-response.v1".equals(envelope.get("format"))
            || !"attachment_get".equals(envelope.get("command")) || !Boolean.TRUE.equals(envelope.get("ok")))
            throw new IOException();
        Map<?, ?> result = object(envelope.get("result"));
        if (!result.keySet().equals(Set.of("format", "event_id", "revision", "digest", "attachment", "data"))
            || !"corpusledger.attachment-data.v1".equals(result.get("format"))
            || !eventId.equals(result.get("event_id")) || revision != integer(result.get("revision"))
            || !digest.equals(result.get("digest"))) throw new IOException();
        Map<?, ?> manifest = object(result.get("attachment"));
        if (!manifest.keySet().equals(Set.of("name", "sha256", "size", "media_type"))
            || !name.equals(manifest.get("name")) || !sha.equals(manifest.get("sha256"))
            || expected.length != integer(manifest.get("size"))
            || !"application/octet-stream".equals(manifest.get("media_type"))) throw new IOException();
        String encoded = text(result.get("data"));
        if (encoded.length() > 4 * ((RAW_LIMIT + 2) / 3)) throw new IOException();
        byte[] actual = Base64.getDecoder().decode(encoded);
        if (!Base64.getEncoder().encodeToString(actual).equals(encoded) || !Arrays.equals(actual, expected)
            || !sha.equals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(actual))))
            throw new IOException();
        System.out.write(encode(Map.of("format", "corpusledger.attachment-read-oracle.v1", "verified", true,
            "event_id", eventId, "revision", revision, "digest", digest, "sha256", sha, "size", actual.length, "name", name)));
        System.out.println();
    }

    public static void main(String[] args) {
        try {
            if (args.length != 0) throw new IllegalArgumentException();
            run();
        } catch (Exception error) {
            System.err.println("attachment_read_failed");
            System.exit(2);
        }
    }
}
