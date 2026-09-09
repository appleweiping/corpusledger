import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** A loopback-only typed reference annotator; no reflection/databind or external calls. */
public final class ReferenceWorker {
    static final int MAX_WIRE = 16 * 1024 * 1024;
    static final String JACKSON_SHA = "36111c3a4372cd5c2be6f4ec44a050382487920f3fc38ca3015c9c1678bd7c56";
    static final JsonFactory JSON = JsonFactory.builder()
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(64)
            .maxDocumentLength(MAX_WIRE).maxStringLength(MAX_WIRE)
            .maxNameLength(MAX_WIRE).maxNumberLength(4301).build()).build();

    private ReferenceWorker() {}

    static IllegalArgumentException invalid() { return new IllegalArgumentException("invalid_request"); }

    static Map<String,Object> map(Object... pairs) {
        Map<String,Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String)pairs[i], pairs[i+1]);
        return result;
    }

    static String text(Object value) {
        if (!(value instanceof String s)) throw invalid();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= s.length() || !Character.isLowSurrogate(s.charAt(i))) throw invalid();
            } else if (Character.isLowSurrogate(c)) throw invalid();
        }
        return s;
    }

    static String name(Object value) {
        String s = text(value);
        if (s.isEmpty() || s.codePoints().allMatch(ReferenceWorker::pythonWhitespace)
            || s.codePointCount(0,s.length()) > 256) throw invalid();
        return s;
    }

    static boolean pythonWhitespace(int c) {
        return c >= 9 && c <= 13 || c >= 0x1C && c <= 0x20 || c == 0x85 || c == 0xA0 || c == 0x1680
            || c >= 0x2000 && c <= 0x200A || c == 0x2028 || c == 0x2029 || c == 0x202F || c == 0x205F || c == 0x3000;
    }

    @SuppressWarnings("unchecked")
    static Map<String,Object> object(Object value, String... keys) {
        if (!(value instanceof Map<?,?> m) || !m.keySet().stream().allMatch(k -> k instanceof String)) throw invalid();
        if (keys.length != 0 && !m.keySet().equals(Set.of(keys))) throw invalid();
        return (Map<String,Object>)m;
    }

    static List<?> array(Object value) { if (!(value instanceof List<?> v)) throw invalid(); return v; }
    static String digest(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException("digest_unavailable"); }
    }

    static Object read(JsonParser p, JsonToken token, int depth) throws IOException {
        if (depth > 64 || token == null) throw invalid();
        switch (token) {
            case START_OBJECT: {
                Map<String,Object> result = new LinkedHashMap<>();
                while (p.nextToken() != JsonToken.END_OBJECT) {
                    if (p.currentToken() != JsonToken.FIELD_NAME) throw invalid();
                    String key = text(p.currentName());
                    if (result.containsKey(key)) throw invalid();
                    result.put(key, read(p,p.nextToken(),depth+1));
                }
                return result;
            }
            case START_ARRAY: {
                List<Object> result = new ArrayList<>();
                JsonToken next;
                while ((next = p.nextToken()) != JsonToken.END_ARRAY) result.add(read(p,next,depth+1));
                return result;
            }
            case VALUE_STRING: return text(p.getText());
            case VALUE_TRUE: return Boolean.TRUE;
            case VALUE_FALSE: return Boolean.FALSE;
            case VALUE_NULL: return null;
            case VALUE_NUMBER_INT: {
                String n = p.getText();
                if ((n.startsWith("-") ? n.length()-1 : n.length()) > 4300) throw invalid();
                return new BigInteger(n);
            }
            case VALUE_NUMBER_FLOAT: {
                double n = p.getDoubleValue();
                if (!Double.isFinite(n)) throw invalid();
                return n;
            }
            default: throw invalid();
        }
    }

    static Object decode(byte[] raw) throws IOException {
        if (raw.length > MAX_WIRE) throw invalid();
        // Explicit UTF-8 checking precedes JSON parsing; escaped surrogates are
        // separately validated in every parsed string and field name.
        StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw));
        try (JsonParser p = JSON.createParser(raw)) {
            Object result = read(p,p.nextToken(),0);
            if (p.nextToken() != null) throw invalid();
            return result;
        }
    }

    static void write(JsonGenerator g, Object value) throws IOException {
        if (value == null) g.writeNull();
        else if (value instanceof String s) g.writeString(s);
        else if (value instanceof Boolean b) g.writeBoolean(b);
        else if (value instanceof BigInteger n) g.writeNumber(n);
        else if (value instanceof Integer n) g.writeNumber(n);
        else if (value instanceof Long n) g.writeNumber(n);
        else if (value instanceof Double n && Double.isFinite(n)) g.writeNumber(n);
        else if (value instanceof Map<?,?> m) {
            g.writeStartObject();
            for (Map.Entry<?,?> entry : m.entrySet()) { g.writeFieldName((String)entry.getKey()); write(g,entry.getValue()); }
            g.writeEndObject();
        } else if (value instanceof List<?> list) {
            g.writeStartArray(); for (Object item : list) write(g,item); g.writeEndArray();
        } else throw invalid();
    }

    static byte[] encode(Object value) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        // Enforce the bound incrementally, before retaining an oversized report.
        OutputStream bounded = new OutputStream() {
            public void write(int b) throws IOException { if (result.size() >= MAX_WIRE) throw new IOException("output_limit"); result.write(b); }
            public void write(byte[] b,int off,int len) throws IOException { if (len > MAX_WIRE-result.size()) throw new IOException("output_limit"); result.write(b,off,len); }
        };
        try (JsonGenerator g = JSON.createGenerator(bounded)) { write(g,value); }
        return result.toByteArray();
    }

    static int offset(Object value,int maximum) {
        if (!(value instanceof BigInteger n) || n.signum() < 0 || n.compareTo(BigInteger.valueOf(maximum)) > 0) throw invalid();
        return n.intValueExact();
    }

    static Map<String,Object> field(String kind,Object target) { return map("kind",kind,"required",true,"nullable",false,"target_type",target); }
    static Map<String,Object> tokenSchema() { return map("name","token","fields",map("text",field("string",null),"position",field("integer",null))); }
    static Map<String,Object> groupSchema() { return map("name","token_group","fields",map("members",field("references","token"),"count",field("integer",null))); }
    static Map<String,Object> description(String config) {
        return map("format","corpusledger.processor.v1","name","demo.java.group","version","1","config_sha256",config,
            "requires",List.of(tokenSchema()),"produces",List.of(groupSchema()));
    }

    static void featureDepth(Object value,int depth) {
        if (depth > 32) throw invalid();
        if (value instanceof Map<?,?> m) for (Object v : m.values()) featureDepth(v,depth+1);
        if (value instanceof List<?> a) for (Object v : a) featureDepth(v,depth+1);
    }

    static void feature(Object value,Map<String,Object> f,Map<String,Map<String,Object>> annotations) {
        String kind = text(f.get("kind"));
        boolean valid = switch (kind) {
            case "string", "reference" -> value instanceof String;
            case "integer" -> value instanceof BigInteger;
            case "number" -> value instanceof BigInteger || value instanceof Double;
            case "boolean" -> value instanceof Boolean;
            case "object" -> value instanceof Map<?,?>;
            case "array", "references" -> value instanceof List<?>;
            default -> false;
        };
        if (!valid) throw invalid();
        if (kind.equals("reference") || kind.equals("references")) {
            List<?> refs = kind.equals("reference") ? List.of(value) : array(value);
            for (Object ref : refs) {
                Map<String,Object> target = annotations.get(text(ref));
                if (target == null || f.get("target_type") != null && !f.get("target_type").equals(target.get("type"))) throw invalid();
            }
        }
    }

    static Map<String,Object> document(Object value) {
        Map<String,Object> doc = object(value,"format","id","text","text_sha256","offset_unit","types","annotations");
        if (!"corpusledger.annotations.v1".equals(doc.get("format")) || !"unicode_codepoint".equals(doc.get("offset_unit"))) throw invalid();
        name(doc.get("id")); String text = text(doc.get("text")); int length = text.codePointCount(0,text.length());
        if (length > 10_000_000 || !digest(text.getBytes(StandardCharsets.UTF_8)).equals(doc.get("text_sha256"))) throw invalid();
        Map<String,Map<String,Object>> schemas = new HashMap<>();
        for (Object raw : array(doc.get("types"))) {
            Map<String,Object> schema = object(raw,"name","fields"); String n = name(schema.get("name"));
            if (schemas.putIfAbsent(n,schema) != null) throw invalid();
            for (Map.Entry<String,Object> e : object(schema.get("fields")).entrySet()) {
                name(e.getKey()); Map<String,Object> f = object(e.getValue(),"kind","required","nullable","target_type");
                String kind = text(f.get("kind"));
                if (!Set.of("string","integer","number","boolean","object","array","reference","references").contains(kind)
                    || !(f.get("required") instanceof Boolean) || !(f.get("nullable") instanceof Boolean)) throw invalid();
                if (f.get("target_type") != null) { name(f.get("target_type")); if (!kind.equals("reference") && !kind.equals("references")) throw invalid(); }
            }
        }
        for (Map<String,Object> schema : schemas.values()) for (Object raw : object(schema.get("fields")).values()) {
            Object target = object(raw).get("target_type"); if (target != null && !schemas.containsKey(target)) throw invalid();
        }
        if (!tokenSchema().equals(schemas.get("token")) || schemas.containsKey("token_group")) throw invalid();
        List<?> rows = array(doc.get("annotations")); if (rows.size() >= 1_000_000) throw invalid();
        Map<String,Map<String,Object>> annotations = new HashMap<>();
        for (Object raw : rows) {
            Map<String,Object> a = object(raw,"id","type","start","end","features"); String id = name(a.get("id"));
            if (annotations.putIfAbsent(id,a) != null || !schemas.containsKey(name(a.get("type")))) throw invalid();
            if (offset(a.get("end"),length) < offset(a.get("start"),length)) throw invalid();
            featureDepth(object(a.get("features")),0);
        }
        for (Map<String,Object> a : annotations.values()) {
            Map<String,Object> fields = object(schemas.get(a.get("type")).get("fields")), features = object(a.get("features"));
            if (!fields.keySet().containsAll(features.keySet())) throw invalid();
            for (Map.Entry<String,Object> e : fields.entrySet()) {
                Map<String,Object> f = object(e.getValue());
                if (!features.containsKey(e.getKey())) { if (Boolean.TRUE.equals(f.get("required"))) throw invalid(); continue; }
                Object v = features.get(e.getKey()); if (v == null && Boolean.TRUE.equals(f.get("nullable"))) continue;
                feature(v,f,annotations);
            }
        }
        if (annotations.containsKey("demo.java.group.0")) throw invalid();
        return doc;
    }

    static Map<String,Object> process(Object value,Map<String,Object> desc) {
        Map<String,Object> r = object(value,"format","operation_id","step_id","processor","input_digest","document");
        if (!"corpusledger.processor-request.v1".equals(r.get("format")) || !desc.equals(r.get("processor"))) throw invalid();
        for (String key : List.of("operation_id","step_id")) if (!text(r.get(key)).matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")) throw invalid();
        if (!text(r.get("input_digest")).matches("[0-9a-f]{64}")) throw invalid();
        Map<String,Object> doc = document(r.get("document")); String text = text(doc.get("text"));
        int codepoints = text.codePointCount(0,text.length());
        // Build once, not offsetByCodePoints(0,offset) for every token: repeated
        // scans make a many-token non-BMP document accidentally quadratic.
        int[] utf16 = new int[codepoints+1];
        for (int i = 0, at = 0; i < codepoints; i++) { utf16[i] = at; at += Character.charCount(text.codePointAt(at)); }
        utf16[codepoints] = text.length();
        List<Map<String,Object>> tokens = new ArrayList<>(); Set<BigInteger> positions = new HashSet<>();
        for (Object raw : array(doc.get("annotations"))) {
            Map<String,Object> a = object(raw);
            if (!a.get("type").equals("token")) continue;
            Map<String,Object> features = object(a.get("features")); BigInteger position = (BigInteger)features.get("position");
            int start = offset(a.get("start"),10_000_000), end = offset(a.get("end"),10_000_000);
            // Do not treat Java UTF-16 indices as wire codepoint offsets.
            String covered = text.substring(utf16[start],utf16[end]);
            if (position.signum() < 0 || !positions.add(position) || !covered.equals(features.get("text"))) throw invalid();
            tokens.add(a);
        }
        tokens.sort(Comparator.comparing((Map<String,Object> a) -> (BigInteger)object(a.get("features")).get("position")));
        List<Object> members = new ArrayList<>(); int start = 0,end = 0;
        for (Map<String,Object> a : tokens) {
            int left = offset(a.get("start"),10_000_000), right = offset(a.get("end"),10_000_000);
            if (members.isEmpty()) start = left; else start = Math.min(start,left);
            end = Math.max(end,right); members.add(a.get("id"));
        }
        Map<String,Object> group = map("id","demo.java.group.0","type","token_group","start",start,"end",end,
            "features",map("members",members,"count",members.size()));
        return map("format","corpusledger.processor-result.v1","operation_id",r.get("operation_id"),"step_id",r.get("step_id"),
            "processor",desc,"input_digest",r.get("input_digest"),"annotations",List.of(group),"duration_ms",0L);
    }

    static void respond(HttpExchange exchange,int status,byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type","application/json");
        exchange.getResponseHeaders().set("Connection","close");
        exchange.getResponseHeaders().set("Cache-Control","no-store");
        exchange.sendResponseHeaders(status,body.length); exchange.getResponseBody().write(body);
    }

    static void handle(HttpExchange exchange,Map<String,Object> desc,String token) throws IOException {
        try {
            List<String> auth = exchange.getRequestHeaders().get("Authorization");
            if (auth == null || auth.size() != 1 || !MessageDigest.isEqual(auth.get(0).getBytes(StandardCharsets.UTF_8),
                ("Bearer "+token).getBytes(StandardCharsets.UTF_8))) { respond(exchange,401,encode(map("error","request_rejected"))); return; }
            for (String header : List.of("Origin","Transfer-Encoding","Content-Encoding")) if (exchange.getRequestHeaders().containsKey(header)) throw invalid();
            String uri = exchange.getRequestURI().toString(), method = exchange.getRequestMethod();
            Object result;
            if (method.equals("GET") && uri.equals("/v1/info")) {
                List<String> sizes = exchange.getRequestHeaders().get("Content-Length");
                if (sizes != null && (sizes.size() != 1 || !sizes.get(0).equals("0"))) throw invalid();
                result = desc;
            } else if (method.equals("POST") && uri.equals("/v1/process")) {
                List<String> sizes = exchange.getRequestHeaders().get("Content-Length"), types = exchange.getRequestHeaders().get("Content-Type");
                if (sizes == null || sizes.size() != 1 || !sizes.get(0).matches("[0-9]{1,8}")
                    || types == null || !types.equals(List.of("application/json"))) throw invalid();
                int length = Integer.parseInt(sizes.get(0)); if (length > MAX_WIRE) throw invalid();
                byte[] raw = exchange.getRequestBody().readNBytes(length+1); if (raw.length != length) throw invalid();
                long start = System.nanoTime(); Map<String,Object> processed = process(decode(raw),desc);
                processed.put("duration_ms",TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start)); result = processed;
            } else { respond(exchange,404,encode(map("error","request_rejected"))); return; }
            respond(exchange,200,encode(result));
        } catch (IllegalArgumentException | IOException e) {
            // Decoder errors may contain caller text. Never reflect or log them.
            try { respond(exchange,400,encode(map("error","request_rejected"))); } catch (IOException ignored) { }
        } finally { exchange.close(); }
    }

    static Path jarPath(Class<?> type) throws Exception {
        Path path = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isRegularFile(path)) throw invalid(); return path;
    }

    static void run(String[] args) throws Exception {
        String host = "127.0.0.1", env = "CORPUSLEDGER_WORKER_TOKEN"; int port = 0;
        for (int i = 0; i < args.length; i += 2) {
            if (i+1 >= args.length) throw invalid();
            switch (args[i]) { case "--host": host=args[i+1]; break; case "--port": port=Integer.parseInt(args[i+1]); break;
                case "--token-env": env=args[i+1]; break; default: throw invalid(); }
        }
        if ((!host.equals("127.0.0.1") && !host.equals("::1")) || port < 0 || port > 65535) throw invalid();
        String token = System.getenv(env); if (token == null || !token.matches("[A-Za-z0-9._~-]{32,256}")) throw invalid();
        String library = digest(Files.readAllBytes(jarPath(JsonFactory.class)));
        if (!library.equals(JACKSON_SHA)) throw invalid();
        String semantics = "corpusledger.java-group-worker.v1\n"+digest(Files.readAllBytes(jarPath(ReferenceWorker.class)))+"\n"+library
            +"\n"+System.getProperty("java.runtime.version")+"\n"+System.getProperty("java.vm.name")+"\n"+System.getProperty("java.vendor")
            +"\nposition-BigInteger-order;codepoint-to-UTF16;no-normalization";
        Map<String,Object> desc = description(digest(semantics.getBytes(StandardCharsets.UTF_8)));
        // Set before the default JDK HTTP implementation initializes. These are
        // tested against Temurin 21; maxReq/RspTime use seconds in that runtime.
        System.setProperty("jdk.httpserver.maxConnections","8");
        System.setProperty("sun.net.httpserver.maxIdleConnections","0");
        System.setProperty("sun.net.httpserver.maxReqHeaders","32");
        System.setProperty("sun.net.httpserver.maxReqHeaderSize","8192");
        System.setProperty("sun.net.httpserver.maxReqTime","5");
        System.setProperty("sun.net.httpserver.maxRspTime","10");
        System.setProperty("sun.net.httpserver.drainAmount","0");
        Logger.getLogger("com.sun.net.httpserver").setLevel(Level.OFF);
        Logger.getLogger("sun.net.httpserver").setLevel(Level.OFF);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(8,8,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(8));
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(host),port),8);
        server.setExecutor(executor); server.createContext("/",e -> handle(e,desc,token));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { server.stop(2); executor.shutdownNow(); }));
        server.start();
        System.out.println("http://"+(host.equals("::1") ? "[::1]" : host)+":"+server.getAddress().getPort());
    }

    public static void main(String[] args) {
        try { run(args); } catch (Exception e) { System.err.println("worker_start_or_run_failed"); System.exit(2); }
    }
}
