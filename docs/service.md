# Local service boundary

`CorpusService` exposes the strict library operations used by CorpusLedger over
a small JSON dispatch API. `create_server()` returns a threaded
`http.server` instance with one endpoint, `POST /v1/dispatch`.

```python
from corpusledger import create_server

server = create_server(host="127.0.0.1", port=8080)
server.serve_forever()
```

Requests use an explicit `operation` (`manifest`, `diff`, or `verify_bundle`)
and filesystem paths. The default loopback binding is intentional; add an
authenticated reverse proxy before exposing the process outside a trusted
machine. Request bodies are bounded at 4 MiB and all operations reuse the
library's strict parsing and digest verification.
