# Browser Integration Tests for msgpackr Web Streams

Browser tests that verify msgpackr Web Streams work correctly in real browser environments.

## Files

- **`browser-integration.html`** - Browser test runner
- **`test-web-stream.js`** - Node.js compatible unit tests

## Running the Tests

Start a local web server:

```bash
# Using Python 3
python3 -m http.server 8080

# Using Node.js
npx http-server -p 8080
```

Then open: `http://localhost:8080/tests/browser-integration.html`

**Note**: Direct file opening (`file://`) won't work due to ES module CORS restrictions.

## What's Tested

- **Web Streams API** - `UnpackrStream()` and `PackrStream()` create proper TransformStreams
- **Data types** - `Uint8Array`, `ArrayBuffer`, chunked data, multiple values per chunk
- **Real-world scenarios** - Network chunking, large datasets (backpressure), round-trip pipelines, records
- **Error handling** - Invalid tokens, incomplete data, error propagation

## Browser Compatibility

All tests pass on modern browsers:
- **Chrome/Edge** 89+ (March 2021)
- **Firefox** 102+ (June 2022)
- **Safari** 14.1+ (April 2021)
