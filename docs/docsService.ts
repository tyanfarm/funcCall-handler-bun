function html(data: string, status = 200): Response {
  return new Response(data, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function renderDocsPage(baseUrl: string): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>TCU API Docs</title>
  <style>
    :root {
      --bg: #f6f7fb;
      --panel: #ffffff;
      --text: #1a1f36;
      --muted: #5f6b85;
      --line: #dbe1ee;
      --accent: #0b57d0;
      --post: #1e7f3b;
      --get: #0059b3;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      background: linear-gradient(180deg, #eef2fb 0%, #f8fafc 100%);
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      color: var(--text);
    }

    .wrap {
      max-width: 1080px;
      margin: 0 auto;
      padding: 24px;
    }

    .hero {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 20px;
      margin-bottom: 20px;
    }

    h1 {
      margin: 0 0 8px;
      font-size: 28px;
    }

    p {
      margin: 6px 0;
      color: var(--muted);
    }

    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
    }

    .row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin-bottom: 10px;
    }

    .method {
      display: inline-block;
      min-width: 54px;
      text-align: center;
      color: #fff;
      border-radius: 6px;
      padding: 4px 8px;
      font-size: 12px;
      font-weight: 700;
    }

    .method.post { background: var(--post); }
    .method.get { background: var(--get); }

    code {
      background: #f3f6ff;
      border: 1px solid #e0e8fb;
      padding: 2px 6px;
      border-radius: 6px;
      color: #21315f;
    }

    pre {
      margin: 0;
      background: #0f172a;
      color: #dbeafe;
      border-radius: 10px;
      padding: 10px;
      overflow: auto;
      font-size: 12px;
      line-height: 1.45;
      border: 1px solid #223155;
    }

    .muted { color: var(--muted); }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>TCU API Documentation</h1>
      <p>Base URL: <code>${baseUrl}</code></p>
      <p>Use query <code>?debug=1</code> to get extra <code>meta</code> for most POST APIs.</p>
    </section>

    <section class="grid">
      <article class="card">
        <div class="row"><span class="method get">GET</span><code>/health</code></div>
        <p class="muted">Server health check.</p>
      </article>

      <article class="card">
        <div class="row"><span class="method get">GET</span><code>/docs</code></div>
        <p class="muted">This documentation page.</p>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/chuongTrinh/monHoc</code></div>
        <pre>{
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/chuongTrinh</code></div>
        <pre>{
  "maxResultCount": 10,
  "skipCount": 0
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/chuongTrinhKhoaHoc/search</code></div>
        <pre>{
  "searchType": "byKhoaHoc", // or "byChuongTrinh"
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/monHoc/baiHoc</code></div>
        <p class="muted">Alias: <code>/api/ctmhMh</code></p>
        <pre>{
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/khoaHoc</code></div>
        <pre>{
  "maxResultCount": 20,
  "skipCount": 0,
  "dateCase": "all" // or "notEnded"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/khoaHoc/lopHoc</code></div>
        <pre>{
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/lopHoc/hocVien</code></div>
        <pre>{
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/hocVien</code></div>
        <pre>{
  "maxResultCount": 20,
  "skipCount": 0,
  "name": "string" // optional: if empty, no CONTAINS filter
}</pre>
      </article>
    </section>
  </div>
</body>
</html>`;
}

export function handleDocsRequest(url: URL): Response {
  return html(renderDocsPage(url.origin));
}
