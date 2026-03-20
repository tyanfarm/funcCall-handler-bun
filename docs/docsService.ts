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
        <div class="row"><span class="method post">POST</span><code>/api/chuongTrinh/details</code></div>
        <p class="muted">Alias (old): <code>/api/chuongTrinh/monHoc</code></p>
        <pre>{
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/chuongTrinh/list</code></div>
        <p class="muted">Alias (old): <code>/api/chuongTrinh</code></p>
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
        <div class="row"><span class="method post">POST</span><code>/api/monHoc/details</code></div>
        <p class="muted">Alias: <code>/api/ctmhMh</code></p>
        <pre>{
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/khoaHoc/list</code></div>
        <pre>{
  "maxResultCount": 20,
  "skipCount": 0,
  "isActive": true // default true, skip outdated khoaHoc
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/khoaHocLopHoc/search</code></div>
        <p class="muted">Alias: <code>/api/khoaHoc/lopHoc</code></p>
        <pre>{
  "searchType": "byKhoaHoc", // or "byLopHoc"
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/khoaHocKeHoachDaoTaoChiTiet/search</code></div>
        <pre>{
  "searchType": "byKhoaHoc"
  "name": "string", // khoaHoc
  "namHoc": "string",
  "hocKy": "string" // optional
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/lopHoc/details</code></div>
        <p class="muted">Alias (old): <code>/api/lopHoc/hocVien</code></p>
        <pre>{
  "name": "string"
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/hocVien/list</code></div>
        <p class="muted">Alias (old): <code>/api/hocVien</code></p>
        <pre>{
  "maxResultCount": 20,
  "skipCount": 0,
  "name": "string", // optional
  "code": "20\u0110K345" // optional, higher priority than name
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/hocVienBangDiem/search</code></div>
        <pre>{
  "searchType": "byHocVien",
  "code": "string", // optional if codes is provided
  "codes": ["20\u0110Q375", "20\u0110Q376"], // optional, supports 1 or many
  "namHoc": "2025-2026",
  "hocKy": "1", // optional
  "includeSummary": true // optional, requires hocKy
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/giangDuong/list</code></div>
        <p class="muted">Alias: <code>/api/giangDuong</code></p>
        <pre>{
  "maxResultCount": 5,
  "skipCount": 0,
  "name": "T3009" // optional
}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/doiTuongDaoTao/list</code></div>
        <p class="muted">Alias: <code>/api/doiTuongDaoTao</code></p>
        <pre>{}</pre>
      </article>

      <article class="card">
        <div class="row"><span class="method post">POST</span><code>/api/phanCongGiangDay/search</code></div>
        <p class="muted">Alias: <code>/api/phanCongGiangDay</code></p>
        <pre>{
  "lopHocName": "\u0110H32LQE",
  "monHocName": "Kinh tế chính trị học Mác - Lênin"
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
