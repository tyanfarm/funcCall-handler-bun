type MonHocBaiHocRow = {
  TenMonHoc?: string;
  ChiTietMonHoc?: string;
  DanhSachBaiHoc?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockMonHocBaiHocResponseFile =
  (process.env.MOCK_MON_HOC_BAI_HOC_RESPONSE_FILE || process.env.MOCK_CTMH_MH_RESPONSE_FILE || "").trim();

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function logInfo(
  requestId: string,
  message: string,
  payload?: Record<string, unknown>,
) {
  if (payload) {
    console.log(`[api][${requestId}] ${message}`, payload);
    return;
  }
  console.log(`[api][${requestId}] ${message}`);
}

function logError(
  requestId: string,
  message: string,
  error?: unknown,
  payload?: Record<string, unknown>,
) {
  if (payload) {
    console.error(`[api][${requestId}] ${message}`, payload);
  } else {
    console.error(`[api][${requestId}] ${message}`);
  }

  if (error instanceof Error) {
    console.error(`[api][${requestId}] ${error.message}`);
    if (error.stack) {
      console.error(error.stack);
    }
    return;
  }

  if (error !== undefined) {
    console.error(error);
  }
}

function parsePossibleJson(value: unknown): unknown {
  if (typeof value !== "string") return value;
  const trimmed = value.trim();
  if (
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    try {
      return JSON.parse(trimmed);
    } catch {
      return value;
    }
  }
  return value;
}

function getValueByKeys(
  source: Record<string, unknown>,
  keys: string[],
): unknown {
  const keyMap = new Map<string, string>();
  for (const key of Object.keys(source)) {
    keyMap.set(key.toLowerCase(), key);
  }
  for (const key of keys) {
    const found = keyMap.get(key.toLowerCase());
    if (found) return source[found];
  }
  return undefined;
}

function escapeSqlValue(input: string): string {
  return input.replace(/'/g, "''").trim();
}

function buildSql(mhName: string): string {
  const safeMhName = escapeSqlValue(mhName);
  return [
    "SELECT top 1 mh.Name AS TenMonHoc, ctmh.moTa AS ChiTietMonHoc, ctmh.baiHocs AS DanhSachBaiHoc",
    "FROM ChuongTrinhMonHocs ctmh",
    "JOIN MonHocs mh ON ctmh.MonHocId = mh.Id",
    `WHERE mh.Name LIKE N'%${safeMhName}%'`,
    "AND ctmh.IsDeleted = 'false' AND mh.IsDeleted = 'false'",
  ].join(" ");
}

function flattenGroupedPayload(payload: unknown): MonHocBaiHocRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: MonHocBaiHocRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;
    const tenMonHoc = String(
      getValueByKeys(obj, ["TenMonHoc", "tenMonHoc", "Name", "name"]) || "",
    ).trim();
    const ctmh = getValueByKeys(obj, ["ctmh", "CTMH"]);

    if (!tenMonHoc || !Array.isArray(ctmh)) continue;

    for (const detail of ctmh) {
      if (detail === null || typeof detail !== "object") continue;
      const d = detail as Record<string, unknown>;
      rows.push({
        TenMonHoc: tenMonHoc,
        ChiTietMonHoc: String(
          getValueByKeys(d, ["ChiTietMonHoc", "chiTietMonHoc", "moTa"]) || "",
        ),
        DanhSachBaiHoc: String(
          getValueByKeys(d, ["DanhSachBaiHoc", "danhSachBaiHoc", "baiHocs"]) || "",
        ),
      });
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const tenMonHoc = getValueByKeys(row, ["TenMonHoc", "tenMonHoc"]);
  return typeof tenMonHoc === "string";
}

function extractRows(rawPayload: unknown): MonHocBaiHocRow[] {
  const queue: unknown[] = [parsePossibleJson(rawPayload)];
  const visited = new Set<object>();

  while (queue.length > 0) {
    const current = parsePossibleJson(queue.shift());

    if (Array.isArray(current)) {
      const flattened = flattenGroupedPayload(current);
      if (flattened) return flattened;

      if (
        current.length === 0 ||
        (current[0] !== null &&
          typeof current[0] === "object" &&
          isLikelyFlatRow(current[0]))
      ) {
        return current as MonHocBaiHocRow[];
      }

      for (const item of current) queue.push(item);
      continue;
    }

    if (current !== null && typeof current === "object") {
      if (visited.has(current)) continue;
      visited.add(current);
      for (const value of Object.values(current as Record<string, unknown>)) {
        queue.push(value);
      }
    }
  }

  return [];
}

function parseDanhSachBaiHoc(input: unknown): { Name: string }[] {
  const parsed = parsePossibleJson(input);

  if (!Array.isArray(parsed)) return [];

  const names: { Name: string }[] = [];
  for (const item of parsed) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;
    const name = String(getValueByKeys(obj, ["Name", "name"]) || "").trim();
    if (!name) continue;
    if (name.normalize("NFC") === "C\u1ed9ng") continue;
    names.push({ Name: name });
  }

  return names;
}

async function callEduDataClient(sql: string, requestId: string): Promise<MonHocBaiHocRow[]> {
  if (mockMonHocBaiHocResponseFile) {
    logInfo(requestId, "Using MOCK_MON_HOC_BAI_HOC_RESPONSE_FILE", {
      file: mockMonHocBaiHocResponseFile,
    });
    const rawText = await Bun.file(mockMonHocBaiHocResponseFile).text();
    const parsedBody = parsePossibleJson(rawText);
    return extractRows(parsedBody);
  }

  const endpoint = new URL(eduGetDataPath, eduBaseUrl).toString();
  const startedAt = Date.now();

  if (enableVerboseLogs) {
    logInfo(requestId, "Calling upstream EDU API", { endpoint });
  }

  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      code: "SQL-QUERY",
      valueData: sql,
    }),
  });

  const rawText = await response.text();
  const parsedBody = parsePossibleJson(rawText);
  const durationMs = Date.now() - startedAt;

  logInfo(requestId, "Upstream EDU API responded", {
    status: response.status,
    ok: response.ok,
    durationMs,
    responseBytes: rawText.length,
  });

  if (!response.ok) {
    throw new Error(
      `Upstream error ${response.status}: ${typeof parsedBody === "string" ? parsedBody : JSON.stringify(parsedBody)}`,
    );
  }

  return extractRows(parsedBody);
}

function buildOutput(rows: MonHocBaiHocRow[]) {
  const monHocMap = new Map<
    string,
    {
      TenMonHoc: string;
      ctmh: {
        ChiTietMonHoc: string;
        tongSoBaiHoc: number;
        DanhSachBaiHoc: { Name: string }[];
      }[];
      dedupe: Set<string>;
    }
  >();

  for (const row of rows) {
    const r = row as Record<string, unknown>;
    const tenMonHoc = String(
      getValueByKeys(r, ["TenMonHoc", "tenMonHoc", "Name", "name"]) || "",
    ).trim();
    const chiTietMonHoc = String(
      getValueByKeys(r, ["ChiTietMonHoc", "chiTietMonHoc", "moTa"]) || "",
    );
    const danhSachRaw = String(
      getValueByKeys(r, ["DanhSachBaiHoc", "danhSachBaiHoc", "baiHocs"]) || "",
    );

    if (!tenMonHoc) continue;

    let monHoc = monHocMap.get(tenMonHoc);
    if (!monHoc) {
      monHoc = { TenMonHoc: tenMonHoc, ctmh: [], dedupe: new Set() };
      monHocMap.set(tenMonHoc, monHoc);
    }

    const baiHocNames = parseDanhSachBaiHoc(danhSachRaw);
    const dedupeKey = `${chiTietMonHoc}::${JSON.stringify(baiHocNames)}`;
    if (monHoc.dedupe.has(dedupeKey)) continue;

    monHoc.dedupe.add(dedupeKey);
    monHoc.ctmh.push({
      ChiTietMonHoc: chiTietMonHoc,
      tongSoBaiHoc: baiHocNames.length,
      DanhSachBaiHoc: baiHocNames,
    });
  }

  return Array.from(monHocMap.values()).map((m) => ({
    TenMonHoc: m.TenMonHoc,
    ctmh: m.ctmh,
  }));
}
export async function handleMonHocBaiHocRequest(
  req: Request,
  url: URL,
  requestId: string,
): Promise<Response> {
  try {
    const includeMeta = url.searchParams.get("debug") === "1";
    const startedAt = Date.now();

    logInfo(requestId, "Incoming request", {
      method: req.method,
      path: url.pathname,
      debug: includeMeta,
    });

    let body: { mhName?: string; Name?: string };
    try {
      body = (await req.json()) as { mhName?: string; Name?: string };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const mhName = String(body?.mhName || body?.Name || "").trim();
    if (!mhName) {
      return json({ error: "Missing required field: mhName (string).", requestId }, 400);
    }

    const sql = buildSql(mhName);
    logInfo(requestId, "Parsed input", { mhName });
    if (enableVerboseLogs || includeMeta) {
      logInfo(requestId, "Generated SQL", { sql });
    }

    const rows = await callEduDataClient(sql, requestId);
    const data = buildOutput(rows);
    const durationMs = Date.now() - startedAt;

    logInfo(requestId, "Response prepared", {
      rows: rows.length,
      resultCount: data.length,
      durationMs,
    });

    if (includeMeta) {
      return json({
        data,
        meta: {
          mhName,
          rawRows: rows.length,
          resultCount: data.length,
        },
        requestId,
      });
    }

    return json(data);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Internal server error";
    logError(requestId, "Unhandled request error", error);
    return json({ error: message, requestId }, 500);
  }
}