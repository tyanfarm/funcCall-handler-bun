type HocVienLopHocRow = {
  TenLopHoc?: string;
  MaHocVien?: string;
  TenHocVien?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockHocVienLopHocResponseFile =
  (process.env.MOCK_HOC_VIEN_LOP_HOC_RESPONSE_FILE || "").trim();

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

function buildSql(lhName: string): string {
  const safeLhName = escapeSqlValue(lhName);
  return [
    "SELECT DISTINCT lh.Name AS TenLopHoc, hv.Code AS MaHocVien, hv.Name AS TenHocVien",
    "FROM HocVienLopHocs hvlh",
    "JOIN LopHocs lh ON hvlh.lopHocId = lh.ID",
    "JOIN HocViens hv ON hvlh.hocVienId = hv.Id",
    `WHERE lh.Name LIKE N'%${safeLhName}%' AND hvlh.isDeleted = 'false'`,
    "ORDER BY hv.Code ASC",
  ].join(" ");
}

function flattenGroupedPayload(payload: unknown): HocVienLopHocRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: HocVienLopHocRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;
    const tenLopHoc = String(
      getValueByKeys(obj, ["TenLopHoc", "tenLopHoc", "Name", "name"]) || "",
    ).trim();
    const hv = getValueByKeys(obj, ["hv", "HV", "hocViens", "HocViens"]);

    if (!tenLopHoc || !Array.isArray(hv)) continue;

    for (const hocVien of hv) {
      if (hocVien === null || typeof hocVien !== "object") continue;
      const h = hocVien as Record<string, unknown>;
      rows.push({
        TenLopHoc: tenLopHoc,
        MaHocVien: String(
          getValueByKeys(h, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
        ).trim(),
        TenHocVien: String(
          getValueByKeys(h, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
        ).trim(),
      });
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const tenLopHoc = getValueByKeys(row, ["TenLopHoc", "tenLopHoc"]);
  const maHocVien = getValueByKeys(row, ["MaHocVien", "maHocVien"]);
  return typeof tenLopHoc === "string" || typeof maHocVien === "string";
}

function normalizeFlatRow(row: Record<string, unknown>): HocVienLopHocRow {
  return {
    TenLopHoc: String(
      getValueByKeys(row, ["TenLopHoc", "tenLopHoc", "Name", "name"]) || "",
    ).trim(),
    MaHocVien: String(
      getValueByKeys(row, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
    ).trim(),
    TenHocVien: String(
      getValueByKeys(row, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
    ).trim(),
  };
}

function extractRows(rawPayload: unknown): HocVienLopHocRow[] {
  const queue: unknown[] = [parsePossibleJson(rawPayload)];
  const visited = new Set<object>();

  while (queue.length > 0) {
    const current = parsePossibleJson(queue.shift());

    if (Array.isArray(current)) {
      const groupedRows = flattenGroupedPayload(current);
      if (groupedRows) return groupedRows;

      if (
        current.length === 0 ||
        (current[0] !== null &&
          typeof current[0] === "object" &&
          isLikelyFlatRow(current[0]))
      ) {
        return (current as Record<string, unknown>[]).map((row) =>
          normalizeFlatRow(row),
        );
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

async function callEduDataClient(
  sql: string,
  requestId: string,
): Promise<HocVienLopHocRow[]> {
  if (mockHocVienLopHocResponseFile) {
    logInfo(requestId, "Using MOCK_HOC_VIEN_LOP_HOC_RESPONSE_FILE", {
      file: mockHocVienLopHocResponseFile,
    });
    const rawText = await Bun.file(mockHocVienLopHocResponseFile).text();
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

function buildOutput(rows: HocVienLopHocRow[]) {
  const lopHocMap = new Map<
    string,
    {
      TenLopHoc: string;
      hv: { MaHocVien: string; TenHocVien: string }[];
      dedupe: Set<string>;
    }
  >();

  for (const row of rows) {
    const tenLopHoc = String(row.TenLopHoc || "").trim();
    const maHocVien = String(row.MaHocVien || "").trim();
    const tenHocVien = String(row.TenHocVien || "").trim();

    if (!tenLopHoc || !maHocVien) continue;

    let lopHoc = lopHocMap.get(tenLopHoc);
    if (!lopHoc) {
      lopHoc = {
        TenLopHoc: tenLopHoc,
        hv: [],
        dedupe: new Set(),
      };
      lopHocMap.set(tenLopHoc, lopHoc);
    }

    const dedupeKey = `${maHocVien}::${tenHocVien}`;
    if (lopHoc.dedupe.has(dedupeKey)) continue;
    lopHoc.dedupe.add(dedupeKey);

    lopHoc.hv.push({
      MaHocVien: maHocVien,
      TenHocVien: tenHocVien,
    });
  }

  return Array.from(lopHocMap.values()).map((lopHoc) => {
    const sortedHv = lopHoc.hv.sort((a, b) => a.MaHocVien.localeCompare(b.MaHocVien));
    return {
      TenLopHoc: lopHoc.TenLopHoc,
      tongSoHocVien: sortedHv.length,
      hv: sortedHv,
    };
  });
}

export async function handleHocVienLopHocRequest(
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

    let body: { name?: string };
    try {
      body = (await req.json()) as { name?: string };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const name = String(body?.name || "").trim();
    if (!name) {
      return json({ error: "Missing required field: name (string).", requestId }, 400);
    }

    const sql = buildSql(name);
    logInfo(requestId, "Parsed input", { name });
    if (enableVerboseLogs || includeMeta) {
      logInfo(requestId, "Generated SQL", { sql });
    }

    const rows = await callEduDataClient(sql, requestId);
    const data = buildOutput(rows);
    const durationMs = Date.now() - startedAt;

    logInfo(requestId, "Response prepared", {
      rawRows: rows.length,
      resultCount: data.length,
      durationMs,
    });

    if (includeMeta) {
      return json({
        data,
        meta: {
          name,
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
