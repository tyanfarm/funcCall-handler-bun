type KhoaHocLopHocRow = {
  TenKhoaHoc?: string;
  NgayBatDau?: string;
  NgayKetThuc?: string;
  TenLopHoc?: string;
  SoLuongHocVien?: number | string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockKhoaHocLopHocResponseFile =
  (process.env.MOCK_KHOA_HOC_LOP_HOC_RESPONSE_FILE || "").trim();

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

function toNumberOrZero(value: unknown): number {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value === "string") {
    const parsed = Number(value.trim());
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function normalizeFlatRow(row: Record<string, unknown>): KhoaHocLopHocRow {
  return {
    TenKhoaHoc: String(
      getValueByKeys(row, ["TenKhoaHoc", "tenKhoaHoc", "Name", "name"]) || "",
    ).trim(),
    NgayBatDau: String(
      getValueByKeys(row, ["NgayBatDau", "ngayBatDau"]) || "",
    ).trim(),
    NgayKetThuc: String(
      getValueByKeys(row, ["NgayKetThuc", "ngayKetThuc"]) || "",
    ).trim(),
    TenLopHoc: String(
      getValueByKeys(row, ["TenLopHoc", "tenLopHoc"]) || "",
    ).trim(),
    SoLuongHocVien: toNumberOrZero(
      getValueByKeys(row, ["SoLuongHocVien", "soLuongHocVien"]),
    ),
  };
}

function flattenGroupedPayload(payload: unknown): KhoaHocLopHocRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: KhoaHocLopHocRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;

    const tenKhoaHoc = String(
      getValueByKeys(obj, ["TenKhoaHoc", "tenKhoaHoc", "Name", "name"]) || "",
    ).trim();
    const ngayBatDau = String(
      getValueByKeys(obj, ["NgayBatDau", "ngayBatDau"]) || "",
    ).trim();
    const ngayKetThuc = String(
      getValueByKeys(obj, ["NgayKetThuc", "ngayKetThuc"]) || "",
    ).trim();
    const lopHocs = getValueByKeys(obj, ["lopHocs", "LopHocs", "lh", "lopHoc"]);

    if (!tenKhoaHoc || !Array.isArray(lopHocs)) continue;

    for (const lopHoc of lopHocs) {
      if (lopHoc === null || typeof lopHoc !== "object") continue;
      const lh = lopHoc as Record<string, unknown>;
      rows.push({
        TenKhoaHoc: tenKhoaHoc,
        NgayBatDau: ngayBatDau,
        NgayKetThuc: ngayKetThuc,
        TenLopHoc: String(
          getValueByKeys(lh, ["TenLopHoc", "tenLopHoc", "Name", "name"]) || "",
        ).trim(),
        SoLuongHocVien: toNumberOrZero(
          getValueByKeys(lh, ["SoLuongHocVien", "soLuongHocVien"]),
        ),
      });
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const tenKhoaHoc = getValueByKeys(row, ["TenKhoaHoc", "tenKhoaHoc"]);
  const tenLopHoc = getValueByKeys(row, ["TenLopHoc", "tenLopHoc"]);
  return typeof tenKhoaHoc === "string" || typeof tenLopHoc === "string";
}

function extractRows(rawPayload: unknown): KhoaHocLopHocRow[] {
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

function buildSql(khName: string): string {
  const safeKhName = escapeSqlValue(khName);
  return [
    "SELECT kh.Name AS TenKhoaHoc, kh.NgayBatDau, kh.NgayKetThuc, lh.Name AS TenLopHoc, COUNT(DISTINCT hv.Id) AS SoLuongHocVien",
    "FROM LopHocs lh",
    "JOIN KhoaHocs kh ON lh.KhoaHocId = kh.Id",
    "LEFT JOIN HocViens hv ON hv.LopHocId = lh.Id",
    `WHERE kh.Name LIKE N'%${safeKhName}%'`,
    "AND lh.IsDeleted = 'false'",
    "GROUP BY kh.Name, kh.NgayBatDau, kh.NgayKetThuc, lh.Name",
  ].join(" ");
}

async function callEduDataClient(
  sql: string,
  requestId: string,
): Promise<KhoaHocLopHocRow[]> {
  if (mockKhoaHocLopHocResponseFile) {
    logInfo(requestId, "Using MOCK_KHOA_HOC_LOP_HOC_RESPONSE_FILE", {
      file: mockKhoaHocLopHocResponseFile,
    });
    const rawText = await Bun.file(mockKhoaHocLopHocResponseFile).text();
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

function buildOutput(rows: KhoaHocLopHocRow[]) {
  const khoaHocMap = new Map<
    string,
    {
      TenKhoaHoc: string;
      NgayBatDau: string;
      NgayKetThuc: string;
      lopHocs: { TenLopHoc: string; SoLuongHocVien: number }[];
      dedupe: Set<string>;
    }
  >();

  for (const row of rows) {
    const tenKhoaHoc = String(row.TenKhoaHoc || "").trim();
    const ngayBatDau = String(row.NgayBatDau || "").trim();
    const ngayKetThuc = String(row.NgayKetThuc || "").trim();
    const tenLopHoc = String(row.TenLopHoc || "").trim();
    const soLuongHocVien = toNumberOrZero(row.SoLuongHocVien);

    if (!tenKhoaHoc || !tenLopHoc) continue;

    const key = `${tenKhoaHoc}::${ngayBatDau}::${ngayKetThuc}`;
    let khoaHoc = khoaHocMap.get(key);
    if (!khoaHoc) {
      khoaHoc = {
        TenKhoaHoc: tenKhoaHoc,
        NgayBatDau: ngayBatDau,
        NgayKetThuc: ngayKetThuc,
        lopHocs: [],
        dedupe: new Set(),
      };
      khoaHocMap.set(key, khoaHoc);
    }

    if (khoaHoc.dedupe.has(tenLopHoc)) continue;
    khoaHoc.dedupe.add(tenLopHoc);
    khoaHoc.lopHocs.push({
      TenLopHoc: tenLopHoc,
      SoLuongHocVien: soLuongHocVien,
    });
  }

  return Array.from(khoaHocMap.values()).map((kh) => ({
    TenKhoaHoc: kh.TenKhoaHoc,
    NgayBatDau: kh.NgayBatDau,
    NgayKetThuc: kh.NgayKetThuc,
    TongSoHocVien: kh.lopHocs.reduce(
      (sum, lh) => sum + toNumberOrZero(lh.SoLuongHocVien),
      0,
    ),
    lopHocs: kh.lopHocs,
  }));
}

export async function handleKhoaHocLopHocRequest(
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

    let body: { khName?: string; Name?: string };
    try {
      body = (await req.json()) as { khName?: string; Name?: string };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const khName = String(body?.khName || body?.Name || "").trim();
    if (!khName) {
      return json({ error: "Missing required field: khName (string).", requestId }, 400);
    }

    const sql = buildSql(khName);
    logInfo(requestId, "Parsed input", { khName });
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
          khName,
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