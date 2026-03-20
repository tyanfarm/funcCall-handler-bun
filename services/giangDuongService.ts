type GiangDuongRow = {
  Name?: string;
  TrangBiDiKems?: string;
  GhiChu?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockGiangDuongResponseFile =
  (process.env.MOCK_GIANG_DUONG_RESPONSE_FILE || "").trim();

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

function toNonNegativeInt(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(0, Math.floor(parsed));
}

function toNumberOrOriginal(value: unknown): number | string {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return "";
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : trimmed;
  }
  return "";
}

function escapeSqlValue(input: string): string {
  return input.replace(/'/g, "''").trim();
}

function buildSql(skipCount: number, maxResultCount: number, name: string): string {
  const whereClauses = ["IsDeleted = 'false'"];

  const safeName = escapeSqlValue(name);
  if (safeName) {
    whereClauses.unshift("Name LIKE N'%" + safeName + "%'" );
  }

  return [
    "SELECT Name, TrangBiDiKems, GhiChu",
    "FROM GiangDuongs",
    "WHERE " + whereClauses.join(" AND "),
    "ORDER BY Id DESC",
    "OFFSET " + skipCount + " ROWS FETCH NEXT " + maxResultCount + " ROWS ONLY",
  ].join(" ");
}

function normalizeFlatRow(row: Record<string, unknown>): GiangDuongRow {
  return {
    Name: String(getValueByKeys(row, ["Name", "name"]) || "").trim(),
    TrangBiDiKems: String(
      getValueByKeys(row, ["TrangBiDiKems", "trangBiDiKems"]) || "",
    ).trim(),
    GhiChu: String(getValueByKeys(row, ["GhiChu", "ghiChu"]) || "").trim(),
  };
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const name = getValueByKeys(row, ["Name", "name"]);
  const trangBiDiKems = getValueByKeys(row, ["TrangBiDiKems", "trangBiDiKems"]);
  return typeof name === "string" || typeof trangBiDiKems === "string";
}

function extractRows(rawPayload: unknown): GiangDuongRow[] {
  const queue: unknown[] = [parsePossibleJson(rawPayload)];
  const visited = new Set<object>();

  while (queue.length > 0) {
    const current = parsePossibleJson(queue.shift());

    if (Array.isArray(current)) {
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
): Promise<GiangDuongRow[]> {
  if (mockGiangDuongResponseFile) {
    logInfo(requestId, "Using MOCK_GIANG_DUONG_RESPONSE_FILE", {
      file: mockGiangDuongResponseFile,
    });
    const rawText = await Bun.file(mockGiangDuongResponseFile).text();
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

function parseTrangBiDiKems(value: string): Array<{ name: string; soLuong: number | string }> {
  const parsed = parsePossibleJson(value);
  if (!Array.isArray(parsed)) return [];

  const result: Array<{ name: string; soLuong: number | string }> = [];

  for (const item of parsed) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;

    const name = String(getValueByKeys(obj, ["name", "Name"]) || "").trim();
    const soLuong = toNumberOrOriginal(getValueByKeys(obj, ["soLuong", "SoLuong"]));

    if (!name) continue;
    result.push({ name, soLuong });
  }

  return result;
}

function buildOutput(rows: GiangDuongRow[]) {
  const dedupe = new Set<string>();
  const data: Array<Record<string, unknown>> = [];

  for (const row of rows) {
    const name = String(row.Name || "").trim();
    const ghiChu = String(row.GhiChu || "").trim();
    const trangBiDiKems = parseTrangBiDiKems(String(row.TrangBiDiKems || ""));

    if (!name) continue;

    const key = `${name}::${ghiChu}::${JSON.stringify(trangBiDiKems)}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);

    data.push({
      Name: name,
      TrangBiDiKems: trangBiDiKems,
      GhiChu: ghiChu,
    });
  }

  return data;
}

export async function handleGiangDuongRequest(
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

    let body: {
      maxResultCount?: number | string;
      skipCount?: number | string;
      name?: string;
    };

    try {
      body = (await req.json()) as {
        maxResultCount?: number | string;
        skipCount?: number | string;
        name?: string;
      };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const maxResultCount = Math.min(
      toNonNegativeInt(body?.maxResultCount, 5),
      200,
    );
    const skipCount = toNonNegativeInt(body?.skipCount, 0);
    const name = String(body?.name || "").trim();

    const sql = buildSql(skipCount, maxResultCount, name);
    logInfo(requestId, "Parsed input", {
      maxResultCount,
      skipCount,
      name,
      useNameFilter: !!name,
    });

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
          maxResultCount,
          skipCount,
          name,
          useNameFilter: !!name,
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