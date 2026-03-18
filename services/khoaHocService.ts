type KhoaHocRow = {
  TenKhoaHoc?: string;
  Name?: string;
  NgayBatDau?: string;
  NgayKetThuc?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockKhoaHocResponseFile =
  (process.env.MOCK_KHOA_HOC_RESPONSE_FILE || "").trim();

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

function normalizeIsActive(value: unknown): boolean {
  if (value === undefined || value === null || value === "") return true;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;

  const normalized = String(value).trim().toLowerCase();
  if (["false", "0", "no", "off", "inactive"].includes(normalized)) {
    return false;
  }
  if (["true", "1", "yes", "on", "active"].includes(normalized)) {
    return true;
  }

  return true;
}

function normalizeRow(row: Record<string, unknown>) {
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
  };
}

function isLikelyKhoaHocRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const tenKhoaHoc = getValueByKeys(row, ["TenKhoaHoc", "Name", "name"]);
  return typeof tenKhoaHoc === "string";
}

function extractRows(rawPayload: unknown): ReturnType<typeof normalizeRow>[] {
  const queue: unknown[] = [parsePossibleJson(rawPayload)];
  const visited = new Set<object>();

  while (queue.length > 0) {
    const current = parsePossibleJson(queue.shift());

    if (Array.isArray(current)) {
      if (
        current.length === 0 ||
        (current[0] !== null &&
          typeof current[0] === "object" &&
          isLikelyKhoaHocRow(current[0]))
      ) {
        return (current as Record<string, unknown>[])
          .map((row) => normalizeRow(row))
          .filter((row) => row.NgayBatDau && row.NgayKetThuc);
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

function buildSql(
  skipCount: number,
  maxResultCount: number,
  isActive: boolean,
): string {
  const whereClauses = [
    "NgayBatDau IS NOT NULL",
    "NgayKetThuc IS NOT NULL",
    "IsDeleted = 'false'",
  ];

  if (isActive) {
    whereClauses.push("NgayKetThuc >= CAST(GETDATE() AS DATE)");
  }

  return [
    "SELECT Name AS TenKhoaHoc, NgayBatDau, NgayKetThuc",
    "FROM KhoaHocs",
    `WHERE ${whereClauses.join(" AND ")}`,
    "ORDER BY CreationTime DESC",
    `OFFSET ${skipCount} ROWS FETCH NEXT ${maxResultCount} ROWS ONLY`,
  ].join(" ");
}

async function callEduDataClient(
  sql: string,
  requestId: string,
): Promise<ReturnType<typeof normalizeRow>[]> {
  if (mockKhoaHocResponseFile) {
    logInfo(requestId, "Using MOCK_KHOA_HOC_RESPONSE_FILE", {
      file: mockKhoaHocResponseFile,
    });
    const rawText = await Bun.file(mockKhoaHocResponseFile).text();
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

export async function handleKhoaHocRequest(
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
      isActive?: boolean | string | number;
    };
    try {
      body = (await req.json()) as {
        maxResultCount?: number | string;
        skipCount?: number | string;
        isActive?: boolean | string | number;
      };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const maxResultCount = Math.min(
      toNonNegativeInt(body?.maxResultCount, 20),
      200,
    );
    const skipCount = toNonNegativeInt(body?.skipCount, 0);
    const isActive = normalizeIsActive(body?.isActive);

    const sql = buildSql(skipCount, maxResultCount, isActive);
    logInfo(requestId, "Parsed input", {
      maxResultCount,
      skipCount,
      isActive,
    });

    if (enableVerboseLogs || includeMeta) {
      logInfo(requestId, "Generated SQL", { sql });
    }

    const data = await callEduDataClient(sql, requestId);
    const durationMs = Date.now() - startedAt;

    logInfo(requestId, "Response prepared", {
      resultCount: data.length,
      isActive,
      durationMs,
    });

    if (includeMeta) {
      return json({
        data,
        meta: {
          maxResultCount,
          skipCount,
          isActive,
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