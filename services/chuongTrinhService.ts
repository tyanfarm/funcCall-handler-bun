type ChuongTrinhRow = {
  Name?: string;
  ThoiGianDaoTaoTheoNam?: number | string;
  NamBanHanh?: string | number;
  TongSoTiet?: number | string;
  ThoiGianDaoTao?: number | string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockChuongTrinhResponseFile =
  (process.env.MOCK_CHUONG_TRINH_RESPONSE_FILE || "").trim();

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

function toNumericOrOriginal(value: unknown): number | string {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length === 0) return "";
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : trimmed;
  }
  return "";
}

function normalizeRow(row: Record<string, unknown>) {
  return {
    Name: String(getValueByKeys(row, ["Name", "name"]) || "").trim(),
    ThoiGianDaoTaoTheoNam: toNumericOrOriginal(
      getValueByKeys(row, [
        "ThoiGianDaoTaoTheoNam",
        "thoiGianDaoTaoTheoNam",
        "ThoiGianDaoTao",
        "thoiGianDaoTao",
      ]),
    ),
    NamBanHanh: String(
      getValueByKeys(row, ["NamBanHanh", "namBanHanh"]) || "",
    ).trim(),
    TongSoTiet: toNumericOrOriginal(
      getValueByKeys(row, ["TongSoTiet", "tongSoTiet"]),
    ),
  };
}

function isLikelyChuongTrinhRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const name = getValueByKeys(row, ["Name", "name"]);
  return typeof name === "string";
}

function extractRows(rawPayload: unknown) {
  const queue: unknown[] = [parsePossibleJson(rawPayload)];
  const visited = new Set<object>();

  while (queue.length > 0) {
    const current = parsePossibleJson(queue.shift());

    if (Array.isArray(current)) {
      if (
        current.length === 0 ||
        (current[0] !== null &&
          typeof current[0] === "object" &&
          isLikelyChuongTrinhRow(current[0]))
      ) {
        return (current as Record<string, unknown>[]).map((row) =>
          normalizeRow(row),
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

  return [] as ReturnType<typeof normalizeRow>[];
}

function buildSql(skipCount: number, maxResultCount: number): string {
  return [
    "SELECT Name, ThoiGianDaoTao AS ThoiGianDaoTaoTheoNam, NamBanHanh, TongSoTiet",
    "FROM ChuongTrinhs",
    "WHERE IsDeleted = 'false'",
    "ORDER BY CreationTime DESC",
    `OFFSET ${skipCount} ROWS FETCH NEXT ${maxResultCount} ROWS ONLY`,
  ].join(" ");
}

async function callEduDataClient(
  sql: string,
  requestId: string,
): Promise<ReturnType<typeof normalizeRow>[]> {
  if (mockChuongTrinhResponseFile) {
    logInfo(requestId, "Using MOCK_CHUONG_TRINH_RESPONSE_FILE", {
      file: mockChuongTrinhResponseFile,
    });
    const rawText = await Bun.file(mockChuongTrinhResponseFile).text();
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

export async function handleChuongTrinhRequest(
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
    };
    try {
      body = (await req.json()) as {
        maxResultCount?: number | string;
        skipCount?: number | string;
      };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const maxResultCount = Math.min(
      toNonNegativeInt(body?.maxResultCount, 10),
      200,
    );
    const skipCount = toNonNegativeInt(body?.skipCount, 0);

    const sql = buildSql(skipCount, maxResultCount);
    logInfo(requestId, "Parsed input", {
      maxResultCount,
      skipCount,
    });

    if (enableVerboseLogs || includeMeta) {
      logInfo(requestId, "Generated SQL", { sql });
    }

    const data = await callEduDataClient(sql, requestId);
    const durationMs = Date.now() - startedAt;

    logInfo(requestId, "Response prepared", {
      resultCount: data.length,
      durationMs,
    });

    if (includeMeta) {
      return json({
        data,
        meta: {
          maxResultCount,
          skipCount,
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