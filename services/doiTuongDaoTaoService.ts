type DoiTuongDaoTaoRow = {
  DoiTuongDaoTao?: string;
  Code?: string;
  HeDaoTao?: string;
  CapDaoTao?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockDoiTuongDaoTaoResponseFile =
  (process.env.MOCK_DOI_TUONG_DAO_TAO_RESPONSE_FILE || "").trim();

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

function normalizeFlatRow(row: Record<string, unknown>): DoiTuongDaoTaoRow {
  return {
    DoiTuongDaoTao: String(
      getValueByKeys(row, ["DoiTuongDaoTao", "doiTuongDaoTao", "Name", "name"]) ||
        "",
    ).trim(),
    Code: String(getValueByKeys(row, ["Code", "code"]) || "").trim(),
    HeDaoTao: String(
      getValueByKeys(row, ["HeDaoTao", "heDaoTao"]) || "",
    ).trim(),
    CapDaoTao: String(
      getValueByKeys(row, ["CapDaoTao", "capDaoTao"]) || "",
    ).trim(),
  };
}

function flattenGroupedPayload(payload: unknown): DoiTuongDaoTaoRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: DoiTuongDaoTaoRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const dtdt = item as Record<string, unknown>;

    const doiTuongDaoTao = String(
      getValueByKeys(dtdt, ["DoiTuongDaoTao", "doiTuongDaoTao", "Name", "name"]) ||
        "",
    ).trim();
    const code = String(getValueByKeys(dtdt, ["Code", "code"]) || "").trim();
    const hdtList = getValueByKeys(dtdt, ["hdt", "HDT"]);

    if (!doiTuongDaoTao) continue;

    if (Array.isArray(hdtList) && hdtList.length > 0) {
      for (const hdtItem of hdtList) {
        if (hdtItem === null || typeof hdtItem !== "object") continue;
        const hdt = hdtItem as Record<string, unknown>;

        const heDaoTao = String(
          getValueByKeys(hdt, ["HeDaoTao", "heDaoTao", "Name", "name"]) || "",
        ).trim();
        const cdtList = getValueByKeys(hdt, ["cdt", "CDT"]);

        if (Array.isArray(cdtList) && cdtList.length > 0) {
          for (const cdtItem of cdtList) {
            if (cdtItem === null || typeof cdtItem !== "object") continue;
            const cdt = cdtItem as Record<string, unknown>;

            rows.push({
              DoiTuongDaoTao: doiTuongDaoTao,
              Code: code,
              HeDaoTao: heDaoTao,
              CapDaoTao: String(
                getValueByKeys(cdt, ["CapDaoTao", "capDaoTao", "Name", "name"]) || "",
              ).trim(),
            });
          }
        } else {
          rows.push({
            DoiTuongDaoTao: doiTuongDaoTao,
            Code: code,
            HeDaoTao: heDaoTao,
            CapDaoTao: "",
          });
        }
      }
      continue;
    }

    rows.push({
      DoiTuongDaoTao: doiTuongDaoTao,
      Code: code,
      HeDaoTao: String(
        getValueByKeys(dtdt, ["HeDaoTao", "heDaoTao"]) || "",
      ).trim(),
      CapDaoTao: String(
        getValueByKeys(dtdt, ["CapDaoTao", "capDaoTao"]) || "",
      ).trim(),
    });
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const doiTuongDaoTao = getValueByKeys(row, ["DoiTuongDaoTao", "doiTuongDaoTao"]);
  const heDaoTao = getValueByKeys(row, ["HeDaoTao", "heDaoTao"]);
  const capDaoTao = getValueByKeys(row, ["CapDaoTao", "capDaoTao"]);
  return (
    typeof doiTuongDaoTao === "string" ||
    typeof heDaoTao === "string" ||
    typeof capDaoTao === "string"
  );
}

function extractRows(rawPayload: unknown): DoiTuongDaoTaoRow[] {
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

function buildSql(): string {
  return [
    "SELECT dtdt.Name AS DoiTuongDaoTao, dtdt.Code, hdt.Name AS HeDaoTao, cdt.Name AS CapDaoTao",
    "FROM DoiTuongDaoTaos dtdt",
    "JOIN [TSQTT.DATA].dbo.Categories hdt ON dtdt.HeDaoTaoId = hdt.Id",
    "JOIN [TSQTT.DATA].dbo.Categories cdt ON dtdt.CapDaotaoId = cdt.Id",
  ].join(" ");
}

async function callEduDataClient(
  sql: string,
  requestId: string,
): Promise<DoiTuongDaoTaoRow[]> {
  if (mockDoiTuongDaoTaoResponseFile) {
    logInfo(requestId, "Using MOCK_DOI_TUONG_DAO_TAO_RESPONSE_FILE", {
      file: mockDoiTuongDaoTaoResponseFile,
    });
    const rawText = await Bun.file(mockDoiTuongDaoTaoResponseFile).text();
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

function buildOutput(rows: DoiTuongDaoTaoRow[]) {
  const dedupe = new Set<string>();
  const data: Array<Record<string, unknown>> = [];

  for (const row of rows) {
    const doiTuongDaoTao = String(row.DoiTuongDaoTao || "").trim();
    const code = String(row.Code || "").trim();
    const heDaoTao = String(row.HeDaoTao || "").trim();
    const capDaoTao = String(row.CapDaoTao || "").trim();

    if (!doiTuongDaoTao) continue;

    const key = `${doiTuongDaoTao}::${code}::${heDaoTao}::${capDaoTao}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);

    data.push({
      DoiTuongDaoTao: doiTuongDaoTao,
      Code: code,
      HeDaoTao: heDaoTao,
      CapDaoTao: capDaoTao,
    });
  }

  return data;
}

export async function handleDoiTuongDaoTaoRequest(
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

    // Optional body only for compatibility; endpoint does not require inputs.
    try {
      await req.text();
    } catch {
      // ignore body parse errors for empty/non-json bodies
    }

    const sql = buildSql();
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