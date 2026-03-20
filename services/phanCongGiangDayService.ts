type PhanCongGiangDayRow = {
  ThongTinGiangVien?: string;
  TenLopHoc?: string;
  TenMonHoc?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockPhanCongGiangDayResponseFile =
  (process.env.MOCK_PHAN_CONG_GIANG_DAY_RESPONSE_FILE || "").trim();

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

function buildSql(lopHocName: string, monHocName: string): string {
  const safeLopHocName = escapeSqlValue(lopHocName);
  const safeMonHocName = escapeSqlValue(monHocName);

  return [
    "SELECT top 3 pcgd.Value1 AS ThongTinGiangVien, lh.Name AS TenLopHoc, mh.Name AS TenMonHoc",
    "FROM PhanCongGiangDays pcgd",
    "JOIN LopHocs lh ON pcgd.lopHocId = lh.Id",
    "JOIN MonHocs mh ON pcgd.monHocId = mh.Id",
    "WHERE lh.Name LIKE N'%" + safeLopHocName + "%'",
    "AND mh.Name LIKE N'%" + safeMonHocName + "%'",
    "AND pcgd.IsDeleted = 'false'",
  ].join(" ");
}

function normalizeFlatRow(row: Record<string, unknown>): PhanCongGiangDayRow {
  return {
    ThongTinGiangVien: String(
      getValueByKeys(row, ["ThongTinGiangVien", "thongTinGiangVien", "Value1", "value1"]) ||
        "",
    ).trim(),
    TenLopHoc: String(
      getValueByKeys(row, ["TenLopHoc", "tenLopHoc", "Name", "name"]) || "",
    ).trim(),
    TenMonHoc: String(
      getValueByKeys(row, ["TenMonHoc", "tenMonHoc"]) || "",
    ).trim(),
  };
}

function flattenGroupedPayload(payload: unknown): PhanCongGiangDayRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: PhanCongGiangDayRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;

    const thongTinGiangVien = String(
      getValueByKeys(obj, ["ThongTinGiangVien", "thongTinGiangVien", "Value1", "value1"]) ||
        "",
    ).trim();
    const lhList = getValueByKeys(obj, ["lh", "LH", "lopHocs", "LopHocs"]);

    if (!Array.isArray(lhList)) continue;

    for (const lhItem of lhList) {
      if (lhItem === null || typeof lhItem !== "object") continue;
      const lh = lhItem as Record<string, unknown>;

      const tenLopHoc = String(
        getValueByKeys(lh, ["TenLopHoc", "tenLopHoc", "Name", "name"]) || "",
      ).trim();
      const mhList = getValueByKeys(lh, ["mh", "MH", "monHocs", "MonHocs"]);

      if (!tenLopHoc || !Array.isArray(mhList)) continue;

      for (const mhItem of mhList) {
        if (mhItem === null || typeof mhItem !== "object") continue;
        const mh = mhItem as Record<string, unknown>;

        rows.push({
          ThongTinGiangVien: thongTinGiangVien,
          TenLopHoc: tenLopHoc,
          TenMonHoc: String(
            getValueByKeys(mh, ["TenMonHoc", "tenMonHoc", "Name", "name"]) || "",
          ).trim(),
        });
      }
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const tenLopHoc = getValueByKeys(row, ["TenLopHoc", "tenLopHoc"]);
  const tenMonHoc = getValueByKeys(row, ["TenMonHoc", "tenMonHoc"]);
  return typeof tenLopHoc === "string" || typeof tenMonHoc === "string";
}

function extractRows(rawPayload: unknown): PhanCongGiangDayRow[] {
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
): Promise<PhanCongGiangDayRow[]> {
  if (mockPhanCongGiangDayResponseFile) {
    logInfo(requestId, "Using MOCK_PHAN_CONG_GIANG_DAY_RESPONSE_FILE", {
      file: mockPhanCongGiangDayResponseFile,
    });
    const rawText = await Bun.file(mockPhanCongGiangDayResponseFile).text();
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

function parseThongTinGiangVien(value: string): Array<{ ChucDanh: string; TenGiangVien: string }> {
  const parsed = parsePossibleJson(value);
  if (!Array.isArray(parsed)) return [];

  const result: Array<{ ChucDanh: string; TenGiangVien: string }> = [];
  const dedupe = new Set<string>();

  for (const item of parsed) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;

    const chucDanh = String(
      getValueByKeys(obj, ["chucDanhName", "ChucDanhName"]) || "",
    ).trim();
    const ho = String(getValueByKeys(obj, ["ho", "Ho"]) || "").trim();
    const ten = String(getValueByKeys(obj, ["ten", "Ten"]) || "").trim();

    const tenGiangVien = `${ho} ${ten}`.replace(/\s+/g, " ").trim();
    const key = `${chucDanh}::${tenGiangVien}`;

    if ((!chucDanh && !tenGiangVien) || dedupe.has(key)) continue;
    dedupe.add(key);

    result.push({
      ChucDanh: chucDanh,
      TenGiangVien: tenGiangVien,
    });
  }

  return result;
}

function buildOutput(rows: PhanCongGiangDayRow[]) {
  const dedupe = new Set<string>();
  const data: Array<Record<string, unknown>> = [];

  for (const row of rows) {
    const tenLopHoc = String(row.TenLopHoc || "").trim();
    const tenMonHoc = String(row.TenMonHoc || "").trim();
    const thongTinGiangVien = parseThongTinGiangVien(
      String(row.ThongTinGiangVien || ""),
    );

    if (!tenLopHoc || !tenMonHoc) continue;

    const key = `${tenLopHoc}::${tenMonHoc}::${JSON.stringify(thongTinGiangVien)}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);

    data.push({
      ThongTinGiangVien: thongTinGiangVien,
      TenLopHoc: tenLopHoc,
      TenMonHoc: tenMonHoc,
    });
  }

  return data;
}

export async function handlePhanCongGiangDayRequest(
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
      lopHocName?: string;
      monHocName?: string;
    };

    try {
      body = (await req.json()) as {
        lopHocName?: string;
        monHocName?: string;
      };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const lopHocName = String(body?.lopHocName || "").trim();
    const monHocName = String(body?.monHocName || "").trim();

    if (!lopHocName) {
      return json({ error: "Missing required field: lopHocName (string).", requestId }, 400);
    }

    if (!monHocName) {
      return json({ error: "Missing required field: monHocName (string).", requestId }, 400);
    }

    const sql = buildSql(lopHocName, monHocName);
    logInfo(requestId, "Parsed input", {
      lopHocName,
      monHocName,
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
          lopHocName,
          monHocName,
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