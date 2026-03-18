type HocVienRow = {
  TenHocVien?: string;
  MaHocVien?: string;
  ThongTinHocVien?: string;
  TenKhoaHoc?: string;
  TenLopHoc?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockHocVienResponseFile =
  (process.env.MOCK_HOC_VIEN_RESPONSE_FILE || "").trim();

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

function escapeContainsValue(input: string): string {
  return input.replace(/'/g, "''").replace(/\"/g, '""').trim();
}

function buildSql(
  skipCount: number,
  maxResultCount: number,
  name: string,
  code: string,
): string {
  const whereClause = code
    ? "WHERE CONTAINS(hv.Code, N'\"" + escapeContainsValue(code) + "\"')"
    : name
      ? "WHERE CONTAINS(hv.Name, N'\"" + escapeContainsValue(name) + "\"')"
      : "";

  return [
    "SELECT hv.Name AS TenHocVien, hv.Code AS MaHocVien, hv.Value2 AS ThongTinHocVien, kh.Name AS TenKhoaHoc, lh.Name AS TenLopHoc",
    "FROM HocViens hv",
    "JOIN KhoaHocs kh ON hv.KhoaHocId = kh.Id",
    "JOIN LopHocs lh ON hv.LopHocId = lh.Id",
    whereClause,
    "ORDER BY hv.Code ASC",
    "OFFSET " + skipCount + " ROWS FETCH NEXT " + maxResultCount + " ROWS ONLY",
  ]
    .filter(Boolean)
    .join(" ");
}

function resolveSearchBy(name: string, code: string): "name" | "code" | "none" {
  if (code) return "code";
  if (name) return "name";
  return "none";
}

function normalizeFlatRow(row: Record<string, unknown>): HocVienRow {
  return {
    TenHocVien: String(
      getValueByKeys(row, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
    ).trim(),
    MaHocVien: String(
      getValueByKeys(row, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
    ).trim(),
    ThongTinHocVien: String(
      getValueByKeys(row, ["ThongTinHocVien", "thongTinHocVien", "Value2", "value2"]) || "",
    ).trim(),
    TenKhoaHoc: String(
      getValueByKeys(row, ["TenKhoaHoc", "tenKhoaHoc"]) || "",
    ).trim(),
    TenLopHoc: String(
      getValueByKeys(row, ["TenLopHoc", "tenLopHoc"]) || "",
    ).trim(),
  };
}

function flattenGroupedPayload(payload: unknown): HocVienRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: HocVienRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const hv = item as Record<string, unknown>;

    const tenHocVien = String(
      getValueByKeys(hv, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
    ).trim();
    const maHocVien = String(
      getValueByKeys(hv, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
    ).trim();
    const thongTinHocVien = String(
      getValueByKeys(hv, ["ThongTinHocVien", "thongTinHocVien", "Value2", "value2"]) || "",
    ).trim();
    const khList = getValueByKeys(hv, ["kh", "KH"]);

    if (!tenHocVien || !maHocVien || !Array.isArray(khList)) continue;

    for (const khItem of khList) {
      if (khItem === null || typeof khItem !== "object") continue;
      const kh = khItem as Record<string, unknown>;

      const tenKhoaHoc = String(
        getValueByKeys(kh, ["TenKhoaHoc", "tenKhoaHoc", "Name", "name"]) || "",
      ).trim();
      const lhList = getValueByKeys(kh, ["lh", "LH", "lopHocs", "LopHocs"]);

      if (!tenKhoaHoc || !Array.isArray(lhList)) continue;

      for (const lhItem of lhList) {
        if (lhItem === null || typeof lhItem !== "object") continue;
        const lh = lhItem as Record<string, unknown>;

        rows.push({
          TenHocVien: tenHocVien,
          MaHocVien: maHocVien,
          ThongTinHocVien: thongTinHocVien,
          TenKhoaHoc: tenKhoaHoc,
          TenLopHoc: String(
            getValueByKeys(lh, ["TenLopHoc", "tenLopHoc", "Name", "name"]) || "",
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
  const tenHocVien = getValueByKeys(row, ["TenHocVien", "tenHocVien", "Name", "name"]);
  const maHocVien = getValueByKeys(row, ["MaHocVien", "maHocVien", "Code", "code"]);
  return typeof tenHocVien === "string" || typeof maHocVien === "string";
}

function extractRows(rawPayload: unknown): HocVienRow[] {
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
): Promise<HocVienRow[]> {
  if (mockHocVienResponseFile) {
    logInfo(requestId, "Using MOCK_HOC_VIEN_RESPONSE_FILE", {
      file: mockHocVienResponseFile,
    });
    const rawText = await Bun.file(mockHocVienResponseFile).text();
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

function buildOutput(rows: HocVienRow[]) {
  const hocVienMap = new Map<
    string,
    {
      TenHocVien: string;
      MaHocVien: string;
      ThongTinHocVien: string;
      khMap: Map<
        string,
        {
          TenKhoaHoc: string;
          lh: { TenLopHoc: string }[];
          lhDedupe: Set<string>;
        }
      >;
    }
  >();

  for (const row of rows) {
    const tenHocVien = String(row.TenHocVien || "").trim();
    const maHocVien = String(row.MaHocVien || "").trim();
    const thongTinHocVien = String(row.ThongTinHocVien || "").trim();
    const tenKhoaHoc = String(row.TenKhoaHoc || "").trim();
    const tenLopHoc = String(row.TenLopHoc || "").trim();

    if (!tenHocVien || !maHocVien || !tenKhoaHoc || !tenLopHoc) continue;

    const hocVienKey = `${tenHocVien}::${maHocVien}::${thongTinHocVien}`;
    let hocVien = hocVienMap.get(hocVienKey);
    if (!hocVien) {
      hocVien = {
        TenHocVien: tenHocVien,
        MaHocVien: maHocVien,
        ThongTinHocVien: thongTinHocVien,
        khMap: new Map(),
      };
      hocVienMap.set(hocVienKey, hocVien);
    }

    let kh = hocVien.khMap.get(tenKhoaHoc);
    if (!kh) {
      kh = {
        TenKhoaHoc: tenKhoaHoc,
        lh: [],
        lhDedupe: new Set(),
      };
      hocVien.khMap.set(tenKhoaHoc, kh);
    }

    if (kh.lhDedupe.has(tenLopHoc)) continue;
    kh.lhDedupe.add(tenLopHoc);
    kh.lh.push({ TenLopHoc: tenLopHoc });
  }

  return Array.from(hocVienMap.values()).map((hv) => ({
    TenHocVien: hv.TenHocVien,
    MaHocVien: hv.MaHocVien,
    ThongTinHocVien: hv.ThongTinHocVien,
    kh: Array.from(hv.khMap.values()).map((kh) => ({
      TenKhoaHoc: kh.TenKhoaHoc,
      lh: kh.lh,
    })),
  }));
}

export async function handleHocVienRequest(
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
      code?: string;
    };
    try {
      body = (await req.json()) as {
        maxResultCount?: number | string;
        skipCount?: number | string;
        name?: string;
        code?: string;
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
    const name = String(body?.name || "").trim();
    const code = String(body?.code || "").trim();
    const searchBy = resolveSearchBy(name, code);

    const sql = buildSql(skipCount, maxResultCount, name, code);
    logInfo(requestId, "Parsed input", {
      maxResultCount,
      skipCount,
      name,
      code,
      searchBy,
      useContainsFilter: searchBy !== "none",
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
          code,
          searchBy,
          useContainsFilter: searchBy !== "none",
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
