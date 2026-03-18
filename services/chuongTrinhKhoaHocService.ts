type ChuongTrinhKhoaHocRow = {
  Name?: string;
  ThoiGianDaoTaoTheoNam?: number | string;
  NamBanHanh?: string;
  TongSoTiet?: number | string;
  DoiTuongDaoTao?: string;
  TenKhoaHoc?: string;
  NgayBatDau?: string;
  NgayKetThuc?: string;
};

type SearchType = "byKhoaHoc" | "byChuongTrinh";

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockChuongTrinhKhoaHocResponseFile =
  (process.env.MOCK_CHUONG_TRINH_KHOA_HOC_RESPONSE_FILE || "").trim();

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

function toNumericOrOriginal(value: unknown): number | string {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return "";
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : trimmed;
  }
  return "";
}

function normalizeFlatRow(
  row: Record<string, unknown>,
): ChuongTrinhKhoaHocRow {
  return {
    Name: String(getValueByKeys(row, ["Name", "name"]) || "").trim(),
    ThoiGianDaoTaoTheoNam: toNumericOrOriginal(
      getValueByKeys(row, ["ThoiGianDaoTaoTheoNam", "thoiGianDaoTaoTheoNam"]),
    ),
    NamBanHanh: String(
      getValueByKeys(row, ["NamBanHanh", "namBanHanh"]) || "",
    ).trim(),
    TongSoTiet: toNumericOrOriginal(
      getValueByKeys(row, ["TongSoTiet", "tongSoTiet"]),
    ),
    DoiTuongDaoTao: String(
      getValueByKeys(row, ["DoiTuongDaoTao", "doiTuongDaoTao"]) || "",
    ).trim(),
    TenKhoaHoc: String(
      getValueByKeys(row, ["TenKhoaHoc", "tenKhoaHoc"]) || "",
    ).trim(),
    NgayBatDau: String(
      getValueByKeys(row, ["NgayBatDau", "ngayBatDau"]) || "",
    ).trim(),
    NgayKetThuc: String(
      getValueByKeys(row, ["NgayKetThuc", "ngayKetThuc"]) || "",
    ).trim(),
  };
}

function flattenGroupedPayload(payload: unknown): ChuongTrinhKhoaHocRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: ChuongTrinhKhoaHocRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const program = item as Record<string, unknown>;

    const name = String(getValueByKeys(program, ["Name", "name"]) || "").trim();
    const thoiGianDaoTaoTheoNam = toNumericOrOriginal(
      getValueByKeys(program, ["ThoiGianDaoTaoTheoNam", "thoiGianDaoTaoTheoNam"]),
    );
    const namBanHanh = String(
      getValueByKeys(program, ["NamBanHanh", "namBanHanh"]) || "",
    ).trim();
    const tongSoTiet = toNumericOrOriginal(
      getValueByKeys(program, ["TongSoTiet", "tongSoTiet"]),
    );

    const dtdtList = getValueByKeys(program, ["dtdt", "DTDT"]);
    if (!name || !Array.isArray(dtdtList)) continue;

    for (const dtdtItem of dtdtList) {
      if (dtdtItem === null || typeof dtdtItem !== "object") continue;
      const dtdt = dtdtItem as Record<string, unknown>;
      const doiTuongDaoTao = String(
        getValueByKeys(dtdt, ["DoiTuongDaoTao", "doiTuongDaoTao"]) || "",
      ).trim();
      const khList = getValueByKeys(dtdt, ["kh", "KH"]);
      if (!doiTuongDaoTao || !Array.isArray(khList)) continue;

      for (const khItem of khList) {
        if (khItem === null || typeof khItem !== "object") continue;
        const kh = khItem as Record<string, unknown>;
        rows.push({
          Name: name,
          ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
          NamBanHanh: namBanHanh,
          TongSoTiet: tongSoTiet,
          DoiTuongDaoTao: doiTuongDaoTao,
          TenKhoaHoc: String(
            getValueByKeys(kh, ["TenKhoaHoc", "tenKhoaHoc"]) || "",
          ).trim(),
          NgayBatDau: String(
            getValueByKeys(kh, ["NgayBatDau", "ngayBatDau"]) || "",
          ).trim(),
          NgayKetThuc: String(
            getValueByKeys(kh, ["NgayKetThuc", "ngayKetThuc"]) || "",
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
  const name = getValueByKeys(row, ["Name", "name"]);
  const tenKhoaHoc = getValueByKeys(row, ["TenKhoaHoc", "tenKhoaHoc"]);
  return typeof name === "string" || typeof tenKhoaHoc === "string";
}

function extractRows(rawPayload: unknown): ChuongTrinhKhoaHocRow[] {
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

function normalizeSearchType(value: unknown): SearchType | null {
  const normalized = String(value || "").trim();
  if (normalized === "byKhoaHoc") return "byKhoaHoc";
  if (normalized === "byChuongTrinh") return "byChuongTrinh";
  return null;
}

function buildSql(searchType: SearchType, name: string): string {
  const safeName = escapeSqlValue(name);
  const whereField = searchType === "byKhoaHoc" ? "kh.Name" : "ct.Name";

  return [
    "SELECT ct.Name, ct.ThoiGianDaoTao AS ThoiGianDaoTaoTheoNam, ct.NamBanHanh, ct.TongSoTiet, dtdt.Name AS DoiTuongDaoTao, kh.Name AS TenKhoaHoc, kh.NgayBatDau, kh.NgayKetThuc",
    "FROM ChuongTrinhs ct",
    "JOIN DoiTuongDaoTaos dtdt ON ct.DoiTuongDaoTaoId = dtdt.Id",
    "JOIN KhoaHocs kh ON kh.DoiTuongDaoTaoId = dtdt.Id",
    "WHERE " + whereField + " LIKE N'%" + safeName + "%'",
    "AND kh.NgayKetThuc >= CAST(GETDATE() AS DATE)",
    "ORDER BY kh.Code DESC",
  ].join(" " );
}

async function callEduDataClient(
  sql: string,
  requestId: string,
): Promise<ChuongTrinhKhoaHocRow[]> {
  if (mockChuongTrinhKhoaHocResponseFile) {
    logInfo(requestId, "Using MOCK_CHUONG_TRINH_KHOA_HOC_RESPONSE_FILE", {
      file: mockChuongTrinhKhoaHocResponseFile,
    });
    const rawText = await Bun.file(mockChuongTrinhKhoaHocResponseFile).text();
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

function buildOutput(rows: ChuongTrinhKhoaHocRow[]) {
  const ctMap = new Map<
    string,
    {
      Name: string;
      ThoiGianDaoTaoTheoNam: number | string;
      NamBanHanh: string;
      TongSoTiet: number | string;
      dtdtMap: Map<
        string,
        {
          DoiTuongDaoTao: string;
          kh: { TenKhoaHoc: string; NgayBatDau: string; NgayKetThuc: string }[];
          khDedupe: Set<string>;
        }
      >;
    }
  >();

  for (const row of rows) {
    const name = String(row.Name || "").trim();
    const thoiGianDaoTaoTheoNam = toNumericOrOriginal(row.ThoiGianDaoTaoTheoNam);
    const namBanHanh = String(row.NamBanHanh || "").trim();
    const tongSoTiet = toNumericOrOriginal(row.TongSoTiet);
    const doiTuongDaoTao = String(row.DoiTuongDaoTao || "").trim();
    const tenKhoaHoc = String(row.TenKhoaHoc || "").trim();
    const ngayBatDau = String(row.NgayBatDau || "").trim();
    const ngayKetThuc = String(row.NgayKetThuc || "").trim();

    if (!name || !doiTuongDaoTao || !tenKhoaHoc) continue;

    const ctKey = `${name}::${thoiGianDaoTaoTheoNam}::${namBanHanh}::${tongSoTiet}`;
    let ct = ctMap.get(ctKey);
    if (!ct) {
      ct = {
        Name: name,
        ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
        NamBanHanh: namBanHanh,
        TongSoTiet: tongSoTiet,
        dtdtMap: new Map(),
      };
      ctMap.set(ctKey, ct);
    }

    let dtdt = ct.dtdtMap.get(doiTuongDaoTao);
    if (!dtdt) {
      dtdt = {
        DoiTuongDaoTao: doiTuongDaoTao,
        kh: [],
        khDedupe: new Set(),
      };
      ct.dtdtMap.set(doiTuongDaoTao, dtdt);
    }

    const khKey = `${tenKhoaHoc}::${ngayBatDau}::${ngayKetThuc}`;
    if (dtdt.khDedupe.has(khKey)) continue;
    dtdt.khDedupe.add(khKey);

    dtdt.kh.push({
      TenKhoaHoc: tenKhoaHoc,
      NgayBatDau: ngayBatDau,
      NgayKetThuc: ngayKetThuc,
    });
  }

  return Array.from(ctMap.values()).map((ct) => ({
    Name: ct.Name,
    ThoiGianDaoTaoTheoNam: ct.ThoiGianDaoTaoTheoNam,
    NamBanHanh: ct.NamBanHanh,
    TongSoTiet: ct.TongSoTiet,
    dtdt: Array.from(ct.dtdtMap.values()).map((dtdt) => ({
      DoiTuongDaoTao: dtdt.DoiTuongDaoTao,
      kh: dtdt.kh,
    })),
  }));
}

export async function handleChuongTrinhKhoaHocRequest(
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
    let body: { searchType?: string; name?: string };
    try {
      body = (await req.json()) as { searchType?: string; name?: string };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }
    const searchType = normalizeSearchType(body?.searchType);
    const name = String(body?.name || "").trim();

    if (!searchType) {
      return json(
        { error: "Missing or invalid field: searchType (byKhoaHoc | byChuongTrinh).", requestId },
        400,
      );
    }

    if (!name) {
      return json({ error: "Missing required field: name (string).", requestId }, 400);
    }

    const sql = buildSql(searchType, name);
    logInfo(requestId, "Parsed input", { searchType, name });
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
          searchType,
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