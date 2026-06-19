type HocVienDiemTongKetRow = {
  MaHocVien?: string;
  TenHocVien?: string;
  NamHoc?: string;
  HocKy?: string;
  DiemTrungBinh?: number | string;
  XepLoai?: string;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockHocVienDiemTongKetResponseFile =
  (process.env.MOCK_HOC_VIEN_DIEM_TONG_KET_RESPONSE_FILE || "").trim();

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

function escapeContainsValue(input: string): string {
  return input.replace(/'/g, "''").replace(/\"/g, '""').trim();
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

function buildSql(code: string, namHoc: string, hocKy: string): string {
  const safeCode = escapeContainsValue(code);
  const safeNamHoc = escapeSqlValue(namHoc);
  const safeHocKy = escapeSqlValue(hocKy);

  const whereClauses = [
    `CONTAINS(hv.Code, N'\"${safeCode}\"')`,
  ];

  if (safeHocKy) {
    whereClauses.push(`hk.Name LIKE N'%${safeHocKy}%'`);
  }

  if (safeNamHoc) {
    whereClauses.push(`nh.Name LIKE N'%${safeNamHoc}%'`);
  }

  return [
    "SELECT hv.Code AS MaHocVien, hv.Name AS TenHocVien, nh.Name AS NamHoc, hk.Name AS HocKy, bdth.DiemTrungBinh, bdth.XepLoai",
    "FROM BangDiemTongHops bdth",
    "JOIN HocViens hv ON hv.Id = bdth.HocVienId",
    "JOIN [TSQTT.DATA].dbo.Categories nh ON bdth.NamHocId = nh.Id",
    "JOIN [TSQTT.DATA].dbo.Categories hk ON bdth.HocKyId = hk.Id",
    `WHERE ${whereClauses.join(" AND ")}`,
    "ORDER BY nh.Name, hk.Name",
  ].join(" ");
}

function normalizeFlatRow(row: Record<string, unknown>): HocVienDiemTongKetRow {
  return {
    MaHocVien: String(
      getValueByKeys(row, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
    ).trim(),
    TenHocVien: String(
      getValueByKeys(row, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
    ).trim(),
    NamHoc: String(
      getValueByKeys(row, ["NamHoc", "namHoc", "TenNamHoc", "tenNamHoc"]) || "",
    ).trim(),
    HocKy: String(
      getValueByKeys(row, ["HocKy", "hocKy", "TenHocKy", "tenHocKy"]) || "",
    ).trim(),
    DiemTrungBinh: toNumberOrOriginal(
      getValueByKeys(row, ["DiemTrungBinh", "diemTrungBinh"]),
    ),
    XepLoai: String(
      getValueByKeys(row, ["XepLoai", "xepLoai"]) || "",
    ).trim(),
  };
}

function flattenGroupedPayload(payload: unknown): HocVienDiemTongKetRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: HocVienDiemTongKetRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const hv = item as Record<string, unknown>;

    const maHocVien = String(
      getValueByKeys(hv, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
    ).trim();
    const tenHocVien = String(
      getValueByKeys(hv, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
    ).trim();
    const nhList = getValueByKeys(hv, ["nh", "NH", "namHocs", "NamHocs"]);

    if (!maHocVien || !tenHocVien || !Array.isArray(nhList)) continue;

    for (const nhItem of nhList) {
      if (nhItem === null || typeof nhItem !== "object") continue;
      const nh = nhItem as Record<string, unknown>;

      const namHoc = String(
        getValueByKeys(nh, ["NamHoc", "namHoc", "TenNamHoc", "tenNamHoc", "Name", "name"]) || "",
      ).trim();
      const hkList = getValueByKeys(nh, ["hk", "HK", "hocKys", "HocKys"]);

      if (!Array.isArray(hkList)) continue;

      for (const hkItem of hkList) {
        if (hkItem === null || typeof hkItem !== "object") continue;
        const hk = hkItem as Record<string, unknown>;

        const hocKy = String(
          getValueByKeys(hk, ["HocKy", "hocKy", "TenHocKy", "tenHocKy", "Name", "name"]) || "",
        ).trim();

        const bdthList = getValueByKeys(hk, ["bdth", "BDTH"]);
        if (Array.isArray(bdthList) && bdthList.length > 0) {
          for (const bdthItem of bdthList) {
            if (bdthItem === null || typeof bdthItem !== "object") continue;
            const bdth = bdthItem as Record<string, unknown>;

            rows.push({
              MaHocVien: maHocVien,
              TenHocVien: tenHocVien,
              NamHoc: namHoc,
              HocKy: hocKy,
              DiemTrungBinh: toNumberOrOriginal(
                getValueByKeys(bdth, ["DiemTrungBinh", "diemTrungBinh"]),
              ),
              XepLoai: String(
                getValueByKeys(bdth, ["XepLoai", "xepLoai"]) || "",
              ).trim(),
            });
          }
        } else {
          rows.push({
            MaHocVien: maHocVien,
            TenHocVien: tenHocVien,
            NamHoc: namHoc,
            HocKy: hocKy,
            DiemTrungBinh: "",
            XepLoai: "",
          });
        }
      }
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const maHocVien = getValueByKeys(row, ["MaHocVien", "maHocVien", "Code", "code"]);
  const namHoc = getValueByKeys(row, ["NamHoc", "namHoc", "TenNamHoc", "tenNamHoc"]);
  return typeof maHocVien === "string" || typeof namHoc === "string";
}

function extractRows(rawPayload: unknown): HocVienDiemTongKetRow[] {
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
): Promise<HocVienDiemTongKetRow[]> {
  if (mockHocVienDiemTongKetResponseFile) {
    logInfo(requestId, "Using MOCK_HOC_VIEN_DIEM_TONG_KET_RESPONSE_FILE", {
      file: mockHocVienDiemTongKetResponseFile,
    });
    const rawText = await Bun.file(mockHocVienDiemTongKetResponseFile).text();
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

function getHocKyOrder(value: unknown): number | null {
  const text = String(value || "").trim();
  if (!text) return null;
  const matched = text.match(/\d+/);
  if (!matched) return null;
  const parsed = Number(matched[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function compareHocKy(a: string, b: string): number {
  const aOrder = getHocKyOrder(a);
  const bOrder = getHocKyOrder(b);

  if (aOrder !== null && bOrder !== null && aOrder !== bOrder) {
    return aOrder - bOrder;
  }

  if (aOrder !== null && bOrder === null) return -1;
  if (aOrder === null && bOrder !== null) return 1;

  return a.localeCompare(b, "vi", {
    numeric: true,
    sensitivity: "base",
  });
}

function buildOutput(rows: HocVienDiemTongKetRow[]) {
  type HocKyNode = {
    HocKy: string;
    bdth: Array<{
      DiemTrungBinh: number | string;
      XepLoai: string;
    }>;
    dedupe: Set<string>;
  };

  type NamHocNode = {
    NamHoc: string;
    hkMap: Map<string, HocKyNode>;
  };

  type HocVienNode = {
    MaHocVien: string;
    TenHocVien: string;
    nhMap: Map<string, NamHocNode>;
  };

  const hocVienMap = new Map<string, HocVienNode>();

  for (const row of rows) {
    const maHocVien = String(row.MaHocVien || "").trim();
    const tenHocVien = String(row.TenHocVien || "").trim();
    const namHoc = String(row.NamHoc || "").trim();
    const hocKy = String(row.HocKy || "").trim();
    const diemTrungBinh = toNumberOrOriginal(row.DiemTrungBinh);
    const xepLoai = String(row.XepLoai || "").trim();

    if (!maHocVien || !tenHocVien || !namHoc || !hocKy) continue;

    const hocVienKey = `${maHocVien}::${tenHocVien}`;
    let hv = hocVienMap.get(hocVienKey);
    if (!hv) {
      hv = {
        MaHocVien: maHocVien,
        TenHocVien: tenHocVien,
        nhMap: new Map(),
      };
      hocVienMap.set(hocVienKey, hv);
    }

    let nh = hv.nhMap.get(namHoc);
    if (!nh) {
      nh = {
        NamHoc: namHoc,
        hkMap: new Map(),
      };
      hv.nhMap.set(namHoc, nh);
    }

    let hk = nh.hkMap.get(hocKy);
    if (!hk) {
      hk = {
        HocKy: hocKy,
        bdth: [],
        dedupe: new Set(),
      };
      nh.hkMap.set(hocKy, hk);
    }

    const dedupeKey = `${diemTrungBinh}::${xepLoai}`;
    if (hk.dedupe.has(dedupeKey)) continue;
    hk.dedupe.add(dedupeKey);

    hk.bdth.push({
      DiemTrungBinh: diemTrungBinh,
      XepLoai: xepLoai,
    });
  }

  return Array.from(hocVienMap.values()).map((hv) => ({
    MaHocVien: hv.MaHocVien,
    TenHocVien: hv.TenHocVien,
    nh: Array.from(hv.nhMap.values())
      .sort((a, b) =>
        a.NamHoc.localeCompare(b.NamHoc, "vi", {
          numeric: true,
          sensitivity: "base",
        }),
      )
      .map((nh) => ({
        NamHoc: nh.NamHoc,
        hk: Array.from(nh.hkMap.values())
          .sort((a, b) => compareHocKy(a.HocKy, b.HocKy))
          .map((hk) => ({
            HocKy: hk.HocKy,
            bdth: hk.bdth,
          })),
      })),
  }));
}

export async function handleHocVienDiemTongKetRequest(
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
      code?: string;
      namHoc?: string;
      hocKy?: string;
    };

    try {
      body = (await req.json()) as {
        code?: string;
        namHoc?: string;
        hocKy?: string;
      };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const code = String(body?.code || "").trim();
    const namHoc = String(body?.namHoc || "").trim();
    const hocKy = String(body?.hocKy || "").trim();

    if (!code) {
      return json({ error: "Missing required field: code (string).", requestId }, 400);
    }

    const sql = buildSql(code, namHoc, hocKy);

    logInfo(requestId, "Parsed input", {
      code,
      namHoc,
      hocKy,
      useNamHocFilter: !!namHoc,
      useHocKyFilter: !!hocKy,
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
          code,
          namHoc,
          hocKy,
          useNamHocFilter: !!namHoc,
          useHocKyFilter: !!hocKy,
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