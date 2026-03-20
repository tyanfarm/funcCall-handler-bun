type KhoaHocKeHoachDaoTaoChiTietRow = {
  TenKhoaHoc?: string;
  NgayBatDau?: string;
  NgayKetThuc?: string;
  MaKhoaHoc?: string;
  ThoiGianDaoTaoTheoNam?: number | string;
  KeHoachDaoTaoChiTiet?: string;
  NamHoc?: string;
  HocKy?: string;
};

type SearchType = "byKhoaHoc";

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockKhoaHocKeHoachDaoTaoChiTietResponseFile =
  (
    process.env.MOCK_KHOA_HOC_KE_HOACH_DAO_TAO_CHI_TIET_RESPONSE_FILE ||
    process.env.MOCK_KE_HOACH_DAO_TAO_CHI_TIET_KHOA_HOC_RESPONSE_FILE ||
    ""
  ).trim();

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

function normalizeSearchType(value: unknown): SearchType | null {
  const normalized = String(value || "").trim();
  if (normalized === "byKhoaHoc") return "byKhoaHoc";
  return null;
}

function buildSql(name: string, namHoc: string, hocKy: string): string {
  const safeName = escapeSqlValue(name);
  const safeNamHoc = escapeSqlValue(namHoc);
  const safeHocKy = escapeSqlValue(hocKy);

  const whereClauses = [
    "kh.Name LIKE N'%" + safeName + "%'",
    "nh.Name LIKE N'%" + safeNamHoc + "%'",
  ];

  if (safeHocKy) {
    whereClauses.push("hk.Name LIKE N'%" + safeHocKy + "%'");
  }

  return [
    "SELECT kh.Name AS TenKhoaHoc, kh.NgayBatDau, kh.NgayKetThuc, kh.Code AS MaKhoaHoc, khdt.ThoiGianDaoTao AS ThoiGianDaoTaoTheoNam, khdtct.MonHocs AS KeHoachDaoTaoChiTiet, nh.Name AS NamHoc, hk.Name AS HocKy",
    "FROM KeHoachDaoTaos khdt",
    "JOIN KhoaHocs kh ON khdt.KhoaHocId = kh.Id",
    "JOIN KeHoachDaoTaoChiTiets khdtct ON khdtct.KeHoachDaoTaoId = khdt.Id",
    "JOIN [TSQTT.DATA].dbo.Categories nh ON khdtct.NamHocId = nh.Id",
    "JOIN [TSQTT.DATA].dbo.Categories hk ON khdtct.HocKyId = hk.Id",
    "WHERE " + whereClauses.join(" AND "),
  ].join(" ");
}

function normalizeFlatRow(row: Record<string, unknown>): KhoaHocKeHoachDaoTaoChiTietRow {
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
    MaKhoaHoc: String(
      getValueByKeys(row, ["MaKhoaHoc", "maKhoaHoc", "Code", "code"]) || "",
    ).trim(),
    ThoiGianDaoTaoTheoNam: toNumericOrOriginal(
      getValueByKeys(row, ["ThoiGianDaoTaoTheoNam", "thoiGianDaoTaoTheoNam", "ThoiGianDaoTao", "thoiGianDaoTao"]),
    ),
    KeHoachDaoTaoChiTiet: String(
      getValueByKeys(row, ["KeHoachDaoTaoChiTiet", "keHoachDaoTaoChiTiet", "MonHocs", "monHocs"]) || "",
    ),
    NamHoc: String(
      getValueByKeys(row, ["NamHoc", "namHoc"]) || "",
    ).trim(),
    HocKy: String(
      getValueByKeys(row, ["HocKy", "hocKy"]) || "",
    ).trim(),
  };
}

function flattenGroupedPayload(payload: unknown): KhoaHocKeHoachDaoTaoChiTietRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: KhoaHocKeHoachDaoTaoChiTietRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const root = item as Record<string, unknown>;

    const tenKhoaHoc = String(
      getValueByKeys(root, ["TenKhoaHoc", "tenKhoaHoc", "Name", "name"]) || "",
    ).trim();
    const ngayBatDau = String(
      getValueByKeys(root, ["NgayBatDau", "ngayBatDau"]) || "",
    ).trim();
    const ngayKetThuc = String(
      getValueByKeys(root, ["NgayKetThuc", "ngayKetThuc"]) || "",
    ).trim();
    const maKhoaHoc = String(
      getValueByKeys(root, ["MaKhoaHoc", "maKhoaHoc", "Code", "code"]) || "",
    ).trim();

    const khdtList = getValueByKeys(root, ["khdt", "KHDT"]);
    if (!tenKhoaHoc || !Array.isArray(khdtList)) continue;

    for (const khdtItem of khdtList) {
      if (khdtItem === null || typeof khdtItem !== "object") continue;
      const khdt = khdtItem as Record<string, unknown>;

      const thoiGianDaoTaoTheoNam = toNumericOrOriginal(
        getValueByKeys(khdt, ["ThoiGianDaoTaoTheoNam", "thoiGianDaoTaoTheoNam", "ThoiGianDaoTao", "thoiGianDaoTao"]),
      );

      const khdtctList = getValueByKeys(khdt, ["khdtct", "KHDTCT"]);
      if (!Array.isArray(khdtctList)) continue;

      for (const khdtctItem of khdtctList) {
        if (khdtctItem === null || typeof khdtctItem !== "object") continue;
        const khdtct = khdtctItem as Record<string, unknown>;

        const keHoachDaoTaoChiTiet = String(
          getValueByKeys(khdtct, ["KeHoachDaoTaoChiTiet", "keHoachDaoTaoChiTiet", "MonHocs", "monHocs"]) || "",
        );

        const nhList = getValueByKeys(khdtct, ["nh", "NH"]);
        if (Array.isArray(nhList) && nhList.length > 0) {
          for (const nhItem of nhList) {
            if (nhItem === null || typeof nhItem !== "object") continue;
            const nh = nhItem as Record<string, unknown>;
            const namHoc = String(
              getValueByKeys(nh, ["NamHoc", "namHoc", "Name", "name"]) || "",
            ).trim();

            const hkList = getValueByKeys(nh, ["hk", "HK"]);
            if (Array.isArray(hkList) && hkList.length > 0) {
              for (const hkItem of hkList) {
                if (hkItem === null || typeof hkItem !== "object") continue;
                const hk = hkItem as Record<string, unknown>;
                rows.push({
                  TenKhoaHoc: tenKhoaHoc,
                  NgayBatDau: ngayBatDau,
                  NgayKetThuc: ngayKetThuc,
                  MaKhoaHoc: maKhoaHoc,
                  ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
                  KeHoachDaoTaoChiTiet: keHoachDaoTaoChiTiet,
                  NamHoc: namHoc,
                  HocKy: String(
                    getValueByKeys(hk, ["HocKy", "hocKy", "Name", "name"]) || "",
                  ).trim(),
                });
              }
            } else {
              rows.push({
                TenKhoaHoc: tenKhoaHoc,
                NgayBatDau: ngayBatDau,
                NgayKetThuc: ngayKetThuc,
                MaKhoaHoc: maKhoaHoc,
                ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
                KeHoachDaoTaoChiTiet: keHoachDaoTaoChiTiet,
                NamHoc: namHoc,
                HocKy: "",
              });
            }
          }
          continue;
        }

        rows.push({
          TenKhoaHoc: tenKhoaHoc,
          NgayBatDau: ngayBatDau,
          NgayKetThuc: ngayKetThuc,
          MaKhoaHoc: maKhoaHoc,
          ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
          KeHoachDaoTaoChiTiet: keHoachDaoTaoChiTiet,
          NamHoc: String(getValueByKeys(khdtct, ["NamHoc", "namHoc"]) || "").trim(),
          HocKy: String(getValueByKeys(khdtct, ["HocKy", "hocKy"]) || "").trim(),
        });
      }
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const tenKhoaHoc = getValueByKeys(row, ["TenKhoaHoc", "tenKhoaHoc"]);
  const namHoc = getValueByKeys(row, ["NamHoc", "namHoc"]);
  return typeof tenKhoaHoc === "string" || typeof namHoc === "string";
}

function extractRows(rawPayload: unknown): KhoaHocKeHoachDaoTaoChiTietRow[] {
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
): Promise<KhoaHocKeHoachDaoTaoChiTietRow[]> {
  if (mockKhoaHocKeHoachDaoTaoChiTietResponseFile) {
    logInfo(requestId, "Using MOCK_KHOA_HOC_KE_HOACH_DAO_TAO_CHI_TIET_RESPONSE_FILE", {
      file: mockKhoaHocKeHoachDaoTaoChiTietResponseFile,
    });
    const rawText = await Bun.file(mockKhoaHocKeHoachDaoTaoChiTietResponseFile).text();
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

function parseAndCleanKeHoachDaoTaoChiTiet(input: string): unknown[] {
  const parsed = parsePossibleJson(input);
  if (!Array.isArray(parsed)) return [];

  const removeFields = new Set([
    "hocky",
    "vitri",
    "id",
    "code",
    "status",
    "gheplops",
  ]);

  return parsed
    .filter((item) => item !== null && typeof item === "object")
    .map((item) => {
      const obj = item as Record<string, unknown>;
      const cleaned: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(obj)) {
        if (removeFields.has(key.toLowerCase())) continue;
        cleaned[key] = value;
      }
      return cleaned;
    });
}

function buildOutput(rows: KhoaHocKeHoachDaoTaoChiTietRow[]) {
  const khoaHocMap = new Map<
    string,
    {
      TenKhoaHoc: string;
      NgayBatDau: string;
      NgayKetThuc: string;
      MaKhoaHoc: string;
      khdtMap: Map<
        string,
        {
          ThoiGianDaoTaoTheoNam: number | string;
          khdtct: {
            KeHoachDaoTaoChiTiet: unknown[];
            NamHoc: string;
            HocKy: string;
          }[];
          dedupe: Set<string>;
        }
      >;
    }
  >();

  for (const row of rows) {
    const tenKhoaHoc = String(row.TenKhoaHoc || "").trim();
    const ngayBatDau = String(row.NgayBatDau || "").trim();
    const ngayKetThuc = String(row.NgayKetThuc || "").trim();
    const maKhoaHoc = String(row.MaKhoaHoc || "").trim();
    const thoiGianDaoTaoTheoNam = toNumericOrOriginal(row.ThoiGianDaoTaoTheoNam);
    const keHoachDaoTaoChiTietRaw = String(row.KeHoachDaoTaoChiTiet || "");
    const namHoc = String(row.NamHoc || "").trim();
    const hocKy = String(row.HocKy || "").trim();

    if (!tenKhoaHoc || !maKhoaHoc || !namHoc) continue;

    const khdtctParsed = parseAndCleanKeHoachDaoTaoChiTiet(keHoachDaoTaoChiTietRaw);

    const khoaHocKey = `${tenKhoaHoc}::${ngayBatDau}::${ngayKetThuc}::${maKhoaHoc}`;
    let khoaHoc = khoaHocMap.get(khoaHocKey);
    if (!khoaHoc) {
      khoaHoc = {
        TenKhoaHoc: tenKhoaHoc,
        NgayBatDau: ngayBatDau,
        NgayKetThuc: ngayKetThuc,
        MaKhoaHoc: maKhoaHoc,
        khdtMap: new Map(),
      };
      khoaHocMap.set(khoaHocKey, khoaHoc);
    }

    const khdtKey = String(thoiGianDaoTaoTheoNam);
    let khdt = khoaHoc.khdtMap.get(khdtKey);
    if (!khdt) {
      khdt = {
        ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
        khdtct: [],
        dedupe: new Set(),
      };
      khoaHoc.khdtMap.set(khdtKey, khdt);
    }

    const dedupeKey = `${namHoc}::${hocKy}::${JSON.stringify(khdtctParsed)}`;
    if (khdt.dedupe.has(dedupeKey)) continue;
    khdt.dedupe.add(dedupeKey);

    khdt.khdtct.push({
      KeHoachDaoTaoChiTiet: khdtctParsed,
      NamHoc: namHoc,
      HocKy: hocKy,
    });
  }

  return Array.from(khoaHocMap.values()).map((kh) => ({
    TenKhoaHoc: kh.TenKhoaHoc,
    NgayBatDau: kh.NgayBatDau,
    NgayKetThuc: kh.NgayKetThuc,
    MaKhoaHoc: kh.MaKhoaHoc,
    khdt: Array.from(kh.khdtMap.values()).map((khdt) => ({
      ThoiGianDaoTaoTheoNam: khdt.ThoiGianDaoTaoTheoNam,
      khdtct: khdt.khdtct,
    })),
  }));
}

export async function handleKhoaHocKeHoachDaoTaoChiTietRequest(
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
      searchType?: string;
      name?: string;
      namHoc?: string;
      hocKy?: string;
    };
    try {
      body = (await req.json()) as {
        searchType?: string;
        name?: string;
        namHoc?: string;
        hocKy?: string;
      };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const searchType = normalizeSearchType(body?.searchType);
    const name = String(body?.name || "").trim();
    const namHoc = String(body?.namHoc || "").trim();
    const hocKy = String(body?.hocKy || "").trim();

    if (!searchType) {
      return json(
        { error: "Missing or invalid field: searchType (byKhoaHoc).", requestId },
        400,
      );
    }

    if (!name) {
      return json({ error: "Missing required field: name (string).", requestId }, 400);
    }

    if (!namHoc) {
      return json({ error: "Missing required field: namHoc (string).", requestId }, 400);
    }

    const sql = buildSql(name, namHoc, hocKy);
    logInfo(requestId, "Parsed input", { searchType, name, namHoc, hocKy });
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
          namHoc,
          hocKy,
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
