type BangDiemRow = {
  DiemThanhPhan?: string;
  CauTrucDiem?: string;
  DiemThi?: number | string;
  DiemTongKet?: number | string;
  XepLoaiMonHoc?: string;
  DiemTrungBinh?: number | string;
  XepLoaiTongHop?: string;
  MaHocVien?: string;
  TenHocVien?: string;
  TenLopHoc?: string;
  TenMonHoc?: string;
  NamHoc?: string;
  HocKy?: string;
};

type SearchType = "byHocVien";

const SCORE_KEY_MAP: Record<string, string> = {
  "ab48cd2c-a0c6-40da-acba-907fc7bfac90": "DiemThuongXuyen",
  "81acf4f5-8169-4cc9-2d99-08dbe1c3abf0": "DiemDanhGiaQuaTrinh",
  "7804e06f-3d31-4897-8583-6ab883f90a60": "DiemGiuaKy",
  "d72e8f86-0d8a-49d7-b361-bdb2a84540b0": "DiemThi",
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockHocVienBangDiemResponseFile =
  (process.env.MOCK_HOC_VIEN_BANG_DIEM_RESPONSE_FILE || "").trim();

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

function normalizeSearchType(value: unknown): SearchType | null {
  const normalized = String(value || "").trim();
  if (normalized === "byHocVien") return "byHocVien";
  return null;
}

function toBoolean(value: unknown): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return false;
}

function normalizeCodeList(codesInput: unknown, singleCode: string): string[] {
  const list: string[] = [];

  if (singleCode) {
    list.push(singleCode);
  }

  if (Array.isArray(codesInput)) {
    for (const item of codesInput) {
      const value = String(item || "").trim();
      if (value) list.push(value);
    }
  }

  return Array.from(new Set(list));
}

function buildSql(
  codes: string[],
  namHoc: string,
  hocKy: string,
  includeSummary: boolean,
): string {
  const safeNamHoc = escapeSqlValue(namHoc);
  const safeHocKy = escapeSqlValue(hocKy);

  const codeClauses = codes
    .map((code) => escapeSqlValue(code))
    .filter((code) => !!code)
    .map((safeCode) => "hv.Code LIKE N'%" + safeCode + "%'");

  const whereClauses = ["bd.DiemThi >= 0", "nh.Name LIKE N'%" + safeNamHoc + "%'"];

  if (codeClauses.length > 0) {
    whereClauses.push("(" + codeClauses.join(" OR ") + ")");
  }

  if (safeHocKy) {
    whereClauses.push("hk.Name LIKE N'%" + safeHocKy + "%'");
  }

  const selectBase = [
    "bd.DiemThanhPhan",
    "bd.CauTrucDiem",
    "bd.DiemThi",
    "bd.DiemTongKet",
    "bd.XepLoai AS XepLoaiMonHoc",
    "hv.Code AS MaHocVien",
    "hv.Name AS TenHocVien",
    "lh.Name AS TenLopHoc",
    "mh.Name AS TenMonHoc",
    "nh.Name AS NamHoc",
    "hk.Name AS HocKy",
  ];

  const fromAndJoin = [
    "FROM BangDiems bd",
    "JOIN HocViens hv ON hv.Id = bd.HocVienId",
    "JOIN MonHocs mh ON mh.Id = bd.MonHocId",
    "JOIN LopHocs lh ON lh.Id = bd.LopHocId",
    "JOIN [TSQTT.DATA].dbo.Categories nh ON bd.NamHocId = nh.Id",
    "JOIN [TSQTT.DATA].dbo.Categories hk ON bd.HocKyId = hk.Id",
  ];

  if (includeSummary) {
    selectBase.push("bdth.DiemTrungBinh AS DiemTrungBinh");
    selectBase.push("bdth.XepLoai AS XepLoaiTongHop");
    fromAndJoin.push(
      "JOIN BangDiemTongHops bdth ON bd.HocVienId = bdth.HocVienId AND bd.NamHocId = bdth.NamHocId AND bd.HocKyId = bdth.HocKyId",
    );
  }

  return [
    "SELECT " + selectBase.join(", "),
    ...fromAndJoin,
    "WHERE " + whereClauses.join(" AND "),
    "ORDER BY bd.Id DESC",
  ].join(" ");
}

function normalizeFlatRow(row: Record<string, unknown>): BangDiemRow {
  return {
    DiemThanhPhan: String(
      getValueByKeys(row, ["DiemThanhPhan", "diemThanhPhan"]) || "",
    ),
    CauTrucDiem: String(
      getValueByKeys(row, ["CauTrucDiem", "cauTrucDiem"]) || "",
    ),
    DiemThi: toNumberOrOriginal(getValueByKeys(row, ["DiemThi", "diemThi"])),
    DiemTongKet: toNumberOrOriginal(
      getValueByKeys(row, ["DiemTongKet", "diemTongKet"]),
    ),
    XepLoaiMonHoc: String(
      getValueByKeys(row, ["XepLoaiMonHoc", "xepLoaiMonHoc", "XepLoai", "xepLoai"]) ||
        "",
    ).trim(),
    DiemTrungBinh: toNumberOrOriginal(
      getValueByKeys(row, ["DiemTrungBinh", "diemTrungBinh"]),
    ),
    XepLoaiTongHop: String(
      getValueByKeys(row, ["XepLoaiTongHop", "xepLoaiTongHop"]) || "",
    ).trim(),
    MaHocVien: String(
      getValueByKeys(row, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
    ).trim(),
    TenHocVien: String(
      getValueByKeys(row, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
    ).trim(),
    TenLopHoc: String(
      getValueByKeys(row, ["TenLopHoc", "tenLopHoc"]) || "",
    ).trim(),
    TenMonHoc: String(
      getValueByKeys(row, ["TenMonHoc", "tenMonHoc"]) || "",
    ).trim(),
    NamHoc: String(
      getValueByKeys(row, ["NamHoc", "namHoc", "TenNamHoc", "tenNamHoc"]) || "",
    ).trim(),
    HocKy: String(
      getValueByKeys(row, ["HocKy", "hocKy", "TenHocKy", "tenHocKy"]) || "",
    ).trim(),
  };
}

function flattenGroupedPayload(payload: unknown): BangDiemRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: BangDiemRow[] = [];

  for (const item of payload) {
    if (item === null || typeof item !== "object") continue;
    const bd = item as Record<string, unknown>;

    const diemThanhPhan = String(
      getValueByKeys(bd, ["DiemThanhPhan", "diemThanhPhan"]) || "",
    );
    const cauTrucDiem = String(
      getValueByKeys(bd, ["CauTrucDiem", "cauTrucDiem"]) || "",
    );
    const diemThi = toNumberOrOriginal(getValueByKeys(bd, ["DiemThi", "diemThi"]));
    const diemTongKet = toNumberOrOriginal(
      getValueByKeys(bd, ["DiemTongKet", "diemTongKet"]),
    );
    const xepLoaiMonHoc = String(
      getValueByKeys(bd, ["XepLoaiMonHoc", "xepLoaiMonHoc", "XepLoai", "xepLoai"]) ||
        "",
    ).trim();

    const hvList = getValueByKeys(bd, ["hv", "HV"]);
    if (!Array.isArray(hvList)) continue;

    for (const hvItem of hvList) {
      if (hvItem === null || typeof hvItem !== "object") continue;
      const hv = hvItem as Record<string, unknown>;

      const maHocVien = String(
        getValueByKeys(hv, ["MaHocVien", "maHocVien", "Code", "code"]) || "",
      ).trim();
      const tenHocVien = String(
        getValueByKeys(hv, ["TenHocVien", "tenHocVien", "Name", "name"]) || "",
      ).trim();
      const lhList = getValueByKeys(hv, ["lh", "LH", "lopHocs", "LopHocs"]);

      if (!maHocVien || !tenHocVien || !Array.isArray(lhList)) continue;

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

          const tenMonHoc = String(
            getValueByKeys(mh, ["TenMonHoc", "tenMonHoc", "Name", "name"]) || "",
          ).trim();
          const nhList = getValueByKeys(mh, ["nh", "NH", "namHocs", "NamHocs"]);

          if (!tenMonHoc || !Array.isArray(nhList)) continue;

          for (const nhItem of nhList) {
            if (nhItem === null || typeof nhItem !== "object") continue;
            const nh = nhItem as Record<string, unknown>;

            const namHoc = String(
              getValueByKeys(nh, [
                "NamHoc",
                "namHoc",
                "TenNamHoc",
                "tenNamHoc",
                "Name",
                "name",
              ]) || "",
            ).trim();
            const hkList = getValueByKeys(nh, ["hk", "HK", "hocKys", "HocKys"]);

            if (Array.isArray(hkList) && hkList.length > 0) {
              for (const hkItem of hkList) {
                if (hkItem === null || typeof hkItem !== "object") continue;
                const hk = hkItem as Record<string, unknown>;
                const bdthList = getValueByKeys(hk, ["bdth", "BDTH"]);

                let diemTrungBinh: number | string = "";
                let xepLoaiTongHop = "";

                if (Array.isArray(bdthList) && bdthList.length > 0) {
                  const bdth0 = bdthList[0];
                  if (bdth0 && typeof bdth0 === "object") {
                    const bdth = bdth0 as Record<string, unknown>;
                    diemTrungBinh = toNumberOrOriginal(
                      getValueByKeys(bdth, ["DiemTrungBinh", "diemTrungBinh"]),
                    );
                    xepLoaiTongHop = String(
                      getValueByKeys(bdth, ["XepLoaiTongHop", "xepLoaiTongHop", "XepLoai", "xepLoai"]) ||
                        "",
                    ).trim();
                  }
                }

                rows.push({
                  DiemThanhPhan: diemThanhPhan,
                  CauTrucDiem: cauTrucDiem,
                  DiemThi: diemThi,
                  DiemTongKet: diemTongKet,
                  XepLoaiMonHoc: xepLoaiMonHoc,
                  DiemTrungBinh: diemTrungBinh,
                  XepLoaiTongHop: xepLoaiTongHop,
                  MaHocVien: maHocVien,
                  TenHocVien: tenHocVien,
                  TenLopHoc: tenLopHoc,
                  TenMonHoc: tenMonHoc,
                  NamHoc: namHoc,
                  HocKy: String(
                    getValueByKeys(hk, ["HocKy", "hocKy", "TenHocKy", "tenHocKy", "Name", "name"]) ||
                      "",
                  ).trim(),
                });
              }
            } else {
              rows.push({
                DiemThanhPhan: diemThanhPhan,
                CauTrucDiem: cauTrucDiem,
                DiemThi: diemThi,
                DiemTongKet: diemTongKet,
                XepLoaiMonHoc: xepLoaiMonHoc,
                MaHocVien: maHocVien,
                TenHocVien: tenHocVien,
                TenLopHoc: tenLopHoc,
                TenMonHoc: tenMonHoc,
                NamHoc: namHoc,
                HocKy: "",
              });
            }
          }
        }
      }
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const maHocVien = getValueByKeys(row, ["MaHocVien", "maHocVien"]);
  const tenMonHoc = getValueByKeys(row, ["TenMonHoc", "tenMonHoc"]);
  return typeof maHocVien === "string" || typeof tenMonHoc === "string";
}

function extractRows(rawPayload: unknown): BangDiemRow[] {
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
): Promise<BangDiemRow[]> {
  if (mockHocVienBangDiemResponseFile) {
    logInfo(requestId, "Using MOCK_HOC_VIEN_BANG_DIEM_RESPONSE_FILE", {
      file: mockHocVienBangDiemResponseFile,
    });
    const rawText = await Bun.file(mockHocVienBangDiemResponseFile).text();
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

function toPercentValue(value: unknown): string {
  if (typeof value === "number") return `${value}%`;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return "";
    return trimmed.endsWith("%") ? trimmed : `${trimmed}%`;
  }
  return "";
}

function mapScoreByKey(
  input: string,
  valueAsPercent: boolean,
): Record<string, unknown> {
  const parsed = parsePossibleJson(input);
  if (!Array.isArray(parsed)) return {};

  const mapped: Record<string, unknown> = {};

  for (const item of parsed) {
    if (item === null || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;

    const rawKey = String(getValueByKeys(obj, ["key", "Key"]) || "")
      .trim()
      .toLowerCase();
    if (!rawKey) continue;

    const targetKey = SCORE_KEY_MAP[rawKey];
    if (!targetKey) continue;

    const rawValue = getValueByKeys(obj, ["value", "Value"]);
    mapped[targetKey] = valueAsPercent
      ? toPercentValue(rawValue)
      : toNumberOrOriginal(rawValue);
  }

  return mapped;
}

function getHocKyOrder(value: unknown): number | null {
  const text = String(value || "").trim();
  if (!text) return null;
  const matched = text.match(/\d+/);
  if (!matched) return null;
  const parsed = Number(matched[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function compareSemesterByHocKy(
  a: Record<string, unknown>,
  b: Record<string, unknown>,
): number {
  const aHocKy = String(a.HocKy || "").trim();
  const bHocKy = String(b.HocKy || "").trim();
  const aOrder = getHocKyOrder(aHocKy);
  const bOrder = getHocKyOrder(bHocKy);

  if (aOrder !== null && bOrder !== null && aOrder !== bOrder) {
    return aOrder - bOrder;
  }

  if (aOrder !== null && bOrder === null) return -1;
  if (aOrder === null && bOrder !== null) return 1;

  return aHocKy.localeCompare(bHocKy, "vi", {
    numeric: true,
    sensitivity: "base",
  });
}

function buildOutput(rows: BangDiemRow[], includeSummary: boolean) {
  type Semester = {
    HocKy: string;
    DiemTrungBinh?: number | string;
    XepLoai?: string;
    MonHocs: Array<Record<string, unknown>>;
    dedupe: Set<string>;
  };

  type StudentGroup = {
    MaHocVien: string;
    TenHocVien: string;
    TenLopHoc: string;
    NamHoc: string;
    semesterMap: Map<string, Semester>;
  };

  const groupMap = new Map<string, StudentGroup>();

  for (const row of rows) {
    const maHocVien = String(row.MaHocVien || "").trim();
    const tenHocVien = String(row.TenHocVien || "").trim();
    const tenLopHoc = String(row.TenLopHoc || "").trim();
    const namHoc = String(row.NamHoc || "").trim();
    const hocKy = String(row.HocKy || "").trim();
    const tenMonHoc = String(row.TenMonHoc || "").trim();

    if (!maHocVien || !tenMonHoc || !namHoc) continue;

    const groupKey = `${maHocVien}::${tenHocVien}::${tenLopHoc}::${namHoc}`;
    let student = groupMap.get(groupKey);

    if (!student) {
      student = {
        MaHocVien: maHocVien,
        TenHocVien: tenHocVien,
        TenLopHoc: tenLopHoc,
        NamHoc: namHoc,
        semesterMap: new Map<string, Semester>(),
      };
      groupMap.set(groupKey, student);
    }

    const semesterKey = hocKy || "";
    let semester = student.semesterMap.get(semesterKey);

    if (!semester) {
      semester = {
        HocKy: hocKy,
        MonHocs: [],
        dedupe: new Set(),
      };
      student.semesterMap.set(semesterKey, semester);
    }

    const diemTrungBinh = toNumberOrOriginal(row.DiemTrungBinh);
    const xepLoaiTongHop = String(row.XepLoaiTongHop || "").trim();

    if (includeSummary) {
      if (diemTrungBinh !== "" && semester.DiemTrungBinh === undefined) {
        semester.DiemTrungBinh = diemTrungBinh;
      }
      if (xepLoaiTongHop && !semester.XepLoai) {
        semester.XepLoai = xepLoaiTongHop;
      }
    }

    const diemThanhPhan = mapScoreByKey(String(row.DiemThanhPhan || ""), false);
    const cauTrucDiem = mapScoreByKey(String(row.CauTrucDiem || ""), true);
    const diemThi = toNumberOrOriginal(row.DiemThi);
    const diemTongKet = toNumberOrOriginal(row.DiemTongKet);
    const xepLoaiMonHoc = String(row.XepLoaiMonHoc || "").trim();

    const monHocKey = `${tenMonHoc}::${JSON.stringify(diemThanhPhan)}::${JSON.stringify(cauTrucDiem)}::${diemThi}::${diemTongKet}::${xepLoaiMonHoc}`;
    if (semester.dedupe.has(monHocKey)) continue;
    semester.dedupe.add(monHocKey);

    semester.MonHocs.push({
      TenMonHoc: tenMonHoc,
      DiemThanhPhan: diemThanhPhan,
      CauTrucDiem: cauTrucDiem,
      DiemThi: diemThi,
      DiemTongKet: diemTongKet,
      XepLoai: xepLoaiMonHoc,
    });
  }

  const output: Array<Record<string, unknown>> = [];

  for (const student of groupMap.values()) {
    const semesters = Array.from(student.semesterMap.values())
      .map((sem) => {
        const item: Record<string, unknown> = {
          HocKy: sem.HocKy,
          MonHocs: sem.MonHocs,
        };

        if (includeSummary) {
          if (sem.DiemTrungBinh !== undefined && sem.DiemTrungBinh !== "") {
            item.DiemTrungBinh = sem.DiemTrungBinh;
          }
          if (sem.XepLoai) {
            item.XepLoai = sem.XepLoai;
          }
        }

        return item;
      })
      .sort(compareSemesterByHocKy);

    output.push({
      MaHocVien: student.MaHocVien,
      TenHocVien: student.TenHocVien,
      TenLopHoc: student.TenLopHoc,
      NamHoc: student.NamHoc,
      BangDiemTheoHocKy: semesters,
    });
  }

  return output;
}

export async function handleHocVienBangDiemRequest(
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
      code?: string;
      codes?: string[];
      namHoc?: string;
      hocKy?: string;
      includeSummary?: boolean | string;
    };

    try {
      body = (await req.json()) as {
        searchType?: string;
        code?: string;
        codes?: string[];
        namHoc?: string;
        hocKy?: string;
        includeSummary?: boolean | string;
      };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }

    const searchType = normalizeSearchType(body?.searchType);
    const code = String(body?.code || "").trim();
    const codes = normalizeCodeList(body?.codes, code);
    const namHoc = String(body?.namHoc || "").trim();
    const hocKy = String(body?.hocKy || "").trim();
    const includeSummary = toBoolean(body?.includeSummary);

    if (!searchType) {
      return json(
        { error: "Missing or invalid field: searchType (byHocVien).", requestId },
        400,
      );
    }

    if (codes.length === 0) {
      return json(
        { error: "Missing required field: code (string) or codes (string[]).", requestId },
        400,
      );
    }

    if (!namHoc) {
      return json({ error: "Missing required field: namHoc (string).", requestId }, 400);
    }

    if (includeSummary && !hocKy) {
      return json(
        {
          error:
            "Field includeSummary=true is only allowed when hocKy is provided.",
          requestId,
        },
        400,
      );
    }

    const sql = buildSql(codes, namHoc, hocKy, includeSummary);
    logInfo(requestId, "Parsed input", {
      searchType,
      codes,
      namHoc,
      hocKy,
      includeSummary,
    });

    if (enableVerboseLogs || includeMeta) {
      logInfo(requestId, "Generated SQL", { sql });
    }

    const rows = await callEduDataClient(sql, requestId);
    const data = buildOutput(rows, includeSummary);
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
          codes,
          namHoc,
          hocKy,
          includeSummary,
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