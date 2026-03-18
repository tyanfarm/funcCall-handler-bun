type EduRow = {
  TenChuongTrinh?: string;
  ThoiGianDaoTaoTheoNam?: number | string;
  NamBanHanh?: number | string;
  TongSoTiet?: number | string;
  TenMonHoc?: string;
  ChiTietMonHoc?: string;
  Id?: string;
};

type SubjectDetail = {
  ChiTietMonHoc: string;
};

type SubjectGroup = {
  TenMonHoc: string;
  ctmh: SubjectDetail[];
  detailSet: Set<string>;
};

type ProgramGroup = {
  TenChuongTrinh: string;
  ThoiGianDaoTaoTheoNam: number | string;
  NamBanHanh: number | string;
  TongSoTiet: number | string;
  mhMap: Map<string, SubjectGroup>;
};

type BuildOutputMeta = {
  totalRows: number;
  acceptedRows: number;
  rejectedSubjects: number;
};

const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";
const enableVerboseLogs = process.env.DEBUG_API_LOGS === "1";
const mockEduResponseFile = (process.env.MOCK_EDU_RESPONSE_FILE || "").trim();

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

function buildSql(ctName: string): string {
  const safeCtName = escapeSqlValue(ctName);
  return [
    "SELECT ct.Name AS TenChuongTrinh, ct.ThoiGianDaoTao AS ThoiGianDaoTaoTheoNam, ct.NamBanHanh, ct.TongSoTiet, mh.Name AS TenMonHoc, ctmh.moTa AS ChiTietMonHoc",
    "FROM MonHocs mh",
    "JOIN ChuongTrinhMonHocs ctmh ON mh.Id = ctmh.MonHocId",
    "JOIN ChuongTrinhs ct ON ctmh.ChuongTrinhId = ct.Id",
    `WHERE ct.Name LIKE N'%${safeCtName}%'`,
    "AND ctmh.IsDeleted = 'false'",
    "AND ctmh.ViTri > 0",
    "ORDER BY ctmh.ViTri ASC",
  ].join(" ");
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

function flattenGroupedPrograms(payload: unknown): EduRow[] | null {
  if (!Array.isArray(payload)) return null;
  const rows: EduRow[] = [];

  for (const program of payload) {
    if (program === null || typeof program !== "object") continue;
    const p = program as Record<string, unknown>;
    const tenChuongTrinh = String(
      getValueByKeys(p, ["TenChuongTrinh", "tenChuongTrinh"]) || "",
    ).trim();
    const thoiGianDaoTaoTheoNam = toNumericOrOriginal(
      getValueByKeys(p, ["ThoiGianDaoTaoTheoNam", "thoiGianDaoTaoTheoNam", "ThoiGianDaoTao", "thoiGianDaoTao"]),
    );
    const namBanHanh = toNumericOrOriginal(
      getValueByKeys(p, ["NamBanHanh", "namBanHanh"]),
    );
    const tongSoTiet = toNumericOrOriginal(
      getValueByKeys(p, ["TongSoTiet", "tongSoTiet"]),
    );
    const mh = getValueByKeys(p, ["mh", "MH"]);

    if (!tenChuongTrinh || !Array.isArray(mh)) continue;

    for (const monHoc of mh) {
      if (monHoc === null || typeof monHoc !== "object") continue;
      const m = monHoc as Record<string, unknown>;
      const tenMonHoc = String(
        getValueByKeys(m, ["TenMonHoc", "tenMonHoc"]) || "",
      ).trim();
      const ctmh = getValueByKeys(m, ["ctmh", "CTMH"]);

      if (!tenMonHoc || !Array.isArray(ctmh)) continue;

      for (const chiTiet of ctmh) {
        if (chiTiet === null || typeof chiTiet !== "object") continue;
        const c = chiTiet as Record<string, unknown>;
        rows.push({
          TenChuongTrinh: tenChuongTrinh,
          ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
          NamBanHanh: namBanHanh,
          TongSoTiet: tongSoTiet,
          TenMonHoc: tenMonHoc,
          Id: String(getValueByKeys(c, ["Id", "id"]) || ""),
          ChiTietMonHoc: String(
            getValueByKeys(c, ["ChiTietMonHoc", "chiTietMonHoc", "moTa"]) || "",
          ),
        });
      }
    }
  }

  return rows.length > 0 ? rows : null;
}

function isLikelyFlatRow(value: unknown): boolean {
  if (value === null || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  const tenChuongTrinh = getValueByKeys(row, ["TenChuongTrinh", "tenChuongTrinh"]);
  const tenMonHoc = getValueByKeys(row, ["TenMonHoc", "tenMonHoc"]);
  return typeof tenChuongTrinh === "string" || typeof tenMonHoc === "string";
}

function extractRows(rawPayload: unknown): EduRow[] {
  const queue: unknown[] = [parsePossibleJson(rawPayload)];
  const visited = new Set<object>();

  while (queue.length > 0) {
    const current = parsePossibleJson(queue.shift());

    if (Array.isArray(current)) {
      const groupedRows = flattenGroupedPrograms(current);
      if (groupedRows) return groupedRows;

      if (
        current.length === 0 ||
        (current[0] !== null &&
          typeof current[0] === "object" &&
          isLikelyFlatRow(current[0]))
      ) {
        return current as EduRow[];
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

function hasTongSoTietZeroFromJson(value: unknown): boolean {
  if (Array.isArray(value)) {
    return value.some((item) => hasTongSoTietZeroFromJson(item));
  }

  if (value !== null && typeof value === "object") {
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      if (key === "tongSoTiet") {
        if (
          (typeof child === "number" && child === 0) ||
          (typeof child === "string" && child.trim() === "0")
        ) {
          return true;
        }
      }
      if (hasTongSoTietZeroFromJson(child)) return true;
    }
  }

  return false;
}

function shouldRejectByChiTietMonHoc(chiTietMonHoc: unknown): boolean {
  if (typeof chiTietMonHoc !== "string") return false;

  const normalized = chiTietMonHoc.replace(/\s+/g, "");
  if (
    normalized.includes('"tongSoTiet":0') ||
    normalized.includes('"tongSoTiet":"0"')
  ) {
    return true;
  }

  const parsed = parsePossibleJson(chiTietMonHoc);
  return hasTongSoTietZeroFromJson(parsed);
}

async function callEduDataClient(sql: string, requestId: string): Promise<EduRow[]> {
  if (mockEduResponseFile) {
    logInfo(requestId, "Using MOCK_EDU_RESPONSE_FILE", {
      file: mockEduResponseFile,
    });
    const rawText = await Bun.file(mockEduResponseFile).text();
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

function buildOutput(rows: EduRow[]) {
  const rejectedSubjectKeys = new Set<string>();

  for (const row of rows) {
    const r = row as Record<string, unknown>;
    const tenChuongTrinh = String(
      getValueByKeys(r, ["TenChuongTrinh", "tenChuongTrinh"]) || "",
    ).trim();
    const tenMonHoc = String(
      getValueByKeys(r, ["TenMonHoc", "tenMonHoc"]) || "",
    ).trim();
    const chiTietMonHoc = String(
      getValueByKeys(r, ["ChiTietMonHoc", "chiTietMonHoc", "moTa"]) || "",
    );

    if (!tenChuongTrinh || !tenMonHoc) continue;
    if (shouldRejectByChiTietMonHoc(chiTietMonHoc)) {
      rejectedSubjectKeys.add(`${tenChuongTrinh}::${tenMonHoc}`);
    }
  }

  const programMap = new Map<string, ProgramGroup>();
  let acceptedRows = 0;

  for (const row of rows) {
    const r = row as Record<string, unknown>;
    const tenChuongTrinh = String(
      getValueByKeys(r, ["TenChuongTrinh", "tenChuongTrinh"]) || "",
    ).trim();
    const thoiGianDaoTaoTheoNam = toNumericOrOriginal(
      getValueByKeys(r, ["ThoiGianDaoTaoTheoNam", "thoiGianDaoTaoTheoNam", "ThoiGianDaoTao", "thoiGianDaoTao"]),
    );
    const namBanHanh = toNumericOrOriginal(
      getValueByKeys(r, ["NamBanHanh", "namBanHanh"]),
    );
    const tongSoTiet = toNumericOrOriginal(
      getValueByKeys(r, ["TongSoTiet", "tongSoTiet"]),
    );
    const tenMonHoc = String(
      getValueByKeys(r, ["TenMonHoc", "tenMonHoc"]) || "",
    ).trim();
    const chiTietMonHoc = String(
      getValueByKeys(r, ["ChiTietMonHoc", "chiTietMonHoc", "moTa"]) || "",
    );
    const id = String(getValueByKeys(r, ["Id", "id"]) || "");

    if (!tenChuongTrinh || !tenMonHoc) continue;
    if (rejectedSubjectKeys.has(`${tenChuongTrinh}::${tenMonHoc}`)) continue;

    let program = programMap.get(tenChuongTrinh);
    if (!program) {
      program = {
        TenChuongTrinh: tenChuongTrinh,
        ThoiGianDaoTaoTheoNam: thoiGianDaoTaoTheoNam,
        NamBanHanh: namBanHanh,
        TongSoTiet: tongSoTiet,
        mhMap: new Map(),
      };
      programMap.set(tenChuongTrinh, program);
    }

    let subject = program.mhMap.get(tenMonHoc);
    if (!subject) {
      subject = { TenMonHoc: tenMonHoc, ctmh: [], detailSet: new Set() };
      program.mhMap.set(tenMonHoc, subject);
    }

    const detailKey = `${id}::${chiTietMonHoc}`;
    if (!subject.detailSet.has(detailKey)) {
      subject.detailSet.add(detailKey);
      subject.ctmh.push({ ChiTietMonHoc: chiTietMonHoc });
      acceptedRows += 1;
    }
  }

  const data = Array.from(programMap.values()).map((program) => {
    const subjects = Array.from(program.mhMap.values()).map((subject) => ({
      TenMonHoc: subject.TenMonHoc,
      ctmh: subject.ctmh,
    }));

    return {
      TenChuongTrinh: program.TenChuongTrinh,
      ThoiGianDaoTaoTheoNam: program.ThoiGianDaoTaoTheoNam,
      NamBanHanh: program.NamBanHanh,
      TongSoTiet: program.TongSoTiet,
      tongSoMonHoc: subjects.length,
      mh: subjects,
    };
  });

  const meta: BuildOutputMeta = {
    totalRows: rows.length,
    acceptedRows,
    rejectedSubjects: rejectedSubjectKeys.size,
  };

  return { data, meta };
}

export async function handleChuongTrinhMonHocRequest(
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
    let body: { name?: string };
    try {
      body = (await req.json()) as { name?: string };
    } catch (error) {
      logError(requestId, "Invalid JSON body", error);
      return json({ error: "Invalid JSON body.", requestId }, 400);
    }
    const name = String(body?.name || "").trim();

    logInfo(requestId, "Parsed input", {
      name,
    });

    if (!name) {
      logInfo(requestId, "Rejected request: missing program name");
      return json(
        {
          error:
            "Missing required field: name (string).",
          requestId,
        },
        400,
      );
    }

    const sql = buildSql(name);
    if (enableVerboseLogs || includeMeta) {
      logInfo(requestId, "Generated SQL", {
        sql,
      });
    }

    const rows = await callEduDataClient(sql, requestId);
    logInfo(requestId, "Rows extracted from upstream payload", {
      rows: rows.length,
    });
    if (rows.length > 0) {
      const row0 = rows[0] as Record<string, unknown>;
      logInfo(requestId, "First extracted row keys", {
        keys: Object.keys(row0).slice(0, 20),
      });
    }

    const result = buildOutput(rows);
    const durationMs = Date.now() - startedAt;

    logInfo(requestId, "Response prepared", {
      totalRows: result.meta.totalRows,
      acceptedRows: result.meta.acceptedRows,
      rejectedSubjects: result.meta.rejectedSubjects,
      durationMs,
    });

    return json(includeMeta ? { ...result, requestId } : result.data);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Internal server error";
    logError(requestId, "Unhandled request error", error);
    return json({ error: message, requestId }, 500);
  }
}
