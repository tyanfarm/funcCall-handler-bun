const eduBaseUrl = process.env.EDU_BASE_URL || "http://localhost:3003";
const eduGetDataPath =
  process.env.EDU_GETDATA_PATH ||
  "/daotao/api/services/EDU/read/EduDataClient/GetData";

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

function buildSql(id: string): string {
  const safeId = escapeSqlValue(id);
  return [
    "SELECT TOP 1 Name",
    "FROM DoiTuongDaoTaos",
    "WHERE Id = '" + safeId + "'",
  ].join(" ");
}

function extractName(rawPayload: unknown): string | null {
  const queue: unknown[] = [parsePossibleJson(rawPayload)];
  const visited = new Set<object>();

  while (queue.length > 0) {
    const current = parsePossibleJson(queue.shift());

    if (Array.isArray(current)) {
      for (const item of current) {
        queue.push(item);
      }
      continue;
    }

    if (current !== null && typeof current === "object") {
      if (visited.has(current)) continue;
      visited.add(current);

      const name = String(
        getValueByKeys(current as Record<string, unknown>, ["Name", "name"]) ||
          "",
      ).trim();

      if (name) return name;

      for (const value of Object.values(current as Record<string, unknown>)) {
        queue.push(value);
      }
    }
  }

  return null;
}

export async function findDoiTuongDaoTaoNameById(
  id: string,
): Promise<string | null> {
  const normalizedId = String(id || "").trim();
  if (!normalizedId) {
    throw new Error("Missing required field: id (string).");
  }

  const endpoint = new URL(eduGetDataPath, eduBaseUrl).toString();
  const sql = buildSql(normalizedId);

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

  if (!response.ok) {
    throw new Error(
      `Upstream error ${response.status}: ${typeof parsedBody === "string" ? parsedBody : JSON.stringify(parsedBody)}`,
    );
  }

  return extractName(parsedBody);
}