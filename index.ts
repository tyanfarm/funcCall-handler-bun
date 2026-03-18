import { handleChuongTrinhRequest } from "./services/chuongTrinhService";
import { handleChuongTrinhMonHocRequest } from "./services/chuongTrinhMonHocService";
import { handleMonHocBaiHocRequest } from "./services/monHocBaiHocService";
import { handleKhoaHocRequest } from "./services/khoaHocService";
import { handleKhoaHocLopHocRequest } from "./services/khoaHocLopHocService";
import { handleHocVienLopHocRequest } from "./services/hocVienLopHocService";
import { handleChuongTrinhKhoaHocRequest } from "./services/chuongTrinhKhoaHocService";
import { handleHocVienRequest } from "./services/hocVienService";
import { handleDocsRequest } from "./docs/docsService";

const port = Number(process.env.PORT || 3000);

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function createRequestId(): string {
  const rand = Math.random().toString(36).slice(2, 8);
  return `${Date.now()}-${rand}`;
}

const server = Bun.serve({
  port,
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const requestId =
      req.headers.get("x-request-id") ||
      url.searchParams.get("requestId") ||
      createRequestId();

    if (url.pathname === "/health" && req.method === "GET") {
      return json({
        status: "ok",
        uptime: process.uptime(),
        timestamp: new Date().toISOString(),
      });
    }

    if (url.pathname === "/docs" && req.method === "GET") {
      return handleDocsRequest(url);
    }

    if (url.pathname === "/api/chuongTrinh/monHoc" && req.method === "POST") {
      return handleChuongTrinhMonHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/chuongTrinh" && req.method === "POST") {
      return handleChuongTrinhRequest(req, url, requestId);
    }

    if (url.pathname === "/api/chuongTrinhKhoaHoc/search" && req.method === "POST") {
      return handleChuongTrinhKhoaHocRequest(req, url, requestId);
    }

    if ((url.pathname === "/api/monHoc/baiHoc" || url.pathname === "/api/ctmhMh") && req.method === "POST") {
      return handleMonHocBaiHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/khoaHoc" && req.method === "POST") {
      return handleKhoaHocRequest(req, url, requestId);
    }

    if ((url.pathname === "/api/khoaHocLopHoc/search" || url.pathname === "/api/khoaHoc/lopHoc") && req.method === "POST") {
      return handleKhoaHocLopHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/lopHoc/hocVien" && req.method === "POST") {
      return handleHocVienLopHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/hocVien" && req.method === "POST") {
      return handleHocVienRequest(req, url, requestId);
    }

    return json({ error: "Not Found" }, 404);
  },
});

console.log(`Server running at http://localhost:${server.port}`);

