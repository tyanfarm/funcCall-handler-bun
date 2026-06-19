import { handleChuongTrinhRequest } from "./services/chuongTrinhService";
import { handleChuongTrinhMonHocRequest } from "./services/chuongTrinhMonHocService";
import { handleMonHocBaiHocRequest } from "./services/monHocBaiHocService";
import { handleKhoaHocRequest } from "./services/khoaHocService";
import { handleKhoaHocLopHocRequest } from "./services/khoaHocLopHocService";
import { handleHocVienLopHocRequest } from "./services/hocVienLopHocService";
import { handleChuongTrinhKhoaHocRequest } from "./services/chuongTrinhKhoaHocService";
import { handleHocVienRequest } from "./services/hocVienService";
import { handleKhoaHocKeHoachDaoTaoChiTietRequest } from "./services/khoaHocKeHoachDaoTaoChiTietService";
import { handleHocVienBangDiemRequest } from "./services/hocVienBangDiemService";
import { handleGiangDuongRequest } from "./services/giangDuongService";
import { handleDoiTuongDaoTaoRequest } from "./services/doiTuongDaoTaoService";
import { handlePhanCongGiangDayRequest } from "./services/phanCongGiangDayService";
import { handleHocVienDiemTongKetRequest } from "./services/hocVienDiemTongKetService";
import { handleDocsRequest } from "./docs/docsService";

const port = Number(process.env.PORT || 3000);
const doiTuongDaoTaoCronHours = 6;

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

function getNextCronRun(now: Date, hourInterval: number): Date {
  const next = new Date(now);
  next.setMilliseconds(0);
  next.setSeconds(0);
  next.setMinutes(0);

  const currentHour = now.getHours();
  const nextHour = Math.floor(currentHour / hourInterval) * hourInterval + hourInterval;

  if (nextHour >= 24) {
    next.setDate(next.getDate() + 1);
    next.setHours(0);
    return next;
  }

  next.setHours(nextHour);
  return next;
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

    if (url.pathname === "/api/chuongTrinh/details" && req.method === "POST") {
      return handleChuongTrinhMonHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/chuongTrinh/list" && req.method === "POST") {
      return handleChuongTrinhRequest(req, url, requestId);
    }

    if (url.pathname === "/api/chuongTrinhKhoaHoc/search" && req.method === "POST") {
      return handleChuongTrinhKhoaHocRequest(req, url, requestId);
    }

    if ((url.pathname === "/api/monHoc/details" || url.pathname === "/api/ctmhMh") && req.method === "POST") {
      return handleMonHocBaiHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/khoaHoc/list" && req.method === "POST") {
      return handleKhoaHocRequest(req, url, requestId);
    }

    if ((url.pathname === "/api/khoaHocLopHoc/search" || url.pathname === "/api/khoaHoc/lopHoc") && req.method === "POST") {
      return handleKhoaHocLopHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/lopHoc/details" && req.method === "POST") {
      return handleHocVienLopHocRequest(req, url, requestId);
    }

    if (url.pathname === "/api/hocVien/list" && req.method === "POST") {
      return handleHocVienRequest(req, url, requestId);
    }

    if (url.pathname === "/api/hocVienBangDiem/search" && req.method === "POST") {
      const cloned = req.clone();
      const body = await cloned.json() as Record<string, unknown>;
      if (!body.namHoc) {
        return json({ error: "Vui long cung cap nam hoc (vi du: 2024-2025) de tra cuu bang diem." }, 400);
      }
      return handleHocVienBangDiemRequest(req, url, requestId);
    }

    if ((url.pathname === "/api/giangDuong/list" || url.pathname === "/api/giangDuong") && req.method === "POST") {
      return handleGiangDuongRequest(req, url, requestId);
    }

    if ((url.pathname === "/api/doiTuongDaoTao/list" || url.pathname === "/api/doiTuongDaoTao") && req.method === "POST") {
      return handleDoiTuongDaoTaoRequest(req, url, requestId);
    }

    if ((url.pathname === "/api/phanCongGiangDay/search" || url.pathname === "/api/phanCongGiangDay") && req.method === "POST") {
      return handlePhanCongGiangDayRequest(req, url, requestId);
    }

    if (url.pathname === "/api/hocVienDiemTongKet/search" && req.method === "POST") {
      return handleHocVienDiemTongKetRequest(req, url, requestId);
    }

    if (url.pathname === "/api/khoaHocKeHoachDaoTaoChiTiet/search" && req.method === "POST") {
      const cloned = req.clone();
      const body = await cloned.json() as Record<string, unknown>;
      if (!body.namHoc) {
        return json({ error: "Vui long cung cap nam hoc (vi du: 2024-2025) de tra cuu ke hoach dao tao chi tiet." }, 400);
      }
      return handleKhoaHocKeHoachDaoTaoChiTietRequest(req, url, requestId);
    }

    return json({ error: "Not Found" }, 404);
  },
});

console.log(`Server running at http://localhost:${server.port}`);

let isDoiTuongDaoTaoCronRunning = false;

async function triggerDoiTuongDaoTaoCron(): Promise<void> {
  if (isDoiTuongDaoTaoCronRunning) {
    console.warn("[cron][doiTuongDaoTao] Previous run still in progress, skipping this slot.");
    return;
  }

  isDoiTuongDaoTaoCronRunning = true;
  const requestId = `cron-doiTuongDaoTao-${createRequestId()}`;
  const endpoint = `http://127.0.0.1:${server.port}/api/doiTuongDaoTao/list`;
  const startedAt = Date.now();

  try {
    console.log(`[cron][doiTuongDaoTao] Triggering ${endpoint}`, {
      requestId,
      scheduledAt: new Date().toISOString(),
    });

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-request-id": requestId,
      },
      body: "{}",
    });
    const responseText = await response.text();
    const durationMs = Date.now() - startedAt;

    if (!response.ok) {
      console.error(`[cron][doiTuongDaoTao] Request failed`, {
        requestId,
        status: response.status,
        durationMs,
        body: responseText,
      });
      return;
    }

    console.log(`[cron][doiTuongDaoTao] Request completed`, {
      requestId,
      status: response.status,
      durationMs,
      responseBytes: responseText.length,
    });
  } catch (error) {
    console.error("[cron][doiTuongDaoTao] Unhandled scheduler error", {
      requestId,
      error: error instanceof Error ? error.message : String(error),
    });
  } finally {
    isDoiTuongDaoTaoCronRunning = false;
  }
}

function scheduleDoiTuongDaoTaoCron(): void {
  const now = new Date();
  const nextRun = getNextCronRun(now, doiTuongDaoTaoCronHours);
  const delayMs = Math.max(nextRun.getTime() - now.getTime(), 0);

  console.log("[cron][doiTuongDaoTao] Next run scheduled", {
    nextRun: nextRun.toISOString(),
    delayMs,
  });

  setTimeout(async () => {
    await triggerDoiTuongDaoTaoCron();
    scheduleDoiTuongDaoTaoCron();
  }, delayMs);
}

scheduleDoiTuongDaoTaoCron();
