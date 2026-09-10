(() => {
  const buttons = [...document.querySelectorAll("[data-export-download]")];
  const status = document.getElementById("export-status");
  if (!status || !buttons.length) return;
  let exporting = false;

  for (const button of buttons) {
    button.addEventListener("click", async (event) => {
      event.preventDefault();
      if (exporting) return;
      exporting = true;
      status.hidden = false;
      status.textContent = "正在导出，请稍候… 数据较多时需要一些时间，请勿重复点击。";
      status.dataset.state = "pending";
      for (const control of buttons) {
        control.setAttribute("aria-disabled", "true");
        control.setAttribute("aria-busy", "true");
      }
      let downloadUrl;
      try {
        const response = await fetch(button.href);
        if (!response.ok) throw new Error(`导出失败（HTTP ${response.status}），请稍后重试。`);
        const contentType = response.headers.get("content-type") || "";
        if (!contentType.includes("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) {
          throw new Error("未收到有效的 Excel 文件，请刷新页面后重试。");
        }
        const blob = await response.blob();
        downloadUrl = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = downloadUrl;
        const disposition = response.headers.get("content-disposition") || "";
        const filename = disposition.match(/filename="([^"]+)"/);
        link.download = filename ? filename[1] : "投诉比对结果.xlsx";
        document.body.appendChild(link);
        link.click();
        link.remove();
        status.textContent = "导出文件已生成，已发起下载。";
        status.dataset.state = "success";
      } catch (error) {
        status.textContent = error instanceof TypeError ? "网络连接异常，导出未完成，请重试。" : error.message;
        status.dataset.state = "error";
      } finally {
        exporting = false;
        for (const control of buttons) {
          control.removeAttribute("aria-disabled");
          control.removeAttribute("aria-busy");
        }
        if (downloadUrl) setTimeout(() => URL.revokeObjectURL(downloadUrl), 60000);
      }
    });
  }
})();
