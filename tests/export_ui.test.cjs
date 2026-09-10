const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");

function setup(fetch) {
  const buttons = ["all", "filtered"].map((scope) => ({
    href: `/comparisons/example/export?scope=${scope}`,
    attributes: {},
    addEventListener(_event, handler) { this.click = handler; },
    setAttribute(name, value) { this.attributes[name] = value; },
    removeAttribute(name) { delete this.attributes[name]; },
  }));
  const status = { hidden: true, dataset: {} };
  const downloads = [];
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../static/export.js"), "utf8"), {
    document: {
      querySelectorAll: () => buttons,
      getElementById: () => status,
      body: { appendChild() {} },
      createElement: () => ({ click() { downloads.push(this.download); }, remove() {} }),
    },
    fetch,
    URL: { createObjectURL: () => "blob:test", revokeObjectURL() {} },
    setTimeout: (callback) => callback(),
    TypeError,
  });
  return { buttons, status, downloads };
}

for (const index of [0, 1]) {
  test(`export button ${index} shows pending, blocks duplicates and downloads`, async () => {
    let resolve;
    const requests = [];
    const pending = new Promise((done) => { resolve = done; });
    const { buttons, status, downloads } = setup((url) => { requests.push(url); return pending; });
    const click = buttons[index].click({ preventDefault() {} });
    assert.equal(status.hidden, false);
    assert.equal(status.dataset.state, "pending");
    assert.ok(buttons.every((button) => button.attributes["aria-disabled"] === "true"));
    await buttons[1 - index].click({ preventDefault() {} });
    assert.equal(requests.length, 1);
    resolve({ ok: true, headers: { get: (name) => name === "content-type" ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" : 'attachment; filename="results.xlsx"' }, blob: async () => ({}) });
    await click;
    assert.deepEqual(downloads, ["results.xlsx"]);
    assert.equal(status.dataset.state, "success");
    assert.ok(buttons.every((button) => !button.attributes["aria-disabled"]));
  });
}

for (const mode of ["http", "network", "invalid-file"]) {
  test(`export ${mode} failure restores controls without downloading`, async () => {
    const { buttons, status, downloads } = setup(async () => {
      if (mode === "network") throw new TypeError("offline");
      return { ok: mode !== "http", status: 500, headers: { get: () => "text/html" } };
    });
    await buttons[0].click({ preventDefault() {} });
    assert.equal(status.dataset.state, "error");
    assert.equal(downloads.length, 0);
    assert.ok(buttons.every((button) => !button.attributes["aria-disabled"]));
  });
}
