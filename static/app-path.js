(function () {
  function normalizeBasePath(value) {
    var prefix = String(value || "").trim();
    if (!prefix || prefix === "/") return "";
    return "/" + prefix.replace(/^\/+|\/+$/g, "");
  }

  var appBasePath = normalizeBasePath(window.__APP_BASE_PATH__);

  window.appUrl = function appUrl(path) {
    var value = String(path || "/");
    if (/^(https?:|mailto:|tel:|data:|blob:|#)/i.test(value) || value.indexOf("//") === 0) return value;
    var normalized = value.indexOf("/") === 0 ? value : "/" + value;
    if (!appBasePath) return normalized;
    if (
      normalized === appBasePath ||
      normalized.indexOf(appBasePath + "/") === 0 ||
      normalized.indexOf(appBasePath + "?") === 0
    ) {
      return normalized;
    }
    return normalized === "/" ? appBasePath + "/" : appBasePath + normalized;
  };
})();
