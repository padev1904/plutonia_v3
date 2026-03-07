(function () {
  function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(";").shift();
    return "";
  }

  document.body.addEventListener("htmx:configRequest", function (event) {
    const csrf = getCookie("csrftoken");
    if (csrf) {
      event.detail.headers["X-CSRFToken"] = csrf;
    }
  });
})();
