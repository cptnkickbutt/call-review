(function () {
    const refreshKey = "callreview_auto_refresh";
    const helpSeenKey = "callreview_help_seen";
    const helpHideKey = "callreview_help_hide_future";

    const checkbox = document.getElementById("auto-refresh-toggle");
    const helpOverlay = document.getElementById("help-overlay");
    const helpOpenButton = document.getElementById("help-open-button");
    const helpCloseButton = document.getElementById("help-close-button");
    const helpHideFuture = document.getElementById("help-hide-future");

    if (checkbox) {
        const saved = window.localStorage.getItem(refreshKey);
        if (saved === "true") {
            checkbox.checked = true;
        }

        let intervalId = null;

        function startAutoRefresh() {
            if (intervalId) return;
            intervalId = window.setInterval(function () {
                window.location.reload();
            }, 30000);
        }

        function stopAutoRefresh() {
            if (!intervalId) return;
            window.clearInterval(intervalId);
            intervalId = null;
        }

        checkbox.addEventListener("change", function () {
            window.localStorage.setItem(refreshKey, checkbox.checked ? "true" : "false");
            if (checkbox.checked) {
                startAutoRefresh();
            } else {
                stopAutoRefresh();
            }
        });

        if (checkbox.checked) {
            startAutoRefresh();
        }
    }

    function openHelp() {
        if (!helpOverlay) return;
        helpOverlay.classList.add("show");
        helpOverlay.setAttribute("aria-hidden", "false");

        if (helpHideFuture) {
            const hidden = window.localStorage.getItem(helpHideKey) === "true";
            helpHideFuture.checked = hidden;
        }
    }

    function closeHelp() {
        if (!helpOverlay) return;
        helpOverlay.classList.remove("show");
        helpOverlay.setAttribute("aria-hidden", "true");

        window.localStorage.setItem(helpSeenKey, "true");

        if (helpHideFuture) {
            window.localStorage.setItem(helpHideKey, helpHideFuture.checked ? "true" : "false");
        }
    }

    if (helpOpenButton) {
        helpOpenButton.addEventListener("click", openHelp);
    }

    if (helpCloseButton) {
        helpCloseButton.addEventListener("click", closeHelp);
    }

    if (helpOverlay) {
        helpOverlay.addEventListener("click", function (event) {
            if (event.target === helpOverlay) {
                closeHelp();
            }
        });
    }

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape" && helpOverlay && helpOverlay.classList.contains("show")) {
            closeHelp();
        }
    });

    const helpSeen = window.localStorage.getItem(helpSeenKey) === "true";
    const helpHide = window.localStorage.getItem(helpHideKey) === "true";

    if (!helpSeen && !helpHide) {
        openHelp();
    }
})();