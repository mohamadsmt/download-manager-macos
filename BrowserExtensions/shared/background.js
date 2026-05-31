const HOST_NAME = "com.mohamadsmt.downloadmanager";

chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({
    id: "download-with-manager",
    title: "Download with Download Manager",
    contexts: ["link", "page", "video", "audio"]
  });
});

chrome.contextMenus.onClicked.addListener((info, tab) => {
  const url = info.linkUrl || info.srcUrl || info.pageUrl || tab?.url;
  if (!url || !/^https?:\/\//i.test(url)) return;

  chrome.runtime.sendNativeMessage(
    HOST_NAME,
    {
      url,
      referrer: info.pageUrl || tab?.url || null,
      suggestedFileName: null,
      headers: {},
      receivedAt: new Date().toISOString()
    },
    () => {
      if (chrome.runtime.lastError) {
        const fallback = `downloadmanager://add?url=${encodeURIComponent(url)}`;
        chrome.tabs.create({ url: fallback });
      }
    }
  );
});
