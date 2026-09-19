// Versioned entry point: changing this filename forces Chrome to replace the
// cached unpacked-extension service worker before loading the current logic.
importScripts('background.js?v=0.2.7');
