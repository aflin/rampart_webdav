/* ==========================================================================
 * File Manager — WebDAV Client Application
 *
 * A modern file manager for WebDAV servers. Organized into modules:
 *   DavClient  — HTTP/WebDAV protocol wrapper
 *   Auth       — Login, logout, session management
 *   Tree       — Sidebar directory tree (lazy-loaded)
 *   FileList   — Detail table / grid view with sorting
 *   Toolbar    — Breadcrumb, view/sort toggles, action buttons
 *   Upload     — Drag-drop and file picker with progress
 *   Viewers    — Image, code, video, audio, PDF, markdown viewers
 *   Clipboard  — Copy/cut/paste state
 *   Dialog     — Modal dialogs (confirm, prompt, viewer host)
 *   Toast      — Notification toasts
 *   App        — Top-level controller, navigation, event wiring
 * ========================================================================== */

'use strict';

/* -----------------------------------------------------------------------
 * CDN URLs — set useLocalScripts=true in index.html to use local copies
 * ----------------------------------------------------------------------- */
// Derive base path from the current page location (e.g. /filemanager/ or /myapp/)
const _BASE = window.location.pathname.replace(/\/[^\/]*$/, '') || '';
const _L = _BASE + '/js/local/';
const CDN = (typeof useLocalScripts !== 'undefined' && useLocalScripts) ? {
  jsmediatags:       _L + 'jsmediatags.min.js',
  videojsCss:        _L + 'video-js.min.css',
  videojsJs:         _L + 'video.min.js',
  pdfjsJs:           _L + 'pdf.min.js',
  pdfjsWorker:       _L + 'pdf.worker.min.js',
  filerobot:         _L + 'filerobot-image-editor.min.js',
  joditCss:          _L + 'jodit.min.css',
  joditJs:           _L + 'jodit.min.js',
  jszip:             _L + 'jszip.min.js',
  epubjs:            _L + 'epub.min.js',
  marked:            _L + 'marked.min.js',
  xtermCss:          _L + 'xterm.min.css',
  xtermJs:           _L + 'xterm.min.js',
  xtermFit:          _L + 'xterm-addon-fit.min.js',
  cmBundle:          _L + 'codemirror-bundle.js',
  beautifyJs:        _L + 'beautify.min.js',
  beautifyHtmlJs:    _L + 'beautify-html.min.js',
  aceJs:             _L + 'ace.js',
  esm:               null,
} : {
  jsmediatags:       'https://cdn.jsdelivr.net/npm/jsmediatags@3.9.7/dist/jsmediatags.min.js',
  videojsCss:        'https://cdn.jsdelivr.net/npm/video.js@8/dist/video-js.min.css',
  videojsJs:         'https://cdn.jsdelivr.net/npm/video.js@8/dist/video.min.js',
  pdfjsJs:           'https://cdn.jsdelivr.net/npm/pdfjs-dist@3/build/pdf.min.js',
  pdfjsWorker:       'https://cdn.jsdelivr.net/npm/pdfjs-dist@3/build/pdf.worker.min.js',
  filerobot:         'https://scaleflex.cloudimg.io/v7/plugins/filerobot-image-editor/latest/filerobot-image-editor.min.js',
  joditCss:          'https://cdnjs.cloudflare.com/ajax/libs/jodit/4.6.13/es2018/jodit.min.css',
  joditJs:           'https://cdnjs.cloudflare.com/ajax/libs/jodit/4.6.13/es2018/jodit.min.js',
  jszip:             'https://cdn.jsdelivr.net/npm/jszip@3/dist/jszip.min.js',
  epubjs:            'https://cdn.jsdelivr.net/npm/epubjs@0.3/dist/epub.min.js',
  marked:            'https://cdn.jsdelivr.net/npm/marked@15/marked.min.js',
  xtermCss:          'https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.min.css',
  xtermJs:           'https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.min.js',
  xtermFit:          'https://cdn.jsdelivr.net/npm/xterm-addon-fit@0.8.0/lib/xterm-addon-fit.min.js',
  beautifyJs:        'https://cdnjs.cloudflare.com/ajax/libs/js-beautify/1.14.4/beautify.min.js',
  beautifyHtmlJs:    'https://cdnjs.cloudflare.com/ajax/libs/js-beautify/1.14.4/beautify-html.min.js',
  aceJs:             'https://cdnjs.cloudflare.com/ajax/libs/ace/1.4.2/ace.js',
  cmBundle:          null,
  esm:               'https://esm.sh/',
};

/* -----------------------------------------------------------------------
 * Section 1: DavClient — WebDAV HTTP wrapper
 * ----------------------------------------------------------------------- */

// Check if a WebDAV response status indicates success (200-207 range)
function isDavOk(resp) {
  return resp.status >= 200 && resp.status < 300;
}

function isValidFileName(name) {
  if (!name || name === '.' || name === '..') return false;
  if (/[\/\x00]/.test(name)) return false;
  return true;
}

const DavClient = {
  async send(method, url, body, extraHeaders) {
    const headers = Object.assign({}, extraHeaders || {});
    const resp = await fetch(url, { method, headers, body: body || undefined, credentials: 'same-origin' });
    if (resp.status === 401) { Auth.handle401(); throw new Error('Unauthorized'); }
    return resp;
  },

  async list(url, depth) {
    if (depth === undefined) depth = 1;
    const body = '<?xml version="1.0" encoding="UTF-8"?>' +
      '<D:propfind xmlns:D="DAV:" xmlns:R="urn:rampart:dav"><D:prop>' +
      '<D:displayname/><D:resourcetype/><D:getcontentlength/>' +
      '<D:getcontenttype/><D:getlastmodified/><D:getetag/>' +
      '<R:owner/><R:permissions/><R:group/><R:shared/><R:fsreadable/><R:fswritable/>' +
      '</D:prop></D:propfind>';

    const resp = await this.send('PROPFIND', url, body, {
      'Depth': String(depth),
      'Content-Type': 'text/xml; charset=utf-8'
    });
    const text = await resp.text();
    return this._parseMultistatus(text, url);
  },

  async mkcol(url) {
    return this.send('MKCOL', url);
  },

  CHUNK_SIZE: 5 * 1024 * 1024,  // 5 MB per chunk
  CHUNK_THRESHOLD: 10 * 1024 * 1024,  // use chunked upload above 10 MB
  MAX_RETRIES: 5,

  async put(url, data, onProgress, abortCtrl) {
    // Use chunked upload for large files
    if (data && data.size && data.size > this.CHUNK_THRESHOLD) {
      return this._putChunked(url, data, onProgress, abortCtrl);
    }
    if (onProgress && typeof XMLHttpRequest !== 'undefined') {
      return this._putXHR(url, data, onProgress, abortCtrl);
    }
    return this.send('PUT', url, data);
  },

  _putXHR(url, data, onProgress, abortCtrl) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', url, true);
      xhr.withCredentials = true;
      if (abortCtrl) {
        abortCtrl.xhr = xhr;
        abortCtrl.onAbort = function() { xhr.abort(); };
      }
      xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) onProgress(e.loaded, e.total);
      };
      xhr.onload = () => {
        if (abortCtrl) abortCtrl.xhr = null;
        if (xhr.status === 401) { Auth.handle401(); reject(new Error('Unauthorized')); return; }
        resolve({ status: xhr.status, ok: xhr.status >= 200 && xhr.status < 300 });
      };
      xhr.onerror = () => {
        if (abortCtrl) abortCtrl.xhr = null;
        reject(new Error('Upload failed'));
      };
      xhr.onabort = () => {
        if (abortCtrl) abortCtrl.xhr = null;
        reject(new Error('Upload cancelled'));
      };
      xhr.send(data);
    });
  },

  async _putChunked(url, file, onProgress, abortCtrl) {
    const totalSize = file.size;
    const chunkSize = this.CHUNK_SIZE;
    const uploadId = crypto.randomUUID ? crypto.randomUUID()
      : 'u-' + Date.now() + '-' + Math.random().toString(36).slice(2);

    let offset = 0;
    while (offset < totalSize) {
      if (abortCtrl && abortCtrl.aborted) throw new Error('Upload cancelled');
      const end = Math.min(offset + chunkSize, totalSize);
      const chunk = file.slice(offset, end);

      let success = false;
      for (let attempt = 0; attempt < this.MAX_RETRIES; attempt++) {
        try {
          const resp = await this._sendChunk(url, chunk, {
            'X-Upload-Id': uploadId,
            'X-Chunk-Offset': String(offset),
            'X-Total-Size': String(totalSize)
          }, abortCtrl);
          if (resp.status === 401) {
            Auth.handle401();
            throw new Error('Unauthorized');
          }
          if (resp.status >= 200 && resp.status < 300) {
            success = true;
            break;
          }
          // Server error — retry
        } catch (e) {
          if (e.message === 'Unauthorized' || e.message === 'Upload cancelled') throw e;
          // Network error — retry after brief pause
          if (attempt < this.MAX_RETRIES - 1) {
            await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
          }
        }
      }

      if (!success) {
        throw new Error('Chunk upload failed after ' + this.MAX_RETRIES + ' retries at offset ' + offset);
      }

      offset = end;
      if (onProgress) onProgress(offset, totalSize);
    }

    return { status: 201, ok: true };
  },

  _sendChunk(url, chunk, extraHeaders, abortCtrl) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', url, true);
      xhr.withCredentials = true;
      Object.keys(extraHeaders).forEach(k => xhr.setRequestHeader(k, extraHeaders[k]));
      if (abortCtrl) {
        abortCtrl.xhr = xhr;
        abortCtrl.onAbort = function() { xhr.abort(); };
      }
      xhr.onload = () => {
        if (abortCtrl) abortCtrl.xhr = null;
        resolve({ status: xhr.status, ok: xhr.status >= 200 && xhr.status < 300 });
      };
      xhr.onerror = () => {
        if (abortCtrl) abortCtrl.xhr = null;
        reject(new Error('Network error'));
      };
      xhr.onabort = () => {
        if (abortCtrl) abortCtrl.xhr = null;
        reject(new Error('Upload cancelled'));
      };
      xhr.send(chunk);
    });
  },

  async del(url) {
    return this.send('DELETE', url);
  },

  async copyMove(method, srcUrl, destUrl, overwrite) {
    return this.send(method, srcUrl, null, {
      'Destination': destUrl,
      'Overwrite': overwrite ? 'T' : 'F'
    });
  },

  async getText(url) {
    const resp = await this.send('GET', url);
    return resp.text();
  },

  async getBlob(url) {
    const resp = await this.send('GET', url);
    return resp.blob();
  },

  async head(url) {
    return this.send('HEAD', url);
  },

  async createSymlink(targetDavPath, linkDavPath) {
    const resp = await fetch(App.davUrl + '_symlink', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: targetDavPath, link: linkDavPath })
    });
    return resp.json();
  },

  _parseMultistatus(xmlText, baseUrl) {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xmlText, 'text/xml');
    const results = [];
    const normalBase = baseUrl.replace(/\/?$/, '/');

    doc.querySelectorAll('response').forEach(node => {
      const hrefEl = node.querySelector('href');
      if (!hrefEl) return;
      const href = hrefEl.textContent;

      // Find the propstat with 200 status
      let props = null;
      node.querySelectorAll('propstat').forEach(ps => {
        const st = ps.querySelector('status');
        if (st && st.textContent.indexOf('200') !== -1) props = ps;
      });
      if (!props) return;

      const isDir = !!props.querySelector('resourcetype collection');
      const nameEl = props.querySelector('displayname');
      let name = nameEl ? nameEl.textContent : '';
      if (!name) {
        name = decodeURIComponent(href.replace(/\/$/, '').split('/').pop());
      }

      const sizeEl = props.querySelector('getcontentlength');
      const mimeEl = props.querySelector('getcontenttype');
      const modEl = props.querySelector('getlastmodified');
      const etagEl = props.querySelector('getetag');

      // Check for owner/permissions (custom namespace urn:rampart:dav)
      // Use :scope > prop > owner to avoid matching D:owner inside lockdiscovery
      const propEl = props.querySelector('prop');
      const ownerEl = propEl ? propEl.querySelector(':scope > owner') : null;
      const permEl = propEl ? propEl.querySelector(':scope > permissions') : null;
      const groupEl = propEl ? propEl.querySelector(':scope > group') : null;

      // Filesystem-level permissions (from mounted volumes)
      const fsReadableEl = propEl ? propEl.querySelector(':scope > fsreadable') : null;
      const fsWritableEl = propEl ? propEl.querySelector(':scope > fswritable') : null;

      // Check for symlink property (custom namespace urn:rampart:dav)
      const sharedEl = propEl ? propEl.querySelector(':scope > shared') : null;

      const symlinkEl = props.querySelector('symlink');
      let isSymlink = false, symlinkTarget = null, symlinkBroken = false;
      if (symlinkEl) {
        isSymlink = true;
        const targetEl = symlinkEl.querySelector('target');
        const brokenEl = symlinkEl.querySelector('broken');
        symlinkTarget = targetEl ? targetEl.textContent : '';
        symlinkBroken = brokenEl ? brokenEl.textContent === 'true' : false;
      }

      // Normalize href for comparison
      const normHref = href.replace(/\/?$/, '/');
      const isSelf = (normHref === normalBase || href === baseUrl.replace(/\/$/, ''));

      results.push({
        name: name,
        href: href,
        isDir: isDir,
        isSelf: isSelf,
        size: sizeEl ? parseInt(sizeEl.textContent, 10) : null,
        mime: mimeEl ? mimeEl.textContent : null,
        modified: modEl ? new Date(modEl.textContent) : null,
        etag: etagEl ? etagEl.textContent : null,
        owner: ownerEl ? ownerEl.textContent : null,
        permissions: permEl ? parseInt(permEl.textContent, 10) : null,
        group: groupEl ? groupEl.textContent : null,
        isSymlink: isSymlink,
        symlinkTarget: symlinkTarget,
        symlinkBroken: symlinkBroken,
        shared: sharedEl ? sharedEl.textContent === '1' : false,
        fsReadable: fsReadableEl ? fsReadableEl.textContent === '1' : true,
        fsWritable: fsWritableEl ? fsWritableEl.textContent === '1' : true
      });
    });

    return results;
  }
};


/* -----------------------------------------------------------------------
 * Section 2: Auth — Login, logout, session
 * ----------------------------------------------------------------------- */

const Auth = {
  username: null,
  groups: [],
  admin: false,

  async login(user, pass) {
    try {
      const resp = await fetch(App.davUrl + '_login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: user, password: pass })
      });
      const data = await resp.json();
      if (!data.ok) {
        return { ok: false, error: data.error || 'Login failed' };
      }
      this.username = user;
      sessionStorage.setItem('dav_user', user);
      if (data.remounted && data.remounted.length) {
        setTimeout(function() {
          Toast.success('Auto-mounted SSH: ' + data.remounted.join(', '));
        }, 500);
      }
      return { ok: true, requirePasswordChange: !!data.requirePasswordChange };
    } catch (e) {
      return { ok: false, error: 'Connection failed' };
    }
  },

  async logout() {
    try {
      await fetch(App.davUrl + '_logout', { method: 'POST' });
    } catch (e) {}
    this.username = null;
    sessionStorage.removeItem('dav_user');
    App.showLogin();
  },

  restoreSession() {
    // If we have a username in sessionStorage, the httpOnly cookie
    // may still be valid — try a PROPFIND to verify
    const user = sessionStorage.getItem('dav_user');
    if (!user) return false;
    this.username = user;
    return true;
  },

  async verifySession() {
    // Actually check if the cookie is still valid
    try {
      const resp = await fetch(App.davUrl + this.username + '/', {
        method: 'PROPFIND',
        headers: { 'Depth': '0', 'Content-Type': 'text/xml; charset=utf-8' },
        body: '<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/></D:prop></D:propfind>'
      });
      if (resp.status === 401) {
        this.username = null;
        sessionStorage.removeItem('dav_user');
        return false;
      }
      return true;
    } catch (e) {
      return false;
    }
  },

  handle401() {
    this.logout();
  },

  getUserHomeUrl() {
    return App.davUrl + this.username + '/';
  }
};


/* -----------------------------------------------------------------------
 * Section 3: Toast — Notification toasts
 * ----------------------------------------------------------------------- */

const Toast = {
  _container: null,

  _getContainer() {
    if (!this._container) {
      this._container = document.createElement('div');
      this._container.className = 'toast-container';
      document.body.appendChild(this._container);
    }
    return this._container;
  },

  show(message, type, duration) {
    if (duration === undefined || duration === null) duration = 4000;
    const c = this._getContainer();
    const el = document.createElement('div');
    el.className = 'toast' + (type ? ' toast-' + type : '');
    var msgSpan = document.createElement('span');
    msgSpan.className = 'toast-message';
    msgSpan.textContent = message;
    el.appendChild(msgSpan);
    if (duration < 0) {
      // Persistent — add close button
      var closeBtn = document.createElement('span');
      closeBtn.textContent = '\u00d7';
      closeBtn.style.cssText = 'margin-left:12px;cursor:pointer;font-size:18px;font-weight:bold;float:right;line-height:1';
      closeBtn.addEventListener('click', function() {
        el.style.opacity = '0';
        el.style.transition = 'opacity 0.3s';
        setTimeout(function() { el.remove(); }, 300);
      });
      el.appendChild(closeBtn);
    } else if (duration > 0) {
      setTimeout(() => {
        el.style.opacity = '0';
        el.style.transition = 'opacity 0.3s';
        setTimeout(() => el.remove(), 300);
      }, duration);
    }
    // duration === 0: persistent, no close button (managed externally)
    c.appendChild(el);
    return el;
  },

  error(msg) { this.show(msg, 'error', -1); },
  success(msg) { this.show(msg, 'success'); },
  warning(msg) { this.show(msg, 'warning', 5000); },
  info(msg, duration) { return this.show(msg, 'info', duration); }
};


// Overwrite confirmation prompt — returns Promise resolving to 'yes'|'no'|'yesAll'|'noAll'|'rename'|'renameAll'
function confirmOverwrite(fileName, remaining) {
  return new Promise(function(resolve) {
    const overlay = document.createElement('div');
    overlay.className = 'confirm-overlay';
    const box = document.createElement('div');
    box.className = 'confirm-box';
    box.innerHTML =
      '<p style="margin:0 0 12px;font-size:14px"><strong>' + _escHtml(fileName) + '</strong> already exists in the destination. Overwrite?</p>' +
      '<div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end">' +
        (remaining > 0 ? '<button class="btn btn-sm" data-v="noAll">No to All</button>' : '') +
        '<button class="btn btn-sm" data-v="no">No</button>' +
        (remaining > 0 ? '<button class="btn btn-sm" data-v="renameAll">Rename All</button>' : '') +
        '<button class="btn btn-sm" data-v="rename">Rename</button>' +
        (remaining > 0 ? '<button class="btn btn-sm" data-v="yesAll">Yes to All</button>' : '') +
        '<button class="btn btn-primary btn-sm" data-v="yes">Yes</button>' +
      '</div>' +
      '<div style="display:flex;justify-content:flex-end;margin-top:8px">' +
        '<button class="btn btn-sm" data-v="cancel" style="padding:4px 16px">Cancel</button>' +
      '</div>';
    overlay.appendChild(box);
    document.body.appendChild(overlay);
    box.addEventListener('click', function(e) {
      const val = e.target.getAttribute('data-v');
      if (val) { overlay.remove(); resolve(val); }
    });
  });
}

// Generate a non-conflicting filename by appending -1, -2, etc.
async function autoRename(targetUrl, fileName) {
  var dot = fileName.lastIndexOf('.');
  var base = dot > 0 ? fileName.substring(0, dot) : fileName;
  var ext = dot > 0 ? fileName.substring(dot) : '';
  var n = 1;
  var newName;
  while (true) {
    newName = base + '-' + n + ext;
    try {
      var resp = await DavClient.send('HEAD', targetUrl + encodeURIComponent(newName));
      if (resp.status >= 400) break; // doesn't exist
    } catch (e) { break; }
    n++;
    if (n > 999) break;
  }
  return newName;
}

// Error prompt for copy/move — returns Promise resolving to 'retry'|'skip'|'skipAll'|'abort'
function confirmCopyError(fileName, errorDetail, remaining) {
  return new Promise(function(resolve) {
    var overlay = document.createElement('div');
    overlay.className = 'confirm-overlay';
    var box = document.createElement('div');
    box.className = 'confirm-box';
    box.innerHTML =
      '<p style="margin:0 0 8px;font-size:14px">Error copying <strong>' + _escHtml(fileName) + '</strong></p>' +
      '<p style="margin:0 0 12px;font-size:12px;color:var(--color-text-muted);word-break:break-word">' + _escHtml(errorDetail) + '</p>' +
      '<div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end">' +
        '<button class="btn btn-sm btn-danger" data-v="abort">Abort</button>' +
        (remaining > 0 ? '<button class="btn btn-sm" data-v="skipAll">Skip All Errors</button>' : '') +
        '<button class="btn btn-sm" data-v="skip">Skip</button>' +
        '<button class="btn btn-primary btn-sm" data-v="retry">Retry</button>' +
      '</div>';
    overlay.appendChild(box);
    document.body.appendChild(overlay);
    box.addEventListener('click', function(e) {
      var val = e.target.getAttribute('data-v');
      if (val) { overlay.remove(); resolve(val); }
    });
  });
}

// Copy or move a directory tree one file at a time to avoid server timeouts.
// Enumerates all items first, creates dirs with MKCOL, copies/moves files individually.
// Shows progress panel with abort support.
// Returns number of items successfully processed.
async function copyMoveTree(method, srcUrl, destUrl, overwriteState, errorMsgs) {
  var aborted = false;
  var panel = document.getElementById('upload-progress');
  var progressBar = null;
  var progressText = null;
  var headerText = null;

  function initPanel(text) {
    panel.hidden = false;
    panel.innerHTML = '';
    var hdr = document.createElement('div');
    hdr.className = 'upload-header';
    headerText = document.createElement('span');
    headerText.textContent = text;
    hdr.appendChild(headerText);
    var closeBtn = document.createElement('button');
    closeBtn.className = 'modal-close';
    closeBtn.innerHTML = '&times;';
    closeBtn.onclick = async function() {
      var yes = await Dialog.confirm('Abort ' + (method === 'MOVE' ? 'move' : 'copy') + '?');
      if (yes) { aborted = true; panel.hidden = true; }
    };
    hdr.appendChild(closeBtn);
    panel.appendChild(hdr);
    var itemEl = document.createElement('div');
    itemEl.className = 'upload-item';
    itemEl.innerHTML = '<div class="upload-item-name"></div><progress value="0" max="100"></progress>';
    panel.appendChild(itemEl);
    progressBar = itemEl.querySelector('progress');
    progressText = itemEl.querySelector('.upload-item-name');
  }

  // Show panel immediately during enumeration
  var verb = method === 'MOVE' ? 'Moving' : 'Copying';
  initPanel(verb + ': scanning files...');
  progressBar.removeAttribute('value'); // indeterminate

  // Phase 1: Enumerate all items recursively
  var items = [];
  var enumCount = 0;
  var skipEnumErrors = false;
  async function enumerate(url, relPath) {
    if (aborted) return;
    var listing;
    var dirName = decodeURIComponent(url.split('/').filter(Boolean).pop());
    for (;;) {
      try {
        listing = await DavClient.list(url, 1);
        break;
      } catch (e) {
        if (skipEnumErrors) { errorMsgs.push(dirName + ': ' + (e.message || 'Failed to list')); return; }
        var choice = await confirmCopyError(dirName, e.message || 'Failed to list', 1);
        if (choice === 'retry') continue;
        if (choice === 'skipAll') skipEnumErrors = true;
        if (choice === 'abort') aborted = true;
        errorMsgs.push(dirName + ': ' + (e.message || 'Failed to list'));
        return;
      }
    }
    var children = listing.filter(function(s) { return !s.isSelf; });
    for (var i = 0; i < children.length; i++) {
      if (aborted) return;
      var child = children[i];
      var isRealDir = child.isDir && !child.isSymlink;
      var childRel = relPath + encodeURIComponent(child.name) + (child.isDir ? '/' : '');
      items.push({ src: child.href, rel: childRel, isDir: isRealDir, name: child.name });
      enumCount++;
      progressText.textContent = 'Found ' + enumCount + ' items...';
      if (isRealDir) {
        await enumerate(child.href, childRel);
      }
    }
  }
  await enumerate(srcUrl, '');

  if (aborted || items.length === 0) {
    panel.hidden = true;
    return 0;
  }

  // Phase 2: Ensure destination root exists
  try { await DavClient.mkcol(destUrl); } catch (e) { /* 405 = already exists, OK */ }

  // Phase 3: Process items with progress
  var total = items.length;
  var completed = 0;
  var skipped = 0;
  var skipErrors = false;
  headerText.textContent = verb + ' ' + total + ' items...';
  progressBar.value = 0;

  // Handle an error with retry/skip/skipAll/abort prompt
  async function handleError(fileName, detail, remaining) {
    if (skipErrors) { errorMsgs.push(fileName + ': ' + detail); return 'skip'; }
    var choice = await confirmCopyError(fileName, detail, remaining);
    if (choice === 'skipAll') { skipErrors = true; errorMsgs.push(fileName + ': ' + detail); return 'skip'; }
    if (choice === 'abort') { aborted = true; errorMsgs.push(fileName + ': ' + detail); }
    return choice; // 'retry', 'skip', or 'abort'
  }

  for (var i = 0; i < items.length; i++) {
    if (aborted) break;
    var item = items[i];
    var itemDest = destUrl + item.rel;
    progressBar.value = Math.round(completed / total * 100);
    progressText.textContent = completed + '/' + total + ': ' + item.name;

    if (item.isDir) {
      // Create directory (201 = created, 405 = already exists)
      for (;;) {
        try {
          var mkResp = await DavClient.mkcol(itemDest);
          if (mkResp.status !== 201 && mkResp.status !== 405) {
            var ea = await handleError(item.name + '/', 'Status ' + mkResp.status, total - i - 1);
            if (ea === 'retry') continue;
          }
          break;
        } catch (e) {
          var ea2 = await handleError(item.name + '/', e.message || 'Failed', total - i - 1);
          if (ea2 === 'retry') continue;
          break;
        }
      }
      completed++;
    } else {
      // Copy/move individual file (or symlink)
      for (;;) {
        try {
          var resp = await DavClient.copyMove(method, item.src, itemDest, false);
          if (resp.status === 412) {
            // File exists at destination — ask to overwrite
            var action;
            if (overwriteState.value === 'cancel') { skipped++; break; }
            else if (overwriteState.value === true) action = 'yes';
            else if (overwriteState.value === false) action = 'no';
            else if (overwriteState.value === 'rename') action = 'rename';
            else {
              var remaining = total - i - 1;
              action = await confirmOverwrite(item.name, remaining);
              if (action === 'yesAll') { overwriteState.value = true; action = 'yes'; }
              else if (action === 'noAll') { overwriteState.value = false; action = 'no'; }
              else if (action === 'renameAll') { overwriteState.value = 'rename'; action = 'rename'; }
              else if (action === 'cancel') { overwriteState.value = 'cancel'; skipped++; break; }
            }
            if (action === 'no') { skipped++; break; }
            if (action === 'rename') {
              var rnName = await autoRename(itemDest.substring(0, itemDest.lastIndexOf('/') + 1), item.name);
              itemDest = itemDest.substring(0, itemDest.lastIndexOf('/') + 1) + encodeURIComponent(rnName);
            }
            resp = await DavClient.copyMove(method, item.src, itemDest, true);
          }
          if (!isDavOk(resp)) {
            var txt = await resp.text().catch(function() { return ''; });
            var ea3 = await handleError(item.name, txt || 'Error ' + resp.status, total - i - 1);
            if (ea3 === 'retry') continue;
          }
          break;
        } catch (e) {
          var ea4 = await handleError(item.name, e.message || 'Failed', total - i - 1);
          if (ea4 === 'retry') continue;
          break;
        }
      }
      completed++;
    }
  }

  // Phase 4: For MOVE, clean up source directories bottom-up
  if (method === 'MOVE' && skipped === 0 && !aborted) {
    try { await DavClient.del(srcUrl); } catch (e) { /* ignore */ }
  }

  // Update and auto-hide progress
  if (!panel.hidden) {
    progressBar.value = 100;
    progressText.textContent = aborted ? 'Aborted at ' + completed + '/' + total : 'Done';
    setTimeout(function() { panel.hidden = true; }, 2000);
  }

  return completed;
}

/* -----------------------------------------------------------------------
 * Section 4: Dialog — Modal dialogs
 * ----------------------------------------------------------------------- */

const Dialog = {
  _overlay: null,
  _modal: null,
  _body: null,
  _title: null,
  _footer: null,
  _closeBtn: null,
  _maxBtn: null,
  _headerActions: null,
  _onClose: null,
  _maximized: false,
  _initialized: false,

  init() {
    if (this._initialized) return;
    this._initialized = true;
    this._overlay = document.getElementById('modal-overlay');
    this._modal = document.getElementById('modal');
    this._body = document.getElementById('modal-body');
    this._title = document.getElementById('modal-title');
    this._footer = document.getElementById('modal-footer');
    this._closeBtn = document.getElementById('modal-close');
    this._maxBtn = document.getElementById('modal-maximize');
    this._headerActions = document.getElementById('modal-header-actions');
    this._headerTrigger = document.getElementById('modal-header-trigger');
    this._headerEl = document.getElementById('modal-header');
    this._headerHideTimer = null;

    this._closeBtn.addEventListener('click', () => this.close());
    this._maxBtn.addEventListener('click', () => this.toggleMaximize());

    // Maximized header show/hide on hover
    this._headerTrigger.addEventListener('mouseenter', () => {
      if (!this._maximized) return;
      this._showMaxHeader();
    });
    this._headerEl.addEventListener('mouseenter', () => {
      if (!this._maximized) return;
      clearTimeout(this._headerHideTimer);
    });
    this._headerEl.addEventListener('mouseleave', () => {
      if (!this._maximized) return;
      this._scheduleHideMaxHeader();
    });
    this._headerTrigger.addEventListener('mouseleave', () => {
      if (!this._maximized) return;
      this._scheduleHideMaxHeader();
    });

    // Drag modal by its header
    const header = document.getElementById('modal-header');
    let dragging = false, dragX, dragY;
    header.style.cursor = 'grab';
    header.addEventListener('dblclick', (e) => {
      if (e.target.closest('button, input, .modal-header-actions')) return;
      this.toggleMaximize();
    });
    header.addEventListener('mousedown', (e) => {
      // Don't drag if clicking a button or input
      if (e.target.closest('button, input, .modal-header-actions')) return;
      if (this._maximized) return;
      dragging = true;
      const rect = this._modal.getBoundingClientRect();
      dragX = e.clientX - rect.left;
      dragY = e.clientY - rect.top;
      header.style.cursor = 'grabbing';
      this._modal.style.transition = 'none';
      e.preventDefault();
    });
    document.addEventListener('mousemove', (e) => {
      if (!dragging) return;
      const x = e.clientX - dragX;
      const y = e.clientY - dragY;
      this._modal.style.position = 'fixed';
      this._modal.style.left = x + 'px';
      this._modal.style.top = y + 'px';
      this._modal.style.margin = '0';
    });
    document.addEventListener('mouseup', () => {
      if (!dragging) return;
      dragging = false;
      header.style.cursor = 'grab';
      this._modal.style.transition = '';
    });
  },

  open(title, content, opts) {
    opts = opts || {};
    this._title.textContent = title;
    this._body.innerHTML = '';
    this._headerActions.innerHTML = '';
    this._footer.innerHTML = '';
    this._footer.hidden = true;
    this._maximized = false;
    this._modal.classList.remove('modal-maximized');

    if (typeof content === 'string') {
      this._body.innerHTML = content;
    } else if (content instanceof HTMLElement) {
      this._body.appendChild(content);
    }

    this._modal.className = 'modal' +
      (opts.wide ? ' modal-wide' : '') +
      (opts.full ? ' modal-full' : '') +
      (opts.noPadding ? ' modal-no-padding' : '');
    this._resetPosition();
    this._overlay.hidden = false;
    this._onClose = opts.onClose || null;
    this._beforeClose = opts.beforeClose || null;

    // Pin to computed top-left so resize always grows down-right
    requestAnimationFrame(() => {
      if (this._maximized) return;
      const rect = this._modal.getBoundingClientRect();
      this._modal.style.position = 'fixed';
      this._modal.style.left = rect.left + 'px';
      this._modal.style.top = rect.top + 'px';
      this._modal.style.margin = '0';
    });

    if (opts.headerActions) {
      opts.headerActions.forEach(a => this._headerActions.appendChild(a));
    }
    if (opts.footer) {
      opts.footer.forEach(b => this._footer.appendChild(b));
      this._footer.hidden = false;
    }
  },

  async close() {
    if (this._beforeClose) {
      var ok = this._beforeClose();
      if (ok && typeof ok.then === 'function') ok = await ok;
      if (!ok) return;
    }
    this._beforeClose = null;
    this._overlay.hidden = true;
    this._body.innerHTML = '';
    this._modal.className = 'modal';
    this._maximized = false;
    if (this._onClose) { this._onClose(); this._onClose = null; }
  },

  toggleMaximize() {
    this._maximized = !this._maximized;
    this._modal.classList.toggle('modal-maximized', this._maximized);
    if (this._maximized) {
      this._resetPosition();
      this._headerEl.classList.remove('header-visible');
    } else {
      clearTimeout(this._headerHideTimer);
      this._headerEl.classList.remove('header-visible');
    }
    window.dispatchEvent(new Event('resize'));
  },

  _showMaxHeader() {
    clearTimeout(this._headerHideTimer);
    this._headerEl.classList.add('header-visible');
  },

  _scheduleHideMaxHeader() {
    clearTimeout(this._headerHideTimer);
    this._headerHideTimer = setTimeout(() => {
      this._headerEl.classList.remove('header-visible');
    }, 2000);
  },

  _resetPosition() {
    this._modal.style.position = '';
    this._modal.style.left = '';
    this._modal.style.top = '';
    this._modal.style.margin = '';
    this._modal.style.width = '';
    this._modal.style.height = '';
  },

  isOpen() {
    return !this._overlay.hidden;
  },

  alert(message) {
    var msgEl = document.createElement('div');
    msgEl.className = 'dialog-message';
    msgEl.textContent = message;
    var okBtn = document.createElement('button');
    okBtn.className = 'btn btn-primary';
    okBtn.textContent = 'OK';
    okBtn.addEventListener('click', () => { this._onClose = null; this.close(); });
    this.open('Notice', msgEl, { footer: [okBtn] });
    okBtn.focus();
  },

  async confirm(message, confirmLabel, danger) {
    // If a dialog is already open, use a floating overlay so we don't destroy it
    if (this.isOpen()) return this._confirmOverlay(message, confirmLabel, danger);

    return new Promise(resolve => {
      const msgEl = document.createElement('div');
      msgEl.className = 'dialog-message';
      msgEl.textContent = message;

      const cancelBtn = document.createElement('button');
      cancelBtn.className = 'btn';
      cancelBtn.textContent = 'Cancel';

      const okBtn = document.createElement('button');
      okBtn.className = danger ? 'btn btn-danger' : 'btn btn-primary';
      okBtn.textContent = confirmLabel || 'OK';

      var done = (val) => { this._onClose = null; this.close(); resolve(val); };
      cancelBtn.addEventListener('click', () => done(false));
      okBtn.addEventListener('click', () => done(true));

      // Arrow keys switch focus between buttons
      [cancelBtn, okBtn].forEach(function(btn, i, arr) {
        btn.addEventListener('keydown', function(e) {
          if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
            e.preventDefault();
            arr[1 - i].focus();
          }
        });
      });

      this.open('Confirm', msgEl, {
        footer: [cancelBtn, okBtn],
        onClose: () => resolve(false)
      });
      cancelBtn.focus();
    });
  },

  // Nested confirm — floats above an already-open dialog
  _confirmOverlay(message, confirmLabel, danger) {
    return new Promise(resolve => {
      var overlay = document.createElement('div');
      overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.35);z-index:200010;display:flex;align-items:center;justify-content:center';

      var box = document.createElement('div');
      box.style.cssText = 'background:var(--color-modal-bg,#fff);border:1px solid var(--color-modal-border,#ccc);border-radius:8px;padding:20px 24px 16px;max-width:400px;box-shadow:0 8px 24px rgba(0,0,0,0.3)';

      var msg = document.createElement('div');
      msg.style.cssText = 'margin-bottom:16px;font-size:14px;color:var(--color-fg,#333)';
      msg.textContent = message;

      var btnRow = document.createElement('div');
      btnRow.style.cssText = 'display:flex;justify-content:flex-end;gap:8px';

      var cancelBtn = document.createElement('button');
      cancelBtn.className = 'btn';
      cancelBtn.textContent = 'Cancel';

      var okBtn = document.createElement('button');
      okBtn.className = danger ? 'btn btn-danger' : 'btn btn-primary';
      okBtn.textContent = confirmLabel || 'OK';

      var done = function(val) { overlay.remove(); resolve(val); };
      cancelBtn.addEventListener('click', function() { done(false); });
      okBtn.addEventListener('click', function() { done(true); });

      [cancelBtn, okBtn].forEach(function(btn, i, arr) {
        btn.addEventListener('keydown', function(e) {
          if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
            e.preventDefault();
            arr[1 - i].focus();
          }
          if (e.key === 'Escape') { e.preventDefault(); done(false); }
          e.stopPropagation();
        });
      });

      btnRow.appendChild(cancelBtn);
      btnRow.appendChild(okBtn);
      box.appendChild(msg);
      box.appendChild(btnRow);
      overlay.appendChild(box);
      overlay.addEventListener('click', function(e) { if (e.target === overlay) done(false); });
      document.body.appendChild(overlay);
      cancelBtn.focus();
    });
  },

  async prompt(title, defaultValue, placeholder, opts) {
    return new Promise(resolve => {
      const input = document.createElement('input');
      input.type = (opts && opts.password) ? 'password' : 'text';
      input.className = 'dialog-input';
      input.value = defaultValue || '';
      input.placeholder = placeholder || '';

      const cancelBtn = document.createElement('button');
      cancelBtn.className = 'btn';
      cancelBtn.textContent = 'Cancel';

      const okBtn = document.createElement('button');
      okBtn.className = 'btn btn-primary';
      okBtn.textContent = 'OK';

      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { this._onClose = null; this.close(); resolve(input.value); }
      });
      cancelBtn.addEventListener('click', () => { this._onClose = null; this.close(); resolve(null); });
      okBtn.addEventListener('click', () => { this._onClose = null; this.close(); resolve(input.value); });

      this.open(title, input, {
        footer: [cancelBtn, okBtn],
        onClose: () => resolve(null)
      });

      // Select filename without extension
      input.focus();
      if (defaultValue) {
        const dot = defaultValue.lastIndexOf('.');
        if (dot > 0) {
          input.setSelectionRange(0, dot);
        } else {
          input.select();
        }
      }
    });
  },

  // Lightweight password prompt that doesn't take over the main modal.
  // Use this when Dialog is already open (e.g. inside Settings).
  miniPrompt(title, placeholder) {
    return new Promise(resolve => {
      var overlay = document.createElement('div');
      overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:300000;display:flex;align-items:center;justify-content:center';
      var box = document.createElement('div');
      box.style.cssText = 'background:var(--color-bg);border:1px solid var(--color-border);border-radius:8px;padding:20px;min-width:300px;max-width:400px;box-shadow:0 8px 32px rgba(0,0,0,.3)';
      var label = document.createElement('div');
      label.style.cssText = 'font-weight:600;margin-bottom:12px';
      label.textContent = title;
      var input = document.createElement('input');
      input.type = 'password';
      input.className = 'dialog-input';
      input.placeholder = placeholder || 'Password';
      input.style.cssText = 'width:100%;box-sizing:border-box;margin-bottom:12px';
      var btns = document.createElement('div');
      btns.style.cssText = 'display:flex;gap:8px;justify-content:flex-end';
      var cancelBtn = document.createElement('button');
      cancelBtn.className = 'btn';
      cancelBtn.textContent = 'Cancel';
      var okBtn = document.createElement('button');
      okBtn.className = 'btn btn-primary';
      okBtn.textContent = 'OK';

      function close(val) { document.body.removeChild(overlay); resolve(val); }
      cancelBtn.addEventListener('click', function() { close(null); });
      okBtn.addEventListener('click', function() { close(input.value); });
      input.addEventListener('keydown', function(e) { if (e.key === 'Enter') close(input.value); if (e.key === 'Escape') close(null); });
      overlay.addEventListener('click', function(e) { if (e.target === overlay) close(null); });

      btns.appendChild(cancelBtn);
      btns.appendChild(okBtn);
      box.appendChild(label);
      box.appendChild(input);
      box.appendChild(btns);
      overlay.appendChild(box);
      document.body.appendChild(overlay);
      input.focus();
    });
  }
};


/* -----------------------------------------------------------------------
 * Section 4b: WinManager — Multi-window manager
 * ----------------------------------------------------------------------- */

const WinManager = {
  _windows: new Map(),
  _nextId: 1,
  _topZ: 150,
  _cascadeOffset: 0,
  _statusbar: null,
  _statusInfo: null,
  _statusWindows: null,
  _skipNextPop: false,

  init() {
    // Create statusbar
    const sb = document.createElement('div');
    sb.className = 'statusbar';
    sb.innerHTML = '<span class="statusbar-info"></span><span class="statusbar-windows"></span>';
    document.body.appendChild(sb);
    this._statusbar = sb;
    this._statusInfo = sb.querySelector('.statusbar-info');
    this._statusWindows = sb.querySelector('.statusbar-windows');
  },

  open(title, content, opts) {
    opts = opts || {};

    // Singleton: close existing window of same type
    if (opts.singleton && opts.type) {
      var existing = this.getByType(opts.type);
      if (existing) this.close(existing.id, true);  // fromPopstate=true to reuse history slot
    }

    var id = this._nextId++;
    var self = this;

    // Build DOM
    var el = document.createElement('div');
    el.className = 'win' +
      (opts.wide ? ' win-wide' : '') +
      (opts.full ? ' win-full' : '') +
      (opts.noPadding ? ' win-no-padding' : '') +
      (opts.type ? ' win-type-' + opts.type : '');
    el.dataset.winId = id;

    var headerTrigger = document.createElement('div');
    headerTrigger.className = 'win-header-trigger';

    var header = document.createElement('div');
    header.className = 'win-header';

    var titleEl = document.createElement('span');
    titleEl.className = 'win-title';
    titleEl.textContent = title;

    var actionsEl = document.createElement('div');
    actionsEl.className = 'win-header-actions';
    if (opts.headerActions) {
      opts.headerActions.forEach(function(a) { actionsEl.appendChild(a); });
    }

    var minBtn = document.createElement('button');
    minBtn.className = 'win-minimize';
    minBtn.title = 'Minimize';
    minBtn.addEventListener('click', function() { self.minimize(id); });

    var maxBtn = document.createElement('button');
    maxBtn.className = 'win-maximize';
    maxBtn.title = 'Maximize';
    maxBtn.addEventListener('click', function() { self.toggleMaximize(id); });

    var closeBtn = document.createElement('button');
    closeBtn.className = 'win-close';
    closeBtn.title = 'Close';
    closeBtn.addEventListener('click', function() { self.close(id); });

    header.appendChild(titleEl);
    header.appendChild(actionsEl);
    header.appendChild(minBtn);
    header.appendChild(maxBtn);
    header.appendChild(closeBtn);

    var body = document.createElement('div');
    body.className = 'win-body';
    if (typeof content === 'string') {
      body.innerHTML = content;
    } else if (content instanceof HTMLElement) {
      body.appendChild(content);
    }

    // Edge and corner resize handles
    var edges = ['n','s','e','w','ne','nw','se','sw'];
    var resizeHandles = {};
    for (var ei = 0; ei < edges.length; ei++) {
      var h = document.createElement('div');
      h.className = 'win-edge win-edge-' + edges[ei];
      h.dataset.edge = edges[ei];
      resizeHandles[edges[ei]] = h;
    }

    // Visual grip indicator on SE corner
    var resizeGrip = resizeHandles['se'];

    el.appendChild(headerTrigger);
    el.appendChild(header);
    el.appendChild(body);
    for (var ei2 = 0; ei2 < edges.length; ei2++) {
      el.appendChild(resizeHandles[edges[ei2]]);
    }

    // Position: cascade — keep within viewport, above statusbar (28px)
    var cascadeStep = this._cascadeOffset % 10;
    this._cascadeOffset++;
    var offsetX = 60 + cascadeStep * 30;
    var offsetY = 60 + cascadeStep * 30;
    // After appending, measure actual size and clamp
    document.body.appendChild(el);
    var rect = el.getBoundingClientRect();
    var vw = window.innerWidth;
    var vh = window.innerHeight;
    var sbH = 28; // statusbar height
    // Clamp right edge inside viewport
    if (offsetX + rect.width > vw) {
      offsetX = Math.max(0, vw - rect.width);
    }
    // Clamp bottom edge above statusbar
    if (offsetY + rect.height > vh - sbH) {
      offsetY = Math.max(0, vh - sbH - rect.height);
    }
    el.style.left = offsetX + 'px';
    el.style.top = offsetY + 'px';

    // Z-index
    this._topZ++;
    el.style.zIndex = this._topZ;

    // Window state
    var winState = {
      id: id,
      el: el,
      titleEl: titleEl,
      headerEl: header,
      headerTrigger: headerTrigger,
      maxBtn: maxBtn,
      body: body,
      type: opts.type || null,
      maximized: false,
      minimized: false,
      onClose: opts.onClose || null,
      beforeClose: opts.beforeClose || null,
      _savedPos: null,
      _headerHideTimer: null
    };
    this._windows.set(id, winState);

    // Viewer mode: start maximized, hide buttons, close = close tab
    if (this._nextViewerMode) {
      winState.maximized = true;
      el.classList.add('win-maximized');
      minBtn.hidden = true;
      maxBtn.hidden = true;
      header.style.cursor = 'default';
      el.style.resize = 'none';
      var edges = el.querySelectorAll('.win-edge');
      for (var ei = 0; ei < edges.length; ei++) edges[ei].hidden = true;
      // Replace close button entirely to remove old event listeners
      var newCloseBtn = closeBtn.cloneNode(true);
      closeBtn.parentNode.replaceChild(newCloseBtn, closeBtn);
      newCloseBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        if (winState.beforeClose) {
          var ok = winState.beforeClose();
          if (ok && typeof ok.then === 'function') {
            ok.then(function(v) {
              if (v === false) return;
              window.close();
              window.location.href = window.location.pathname;
            });
            return;
          }
          if (ok === false) return;
        }
        window.close();
        window.location.href = window.location.pathname;
      });
    }

    // Bring to front on click
    el.addEventListener('mousedown', function() { self.focus(id); });

    // Drag
    this._setupDrag(winState);

    // Resize from any edge/corner
    this._setupResize(winState);

    // Maximized header hover
    this._setupMaxHeaderHover(winState);

    // Focus this window
    this.focus(id);
    this._syncPasteBar();

    // Push history state so mobile back button closes this window
    if (!this._nextViewerMode) {
      history.pushState({ win: true }, '', location.hash);
    }

    return id;
  },

  async close(id, fromPopstate) {
    var win = this._windows.get(id);
    if (!win) return true;
    if (win.beforeClose) {
      var ok = win.beforeClose();
      if (ok && typeof ok.then === 'function') ok = await ok;
      if (!ok) return false;
    }
    win.beforeClose = null;
    if (win.onClose) { win.onClose(); win.onClose = null; }
    win.el.remove();
    this._windows.delete(id);
    this._removeStatusIcon(id);
    // Focus next topmost window
    this._focusTopmost();
    this._syncPasteBar();
    // Clean up the history entry we pushed on open
    if (!fromPopstate) {
      this._skipNextPop = true;
      history.back();
    }
    return true;
  },

  focus(id) {
    var win = this._windows.get(id);
    if (!win || win.minimized) return;
    // Remove focus from all
    this._windows.forEach(function(w) {
      w.el.classList.remove('win-focused');
    });
    this._topZ++;
    win.el.style.zIndex = this._topZ;
    win.el.classList.add('win-focused');
  },

  minimize(id) {
    var win = this._windows.get(id);
    if (!win) return;
    win.minimized = true;
    win.el.classList.add('win-minimized');
    win.el.classList.remove('win-focused');
    this._addStatusIcon(win);
    this._focusTopmost();
    this._syncPasteBar();
  },

  restore(id) {
    var win = this._windows.get(id);
    if (!win) return;
    win.minimized = false;
    win.el.classList.remove('win-minimized');
    this._removeStatusIcon(id);
    this.focus(id);
    this._syncPasteBar();
  },

  toggleMaximize(id) {
    var win = this._windows.get(id);
    if (!win) return;
    win.maximized = !win.maximized;
    if (win.maximized) {
      win._savedPos = {
        left: win.el.style.left,
        top: win.el.style.top,
        width: win.el.style.width,
        height: win.el.style.height
      };
      win.el.classList.add('win-maximized');
      win.headerEl.classList.remove('header-visible');
    } else {
      win.el.classList.remove('win-maximized');
      if (win._savedPos) {
        win.el.style.left = win._savedPos.left;
        win.el.style.top = win._savedPos.top;
        win.el.style.width = win._savedPos.width;
        win.el.style.height = win._savedPos.height;
      }
      clearTimeout(win._headerHideTimer);
      win.headerEl.classList.remove('header-visible');
    }
    window.dispatchEvent(new Event('resize'));
  },

  setTitle(id, text) {
    var win = this._windows.get(id);
    if (win) win.titleEl.textContent = text;
  },

  getWindow(id) {
    return this._windows.get(id) || null;
  },

  getByType(type) {
    var found = null;
    this._windows.forEach(function(w) {
      if (w.type === type) found = w;
    });
    return found;
  },

  getFocusedWindow() {
    var top = null, topZ = -1;
    this._windows.forEach(function(w) {
      if (w.minimized) return;
      var z = parseInt(w.el.style.zIndex) || 0;
      if (z > topZ) { topZ = z; top = w; }
    });
    return top;
  },

  updateStatusInfo(text) {
    if (this._statusInfo) this._statusInfo.textContent = text;
  },

  _focusTopmost() {
    var top = this.getFocusedWindow();
    if (top) this.focus(top.id);
  },

  // Hide paste bar when any non-minimized window is open (mirrors old modal overlay behavior)
  _syncPasteBar() {
    var bar = document.getElementById('paste-bar');
    if (!bar) return;
    var hasVisible = false;
    this._windows.forEach(function(w) {
      if (!w.minimized) hasVisible = true;
    });
    if (hasVisible) {
      bar.dataset.hiddenByWin = bar.hidden ? '' : '1';
      bar.hidden = true;
    } else {
      if (bar.dataset.hiddenByWin === '1') {
        bar.hidden = false;
      }
      delete bar.dataset.hiddenByWin;
    }
  },

  _setupDrag(win) {
    var dragging = false, dragX, dragY;
    var self = this;
    var header = win.headerEl;

    header.addEventListener('dblclick', function(e) {
      if (e.target.closest('button, input, .win-header-actions')) return;
      self.toggleMaximize(win.id);
    });

    header.addEventListener('mousedown', function(e) {
      if (e.target.closest('button, input, .win-header-actions')) return;
      if (win.maximized) return;
      dragging = true;
      var rect = win.el.getBoundingClientRect();
      dragX = e.clientX - rect.left;
      dragY = e.clientY - rect.top;
      header.style.cursor = 'grabbing';
      win.el.style.transition = 'none';
      e.preventDefault();
    });

    document.addEventListener('mousemove', function(e) {
      if (!dragging) return;
      win.el.style.left = (e.clientX - dragX) + 'px';
      win.el.style.top = (e.clientY - dragY) + 'px';
    });

    document.addEventListener('mouseup', function() {
      if (!dragging) return;
      dragging = false;
      header.style.cursor = '';
      win.el.style.transition = '';
    });
  },

  _setupResize(win) {
    var dragging = false, edge, startX, startY, startW, startH, startL, startT;
    var handles = win.el.querySelectorAll('.win-edge');
    for (var i = 0; i < handles.length; i++) {
      handles[i].addEventListener('mousedown', function(e) {
        if (win.maximized) return;
        e.preventDefault();
        e.stopPropagation();
        dragging = true;
        edge = this.dataset.edge;
        startX = e.clientX;
        startY = e.clientY;
        var rect = win.el.getBoundingClientRect();
        startW = rect.width;
        startH = rect.height;
        startL = rect.left;
        startT = rect.top;
      });
    }
    document.addEventListener('mousemove', function(e) {
      if (!dragging) return;
      var dx = e.clientX - startX;
      var dy = e.clientY - startY;
      var newW = startW, newH = startH, newL = startL, newT = startT;

      if (edge.indexOf('e') !== -1) newW = startW + dx;
      if (edge.indexOf('s') !== -1) newH = startH + dy;
      if (edge.indexOf('w') !== -1) { newW = startW - dx; newL = startL + dx; }
      if (edge.indexOf('n') !== -1 && edge !== 'ne') { newH = startH - dy; newT = startT + dy; }
      if (edge === 'ne') newH = startH - dy, newT = startT + dy;

      if (newW < 320) { if (edge.indexOf('w') !== -1) newL -= 320 - newW; newW = 320; }
      if (newH < 200) { if (edge.indexOf('n') !== -1) newT -= 200 - newH; newH = 200; }

      win.el.style.width = newW + 'px';
      win.el.style.height = newH + 'px';
      if (edge.indexOf('w') !== -1) win.el.style.left = newL + 'px';
      if (edge.indexOf('n') !== -1) win.el.style.top = newT + 'px';
    });
    document.addEventListener('mouseup', function() {
      if (!dragging) return;
      dragging = false;
      window.dispatchEvent(new Event('resize'));
    });
  },

  _setupMaxHeaderHover(win) {
    var self = this;
    win.headerTrigger.addEventListener('mouseenter', function() {
      if (!win.maximized) return;
      clearTimeout(win._headerHideTimer);
      win.headerEl.classList.add('header-visible');
    });
    win.headerEl.addEventListener('mouseenter', function() {
      if (!win.maximized) return;
      clearTimeout(win._headerHideTimer);
    });
    win.headerEl.addEventListener('mouseleave', function() {
      if (!win.maximized) return;
      win._headerHideTimer = setTimeout(function() {
        win.headerEl.classList.remove('header-visible');
      }, 1250);
    });
    win.headerTrigger.addEventListener('mouseleave', function() {
      if (!win.maximized) return;
      win._headerHideTimer = setTimeout(function() {
        win.headerEl.classList.remove('header-visible');
      }, 1250);
    });
  },

  _addStatusIcon(win) {
    var self = this;
    var btn = document.createElement('button');
    btn.className = 'statusbar-win-btn';
    btn.dataset.winId = win.id;
    btn.dataset.type = win.type || '';
    btn.title = win.titleEl.textContent;
    btn.addEventListener('click', function() { self.restore(win.id); });
    this._statusWindows.appendChild(btn);
  },

  _removeStatusIcon(id) {
    var btn = this._statusWindows.querySelector('[data-win-id="' + id + '"]');
    if (btn) btn.remove();
  }
};


/* -----------------------------------------------------------------------
 * Section 5: Editor Registry — track open editors for unsaved-change warnings
 * ----------------------------------------------------------------------- */

const Editors = {
  _checks: new Set(),

  register(dirtyCheckFn) {
    this._checks.add(dirtyCheckFn);
  },

  unregister(dirtyCheckFn) {
    this._checks.delete(dirtyCheckFn);
  },

  hasDirty() {
    for (var fn of this._checks) {
      if (fn()) return true;
    }
    return false;
  }
};

window.addEventListener('beforeunload', function(e) {
  if (Editors.hasDirty()) {
    e.preventDefault();
    e.returnValue = '';
  }
});

/* -----------------------------------------------------------------------
 * Section 5b: Playlist Builder — floating mini-widget for assembling playlists
 * ----------------------------------------------------------------------- */

const PlaylistBuilder = {
  _el: null,
  _listEl: null,
  _titleEl: null,
  _countEl: null,
  tracks: [],   // [{path, name, title, duration}]
  _name: '',
  _onDone: null,

  _isAudio: function(name) {
    return /\.(mp3|wav|ogg|oga|flac|aac|m4a|opus|wma)$/i.test(name);
  },

  _init: function() {
    if (this._el) return;
    var el = document.createElement('div');
    el.className = 'pl-builder';
    el.innerHTML =
      '<div class="pl-builder-header">' +
        '<span class="pl-builder-title"></span>' +
        '<span class="pl-builder-count"></span>' +
        '<div class="pl-builder-header-btns">' +
          '<button class="pl-builder-btn" title="Open in Player" data-act="done">Done</button>' +
          '<button class="pl-builder-btn" title="Save" data-act="save">Save</button>' +
          '<button class="pl-builder-btn pl-builder-close" title="Close" data-act="close">&times;</button>' +
        '</div>' +
      '</div>' +
      '<div class="pl-builder-body">' +
        '<div class="pl-builder-list"></div>' +
        '<div class="pl-builder-drop-overlay">Drop audio files here</div>' +
      '</div>';
    document.body.appendChild(el);
    this._el = el;
    this._listEl = el.querySelector('.pl-builder-list');
    this._titleEl = el.querySelector('.pl-builder-title');
    this._countEl = el.querySelector('.pl-builder-count');

    var self = this;

    // Header button actions
    el.querySelector('[data-act="done"]').addEventListener('click', function() { self.done(); });
    el.querySelector('[data-act="save"]').addEventListener('click', function() { self.save(); });
    el.querySelector('[data-act="close"]').addEventListener('click', function() { self.close(); });

    // Drop zone
    var body = el.querySelector('.pl-builder-body');
    var overlay = el.querySelector('.pl-builder-drop-overlay');
    body.addEventListener('dragover', function(e) {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
      overlay.classList.add('active');
    });
    body.addEventListener('dragleave', function(e) {
      if (!body.contains(e.relatedTarget)) overlay.classList.remove('active');
    });
    body.addEventListener('drop', function(e) {
      e.preventDefault();
      overlay.classList.remove('active');
      var data = e.dataTransfer.getData('application/x-fm-tracks');
      if (data) {
        try {
          var items = JSON.parse(data);
          for (var i = 0; i < items.length; i++) self.addTrack(items[i]);
        } catch(err) {}
      }
    });

    // Draggable header
    var header = el.querySelector('.pl-builder-header');
    var dragging = false, dragX = 0, dragY = 0;
    header.addEventListener('mousedown', function(e) {
      if (e.target.closest('.pl-builder-btn')) return;
      dragging = true;
      dragX = e.clientX - el.offsetLeft;
      dragY = e.clientY - el.offsetTop;
      header.style.cursor = 'grabbing';
      e.preventDefault();
    });
    document.addEventListener('mousemove', function(e) {
      if (!dragging) return;
      el.style.left = (e.clientX - dragX) + 'px';
      el.style.top = (e.clientY - dragY) + 'px';
      el.style.bottom = 'auto';
      el.style.right = 'auto';
    });
    document.addEventListener('mouseup', function() {
      if (!dragging) return;
      dragging = false;
      header.style.cursor = '';
    });

  },

  open: function(name, initialTracks, sourceDir) {
    this._init();
    this._name = name || 'New Playlist';
    this._sourceDir = sourceDir || '';
    this.tracks = (initialTracks || []).slice();
    this._titleEl.textContent = this._name;
    this._el.hidden = false;
    this._renderList();
    // Close any open dialog to let user browse
    Dialog.close();
  },

  close: function() {
    if (this._el) this._el.hidden = true;
    this.tracks = [];
  },

  isOpen: function() {
    return this._el && !this._el.hidden;
  },

  addTrack: function(track) {
    // Avoid duplicates by path
    for (var i = 0; i < this.tracks.length; i++) {
      if (this.tracks[i].path === track.path) return;
    }
    this.tracks.push(track);
    this._renderList();
  },

  _renderList: function() {
    this._countEl.textContent = '(' + this.tracks.length + ')';
    var html = '';
    for (var i = 0; i < this.tracks.length; i++) {
      var t = this.tracks[i];
      var display = (t.title || t.name || t.path.split('/').pop()).replace(/</g, '&lt;');
      html += '<div class="pl-builder-item" data-idx="' + i + '">' +
        '<span class="pl-builder-track-name">' + display + '</span>' +
        '<button class="pl-builder-remove" data-idx="' + i + '">&times;</button>' +
        '</div>';
    }
    if (!html) {
      html = '<div class="pl-builder-empty">Drag and drop your mp3s here</div>';
    }
    this._listEl.innerHTML = html;

    // Wire remove buttons
    var self = this;
    var removeBtns = this._listEl.querySelectorAll('.pl-builder-remove');
    for (var r = 0; r < removeBtns.length; r++) {
      removeBtns[r].addEventListener('click', function(e) {
        e.stopPropagation();
        var idx = parseInt(this.dataset.idx);
        self.tracks.splice(idx, 1);
        self._renderList();
      });
    }

  },

  done: function() {
    if (!this.tracks.length) return;
    // Build items for _openAudio from playlist tracks
    var audioItems = [];
    for (var i = 0; i < this.tracks.length; i++) {
      var t = this.tracks[i];
      audioItems.push({
        name: t.name || t.path.split('/').pop(),
        href: t.path,
        isDir: false,
        _plTitle: t.title || ''
      });
    }
    this._el.hidden = true;
    Viewers._openAudioFromList(audioItems, this._name);
  },

  save: async function(defaultDir) {
    var musicDir = defaultDir || this._sourceDir || App.davUrl + Auth.username + '/Music/';
    var defaultName = this._name.replace(/\.m3u$/i, '') + '.m3u';
    var result = await FilePicker.save('Save Playlist', musicDir, defaultName);
    if (!result) return;
    var filename = result.filename;
    if (!/\.m3u$/i.test(filename)) filename += '.m3u';
    var fullPath = result.dir + encodeURIComponent(filename);

    // Build M3U content
    var content = '#EXTM3U\n';
    for (var i = 0; i < this.tracks.length; i++) {
      var t = this.tracks[i];
      var dur = t.duration ? Math.round(t.duration) : -1;
      var title = t.title || t.name || t.path.split('/').pop();
      content += '#EXTINF:' + dur + ',' + title + '\n' + t.path + '\n';
    }

    try {
      var resp = await fetch(fullPath, {
        method: 'PUT',
        body: content,
        headers: { 'Content-Type': 'audio/x-mpegurl' }
      });
      if (resp.ok) {
        Dialog.alert('Playlist saved to ' + decodeURIComponent(result.dir.replace(App.davUrl, '')) + filename);
      } else {
        Dialog.alert('Failed to save: ' + resp.statusText);
      }
    } catch(e) {
      Dialog.alert('Failed to save: ' + e.message);
    }
  }
};


/* -----------------------------------------------------------------------
 * Section 5c: FilePicker — reusable save/open directory+filename picker
 * ----------------------------------------------------------------------- */

const FilePicker = {
  _overlay: null,
  _resolve: null,
  _currentPath: '',
  _items: [],

  _init: function() {
    if (this._overlay) return;
    var overlay = document.createElement('div');
    overlay.className = 'fp-overlay';
    overlay.innerHTML =
      '<div class="fp-dialog">' +
        '<div class="fp-header">' +
          '<span class="fp-title"></span>' +
          '<button class="fp-close">&times;</button>' +
        '</div>' +
        '<div class="fp-breadcrumb"></div>' +
        '<div class="fp-list"></div>' +
        '<div class="fp-footer">' +
          '<div class="fp-name-row">' +
            '<label>Name:</label>' +
            '<input type="text" class="fp-filename" autocomplete="off">' +
          '</div>' +
          '<div class="fp-buttons">' +
            '<button class="btn fp-cancel">Cancel</button>' +
            '<button class="btn btn-primary fp-ok">Save</button>' +
          '</div>' +
        '</div>' +
      '</div>';
    document.body.appendChild(overlay);
    this._overlay = overlay;

    var self = this;
    overlay.querySelector('.fp-close').addEventListener('click', function() { self._cancel(); });
    overlay.querySelector('.fp-cancel').addEventListener('click', function() { self._cancel(); });
    overlay.querySelector('.fp-ok').addEventListener('click', function() { self._confirm(); });
    overlay.querySelector('.fp-filename').addEventListener('keydown', function(e) {
      if (e.key === 'Enter') self._confirm();
    });
    overlay.addEventListener('mousedown', function(e) {
      if (e.target === overlay) self._cancel();
    });
  },

  // Show save picker. Returns promise of {dir, filename, fullPath} or null
  save: function(title, startDir, defaultFilename) {
    this._init();
    this._overlay.querySelector('.fp-title').textContent = title || 'Save';
    this._overlay.querySelector('.fp-ok').textContent = 'Save';
    this._overlay.querySelector('.fp-filename').value = defaultFilename || '';
    this._overlay.querySelector('.fp-filename').readOnly = false;
    this._overlay.hidden = false;
    this._navigate(startDir || App.davUrl + Auth.username + '/');
    var self = this;
    return new Promise(function(resolve) { self._resolve = resolve; });
  },

  _navigate: function(path) {
    path = path.replace(/\/?$/, '/');
    this._currentPath = path;
    this._renderBreadcrumb();
    var listEl = this._overlay.querySelector('.fp-list');
    listEl.innerHTML = '<div class="fp-loading">Loading...</div>';
    var self = this;
    DavClient.list(path, 1).then(function(items) {
      self._items = items.filter(function(it) { return !it.isSelf && it.isDir; });
      self._items.sort(function(a, b) {
        return a.name.toLowerCase().localeCompare(b.name.toLowerCase());
      });
      self._renderList();
    }).catch(function() {
      listEl.innerHTML = '<div class="fp-loading">Failed to load directory</div>';
    });
  },

  _renderBreadcrumb: function() {
    var el = this._overlay.querySelector('.fp-breadcrumb');
    el.innerHTML = '';
    var davUrl = App.davUrl;
    var rel = this._currentPath.replace(davUrl, '');
    var segments = rel.split('/').filter(Boolean);
    var self = this;

    var homeLink = document.createElement('a');
    homeLink.textContent = 'Root';
    homeLink.addEventListener('click', function() { self._navigate(davUrl); });
    el.appendChild(homeLink);

    var accumulated = davUrl;
    for (var i = 0; i < segments.length; i++) {
      var sep = document.createElement('span');
      sep.textContent = ' / ';
      el.appendChild(sep);
      accumulated += segments[i] + '/';
      var link = document.createElement('a');
      link.textContent = decodeURIComponent(segments[i]);
      (function(target) {
        link.addEventListener('click', function() { self._navigate(target); });
      })(accumulated);
      el.appendChild(link);
    }
  },

  _renderList: function() {
    var listEl = this._overlay.querySelector('.fp-list');
    listEl.innerHTML = '';
    var self = this;

    // Parent
    var parentUrl = this._getParent();
    if (parentUrl) {
      var parentItem = document.createElement('div');
      parentItem.className = 'fp-item';
      parentItem.innerHTML = '<span class="fp-item-icon">\u{1F4C1}</span><span class="fp-item-name">..</span>';
      parentItem.addEventListener('click', function() { self._navigate(parentUrl); });
      listEl.appendChild(parentItem);
    }

    for (var i = 0; i < this._items.length; i++) {
      (function(item) {
        var row = document.createElement('div');
        row.className = 'fp-item';
        row.innerHTML = '<span class="fp-item-icon">\u{1F4C1}</span><span class="fp-item-name">' +
          item.name.replace(/</g, '&lt;') + '</span>';
        row.addEventListener('click', function() { self._navigate(item.href); });
        listEl.appendChild(row);
      })(this._items[i]);
    }

    if (this._items.length === 0 && !parentUrl) {
      listEl.innerHTML = '<div class="fp-loading">Empty folder</div>';
    }
  },

  _getParent: function() {
    var davUrl = App.davUrl;
    if (this._currentPath === davUrl || this._currentPath.length <= davUrl.length) return null;
    var parts = this._currentPath.replace(/\/$/, '').split('/');
    parts.pop();
    return parts.join('/') + '/';
  },

  _confirm: function() {
    var filename = this._overlay.querySelector('.fp-filename').value.trim();
    if (!filename) return;
    this._overlay.hidden = true;
    if (this._resolve) {
      this._resolve({
        dir: this._currentPath,
        filename: filename,
        fullPath: this._currentPath + encodeURIComponent(filename)
      });
      this._resolve = null;
    }
  },

  _cancel: function() {
    this._overlay.hidden = true;
    if (this._resolve) { this._resolve(null); this._resolve = null; }
  }
};


/* -----------------------------------------------------------------------
 * Section 6: Clipboard — Copy/cut/paste
 * ----------------------------------------------------------------------- */

const Clipboard = {
  items: [],
  action: null, // 'copy' or 'cut'
  sourceDir: null,

  set(items, action, sourceDir) {
    this.items = items.slice();
    this.action = action;
    this.sourceDir = sourceDir;
    this._updatePasteBar();
  },

  clear() {
    this.items = [];
    this.action = null;
    this.sourceDir = null;
    this._updatePasteBar();
  },

  hasItems() {
    return this.items.length > 0;
  },

  async paste(targetUrl) {
    if (!this.hasItems()) return;
    const method = this.action === 'cut' ? 'MOVE' : 'COPY';
    let errors = 0;
    const verb = method === 'MOVE' ? 'Moving' : 'Copying';
    const total = this.items.length;
    var progressToast = Toast.show(verb + ' ' + total + ' item(s)...', 'success', 0);
    await new Promise(function(r) { setTimeout(r, 50); });

    const errorMsgs = [];
    const overwriteState = {value: null}; // null=ask, true=yesAll, false=noAll
    let copied = 0;
    for (let i = 0; i < this.items.length; i++) {
      const item = this.items[i];
      const destUrl = targetUrl + encodeURIComponent(item.name) + (item.isDir ? '/' : '');
      if (item.isDir) {
        // Directory: enumerate and copy/move one file at a time
        copied += await copyMoveTree(method, item.href, destUrl, overwriteState, errorMsgs);
        continue;
      }
      try {
        let resp = await DavClient.copyMove(method, item.href, destUrl, false);
        if (resp.status === 412) {
          // File exists — ask user
          let action;
          if (overwriteState.value === 'cancel') break;
          else if (overwriteState.value === true) action = 'yes';
          else if (overwriteState.value === false) action = 'no';
          else if (overwriteState.value === 'rename') action = 'rename';
          else {
            const remaining = this.items.length - i - 1;
            action = await confirmOverwrite(item.name, remaining);
            if (action === 'yesAll') { overwriteState.value = true; action = 'yes'; }
            else if (action === 'noAll') { overwriteState.value = false; action = 'no'; }
            else if (action === 'renameAll') { overwriteState.value = 'rename'; action = 'rename'; }
            else if (action === 'cancel') { overwriteState.value = 'cancel'; break; }
          }
          if (action === 'no') continue;
          if (action === 'rename') {
            var rnName = await autoRename(targetUrl, item.name);
            destUrl = targetUrl + encodeURIComponent(rnName) + (item.isDir ? '/' : '');
          }
          resp = await DavClient.copyMove(method, item.href, destUrl, true);
        }
        if (!isDavOk(resp)) {
          const txt = await resp.text().catch(function() { return ''; });
          errorMsgs.push(item.name + ': ' + (txt || 'Error ' + resp.status));
        } else {
          copied++;
        }
      } catch (e) {
        errorMsgs.push(item.name + ': ' + (e.message || 'Failed'));
      }
    }

    clearTimeout(pasteTimer);
    if (progressToast) progressToast.remove();
    if (this.action === 'cut') this.clear();
    if (errorMsgs.length) Toast.error(errorMsgs.join('\n'));
    if (copied > 0) Toast.success((method === 'MOVE' ? 'Moved' : 'Copied') + ' ' + copied + ' item(s)');

    FileList.reload();
    Tree.refresh(targetUrl);
  },

  _freeName(name) {
    const dot = name.lastIndexOf('.');
    if (dot > 0) return name.substring(0, dot) + ' (copy)' + name.substring(dot);
    return name + ' (copy)';
  },

  _updatePasteBar() {
    const bar = document.getElementById('paste-bar');
    const info = document.getElementById('paste-info');
    if (this.hasItems()) {
      bar.hidden = false;
      info.textContent = (this.action === 'cut' ? 'Moving ' : 'Copying ') +
        this.items.length + ' item(s)';
    } else {
      bar.hidden = true;
    }
  }
};


/* -----------------------------------------------------------------------
 * Section 5b: DragDrop — Internal file drag-and-drop (move/copy)
 * ----------------------------------------------------------------------- */

const DragDrop = {
  _dragItems: null,

  // Determine the zone of a DAV path:
  //   'mount:<name>' for rclone mounts inside user home
  //   'home' for user home directory (non-mount)
  //   'external' for anything outside user home
  _getZone(href) {
    var home = Auth.getUserHomeUrl(); // e.g. /dav/username/
    if (href.indexOf(home) !== 0) return 'external';
    // Check if path is inside an rclone mount: /dav/username/mountName/...
    var rel = href.substring(home.length); // e.g. 'mountName/sub/...'
    var firstSeg = rel.split('/')[0];
    if (firstSeg && Auth.mountNames && Auth.mountNames.indexOf(firstSeg) !== -1) {
      return 'mount:' + firstSeg;
    }
    return 'home';
  },

  _isCrossZone(items, targetHref) {
    var targetZone = this._getZone(targetHref);
    for (var i = 0; i < items.length; i++) {
      if (this._getZone(items[i].href) !== targetZone) return true;
    }
    return false;
  },

  // Make an element a drag source for a file item
  makeSource(el, item) {
    el.draggable = true;
    el.addEventListener('dragstart', (e) => {
      // If item is in selection, drag all selected; otherwise just this item
      if (FileList.selected.has(item.href) && FileList.selected.size > 1) {
        this._dragItems = FileList.getSelected();
      } else {
        this._dragItems = [item];
      }
      e.dataTransfer.effectAllowed = 'all';
      e.dataTransfer.setData('text/plain', item.name);
      e.dataTransfer.setData('text/x-dav-drag', 'internal');
      // Include HTML for editor drops: images get <img>, others get <a>
      var dropHtml = this._dragItems.filter(function(it) { return !it.isDir; }).map(function(it) {
        var esc = it.name.replace(/"/g, '&quot;');
        if (/\.(png|jpe?g|gif|svg|webp|bmp|ico)$/i.test(it.name)) {
          return '<img src="' + it.href + '" alt="' + esc + '">';
        }
        return '<a href="' + it.href + '">' + esc + '</a>';
      }).join('\n');
      if (dropHtml) e.dataTransfer.setData('text/html', dropHtml);
      // Include audio track data for playlist builder
      var audioTracks = this._dragItems.filter(function(it) {
        return /\.(mp3|wav|ogg|oga|flac|aac|m4a|opus|wma)$/i.test(it.name);
      }).map(function(it) {
        return { path: it.href, name: it.name, title: it.name, duration: 0 };
      });
      if (audioTracks.length) {
        e.dataTransfer.setData('application/x-fm-tracks', JSON.stringify(audioTracks));
      }
      // Custom drag ghost showing file name / count
      if (!DragDrop._dragGhost) {
        var ghost = document.createElement('div');
        ghost.className = 'drag-ghost';
        ghost.style.cssText = 'position:fixed;top:-1000px;left:-1000px;padding:4px 10px;' +
          'border-radius:4px;font:13px/1.4 -apple-system,sans-serif;white-space:nowrap;' +
          'pointer-events:none;z-index:-1;max-width:260px;overflow:hidden;text-overflow:ellipsis';
        document.body.appendChild(ghost);
        DragDrop._dragGhost = ghost;
      }
      var count = this._dragItems.length;
      DragDrop._dragGhost.textContent = count > 1 ? count + ' items' : item.name;
      DragDrop._dragGhost.style.background = 'var(--color-accent)';
      DragDrop._dragGhost.style.color = 'var(--color-accent-fg)';
      e.dataTransfer.setDragImage(DragDrop._dragGhost, 10, 10);
      el.classList.add('dragging');
    });
    el.addEventListener('dragend', () => {
      el.classList.remove('dragging');
      this._dragItems = null;
      document.querySelectorAll('.drop-target').forEach(t => {
        t.classList.remove('drop-target');
        t.classList.remove('drop-target-copy');
      });
    });
  },

  // Determine default operation for drag items onto a target
  _defaultOp(items, dirHref, shiftKey) {
    var cross = this._isCrossZone(items, dirHref);
    // Cross-zone: default copy, shift to move
    // Same-zone: default move, shift to copy
    if (cross) {
      return shiftKey ? 'MOVE' : 'COPY';
    }
    return shiftKey ? 'COPY' : 'MOVE';
  },

  // Make an element a drop target for a directory href
  makeTarget(el, dirHref) {
    el.addEventListener('dragover', (e) => {
      if (!this._dragItems) return;
      e.preventDefault();
      if (e.ctrlKey && e.shiftKey) {
        e.dataTransfer.dropEffect = 'link';
      } else {
        var op = this._defaultOp(this._dragItems, dirHref, e.shiftKey);
        e.dataTransfer.dropEffect = op === 'COPY' ? 'copy' : 'move';
      }
      el.classList.add('drop-target');
      el.classList.toggle('drop-target-copy', e.dataTransfer.dropEffect === 'copy');
    });
    el.addEventListener('dragleave', () => {
      el.classList.remove('drop-target');
      el.classList.remove('drop-target-copy');
    });
    el.addEventListener('drop', async (e) => {
      e.preventDefault();
      el.classList.remove('drop-target');
      el.classList.remove('drop-target-copy');
      if (!this._dragItems || !e.dataTransfer.types.includes('text/x-dav-drag')) return;
      const items = this._dragItems;
      this._dragItems = null;
      const isSymlink = e.ctrlKey && e.shiftKey;
      const method = isSymlink ? 'LINK' : this._defaultOp(items, dirHref, e.shiftKey);
      const targetUrl = dirHref.replace(/\/?$/, '/');

      const verb = isSymlink ? 'Linking' : (method === 'MOVE' ? 'Moving' : 'Copying');
      var ddProgressToast = Toast.show(verb + ' ' + items.length + ' item(s)...', 'success', 0);
      await new Promise(function(r) { setTimeout(r, 50); });

      const errorMsgs = [];
      const overwriteState = {value: null};
      let completed = 0;
      for (let di = 0; di < items.length; di++) {
        const item = items[di];
        // Don't drop onto self
        if (item.href === targetUrl || item.href === targetUrl + item.name + '/') continue;
        if (ddProgressToast) {
          var ddPtEl = ddProgressToast.querySelector('.toast-message') || ddProgressToast;
          ddPtEl.textContent = verb + ' ' + (di + 1) + '/' + items.length + ': ' + item.name;
        }

        if (isSymlink) {
          const linkPath = targetUrl + encodeURIComponent(item.name);
          try {
            const data = await DavClient.createSymlink(item.href, linkPath);
            if (!data.ok) errorMsgs.push(item.name + ': ' + (data.error || 'Failed'));
            else completed++;
          } catch (e2) { errorMsgs.push(item.name + ': ' + (e2.message || 'Failed')); }
        } else if (item.isDir) {
          // Directory: enumerate and copy/move one file at a time
          const destUrl = targetUrl + encodeURIComponent(item.name) + '/';
          completed += await copyMoveTree(method, item.href, destUrl, overwriteState, errorMsgs);
        } else {
          const destUrl = targetUrl + encodeURIComponent(item.name);
          try {
            let resp = await DavClient.copyMove(method, item.href, destUrl, false);
            if (resp.status === 412) {
              // File exists — ask user
              let action2;
              if (overwriteState.value === 'cancel') break;
              else if (overwriteState.value === true) action2 = 'yes';
              else if (overwriteState.value === false) action2 = 'no';
              else if (overwriteState.value === 'rename') action2 = 'rename';
              else {
                const remaining = items.length - di - 1;
                action2 = await confirmOverwrite(item.name, remaining);
                if (action2 === 'yesAll') { overwriteState.value = true; action2 = 'yes'; }
                else if (action2 === 'noAll') { overwriteState.value = false; action2 = 'no'; }
                else if (action2 === 'renameAll') { overwriteState.value = 'rename'; action2 = 'rename'; }
                else if (action2 === 'cancel') { overwriteState.value = 'cancel'; break; }
              }
              if (action2 === 'no') continue;
              if (action2 === 'rename') {
                var newName = await autoRename(targetUrl, item.name);
                resp = await DavClient.copyMove(method, item.href, targetUrl + encodeURIComponent(newName), false);
              } else {
                resp = await DavClient.copyMove(method, item.href, destUrl, true);
              }
            }
            if (!isDavOk(resp)) {
              const txt = await resp.text().catch(function() { return ''; });
              errorMsgs.push(item.name + ': ' + (txt || 'Error ' + resp.status));
            } else {
              completed++;
            }
          } catch (e2) { errorMsgs.push(item.name + ': ' + (e2.message || 'Failed')); }
        }
      }

      if (ddProgressToast) ddProgressToast.remove();
      const action = isSymlink ? 'Linked' : (method === 'MOVE' ? 'Moved' : 'Copied');
      if (errorMsgs.length) Toast.error(errorMsgs.join('\n'));
      if (completed > 0) Toast.success(action + ' ' + completed + ' item(s)');
      FileList.reload();
      Tree.refresh(FileList.currentPath);
      Tree.refresh(targetUrl);
    });
  }
};


/* -----------------------------------------------------------------------
 * Section 5b2: Autocomplete — reusable dropdown suggestions
 * ----------------------------------------------------------------------- */

const Autocomplete = {
  // Attach autocomplete to an input element
  // options:
  //   fetchSuggestions(query, callback) — async, calls callback([{label, value, ...}])
  //   onSelect(item, input) — called when a suggestion is selected
  //   minLength — minimum chars before triggering (default 2)
  //   debounceMs — debounce delay (default 200)
  //   shouldActivate(value) — optional, return false to skip autocomplete for this input value
  attach(input, options) {
    var opts = options || {};
    var minLen = opts.minLength || 2;
    var debounceMs = opts.debounceMs || 200;
    var timer = null;
    var activeIdx = -1;
    var items = [];

    // Create dropdown — appended to body with fixed positioning to avoid overflow clipping
    var dropdown = document.createElement('div');
    dropdown.className = 'ac-dropdown';
    dropdown.hidden = true;
    document.body.appendChild(dropdown);

    function positionDropdown() {
      var rect = input.getBoundingClientRect();
      dropdown.style.left = rect.left + 'px';
      dropdown.style.top = (rect.bottom + 2) + 'px';
      dropdown.style.width = rect.width + 'px';
    }

    function show(suggestions) {
      items = suggestions;
      activeIdx = -1;
      if (!items.length) { hide(); return; }
      dropdown.innerHTML = '';
      for (var i = 0; i < items.length; i++) {
        var row = document.createElement('div');
        row.className = 'ac-item';
        row.textContent = items[i].label || items[i].path || items[i].value;
        row.dataset.idx = i;
        row.addEventListener('mousedown', function(e) {
          e.preventDefault(); // prevent input blur
          var idx = parseInt(this.dataset.idx);
          select(idx);
        });
        dropdown.appendChild(row);
      }
      positionDropdown();
      dropdown.hidden = false;
    }

    function hide() {
      dropdown.hidden = true;
      dropdown.innerHTML = '';
      items = [];
      activeIdx = -1;
    }

    function highlight(idx) {
      var rows = dropdown.querySelectorAll('.ac-item');
      for (var i = 0; i < rows.length; i++) {
        rows[i].classList.toggle('ac-active', i === idx);
      }
      activeIdx = idx;
      if (rows[idx]) rows[idx].scrollIntoView({block: 'nearest'});
    }

    function select(idx) {
      if (idx >= 0 && idx < items.length) {
        if (opts.onSelect) opts.onSelect(items[idx], input);
        hide();
      }
    }

    input.addEventListener('input', function() {
      clearTimeout(timer);
      var val = input.value;
      if (opts.shouldActivate && !opts.shouldActivate(val)) { hide(); return; }
      if (val.length < minLen) { hide(); return; }
      timer = setTimeout(function() {
        opts.fetchSuggestions(val, function(suggestions) {
          show(suggestions);
        });
      }, debounceMs);
    });

    input.addEventListener('keydown', function(e) {
      if (dropdown.hidden) return;
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        highlight(activeIdx < items.length - 1 ? activeIdx + 1 : -1);
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        highlight(activeIdx > 0 ? activeIdx - 1 : activeIdx === 0 ? -1 : items.length - 1);
      } else if (e.key === 'Enter' && activeIdx >= 0) {
        e.preventDefault();
        e.stopPropagation();
        if (opts.onEnterHandled) opts.onEnterHandled();
        select(activeIdx);
      } else if (e.key === 'Escape') {
        hide();
      }
    });

    input.addEventListener('blur', function() {
      // Small delay so click on dropdown item registers
      setTimeout(hide, 150);
    });

    return { hide: hide, destroy: function() { dropdown.remove(); } };
  }
};


/* -----------------------------------------------------------------------
 * Section 5c: Search — Full-text document search
 * ----------------------------------------------------------------------- */

const Search = {
  _winId: null,

  // Check if the current path is inside an indexed directory
  getSearchDir(currentPath) {
    if (!Auth.searchDirs || !Auth.searchDirs.length) return null;
    var davPrefix = App.davUrl.replace(/\/$/, '');
    for (var i = 0; i < Auth.searchDirs.length; i++) {
      var dir = Auth.searchDirs[i];
      var fullDir = davPrefix + dir;
      if (currentPath.indexOf(fullDir) === 0) return dir;
    }
    return null;
  },

  // Search button is always visible
  updateButton() {
    var btn = document.getElementById('search-btn');
    btn.hidden = false;
  },

  // Check if current path is in an indexed directory (for toggle)
  isInIndexedDir() {
    return !!this.getSearchDir(FileList.currentPath);
  },

  _filenameMode: false,

  _buildTitle(query) {
    var prefix = this._filenameMode ? 'Files: ' : 'Search: ';
    var scope = this._searchThisDir ? ' in ' + this._lastSubPath : ' in all folders';
    return prefix + query + scope;
  },

  _updatePlaceholder() {
    var input = document.getElementById('search-input');
    if (this._filenameMode) {
      input.placeholder = this._searchThisDir ? 'Search filenames in this folder' : 'Search all filenames';
    } else {
      input.placeholder = this._searchThisDir ? 'Search documents in this folder' : 'Search all documents';
    }
  },

  _isNarrow() {
    return window.matchMedia('(max-width: 480px)').matches;
  },

  // Show the search input, hide the breadcrumb and toolbar actions
  showInput() {
    document.getElementById('breadcrumb').hidden = true;
    document.querySelector('.toolbar-actions').hidden = true;
    if (this._isNarrow()) {
      document.getElementById('nav-back').hidden = true;
      document.getElementById('nav-forward').hidden = true;
      document.getElementById('nav-refresh').hidden = true;
      document.getElementById('search-btn').hidden = true;
    }
    var bar = document.getElementById('search-bar');
    bar.hidden = false;
    var input = document.getElementById('search-input');
    input.value = '';
    this._updatePlaceholder();
    if (this._updateModeBtn) this._updateModeBtn();
    input.focus();
  },

  // Hide search input, restore breadcrumb and toolbar actions
  hideInput() {
    document.getElementById('search-bar').hidden = true;
    document.getElementById('breadcrumb').hidden = false;
    document.querySelector('.toolbar-actions').hidden = false;
    if (this._isNarrow()) {
      document.getElementById('nav-back').hidden = false;
      document.getElementById('nav-forward').hidden = false;
      document.getElementById('nav-refresh').hidden = false;
      document.getElementById('search-btn').hidden = false;
    }
  },

  // Get the current dav-relative path (without /dav prefix)
  getCurrentDavPath() {
    var davPrefix = App.davUrl.replace(/\/$/, '');
    var path = FileList.currentPath;
    if (path.indexOf(davPrefix) === 0) path = path.substring(davPrefix.length);
    return path.replace(/\/$/, '');
  },

  _searchThisDir: true, // default: search current directory only

  // Execute search and show results
  async doSearch(query) {
    if (!query.trim()) return;
    var isFilenameMode = this._filenameMode;
    this.hideInput();
    var subPath = this._searchThisDir ? this.getCurrentDavPath() : null;
    if (isFilenameMode) {
      this._fetchAndShowFiles(query, subPath, 0);
    } else {
      this._fetchAndShow(query, subPath, 0);
    }
  },

  _pageSize: 10,
  _lastSubPath: null,

  // Display search results in a WinManager window
  showResults(query, results, total, skip) {
    var self = this;
    skip = skip || 0;
    this._lastSubPath = this.getCurrentDavPath();

    var wrap = document.createElement('div');
    wrap.className = 'search-results';

    // Search bar at top
    var searchForm = document.createElement('div');
    searchForm.className = 'search-results-bar';
    var searchInput = document.createElement('input');
    searchInput.type = 'text';
    searchInput.value = query;
    searchInput.placeholder = 'Search documents...';
    searchInput.className = 'search-results-input';
    searchForm.appendChild(searchInput);

    // Scope toggle in results window
    var winScopeBtn = document.createElement('button');
    winScopeBtn.className = 'toolbar-btn search-scope-btn' + (self._searchThisDir ? '' : ' scope-all');
    winScopeBtn.title = self._searchThisDir ? 'Search this directory' : 'Search all documents';
    winScopeBtn.addEventListener('click', function() {
      self._searchThisDir = !self._searchThisDir;
      winScopeBtn.classList.toggle('scope-all', !self._searchThisDir);
      winScopeBtn.title = self._searchThisDir ? 'Search this directory' : 'Search all documents';
      if (self._updateScopeBtn) self._updateScopeBtn();
    });
    searchForm.appendChild(winScopeBtn);

    var winModeBtn = document.createElement('button');
    winModeBtn.className = 'search-mode-btn';
    winModeBtn.textContent = self._filenameMode ? 'File' : 'Doc';
    winModeBtn.title = self._filenameMode ? 'Search filenames' : 'Search document contents';
    winModeBtn.addEventListener('click', function() {
      self._filenameMode = !self._filenameMode;
      winModeBtn.textContent = self._filenameMode ? 'File' : 'Doc';
      winModeBtn.title = self._filenameMode ? 'Search filenames' : 'Search document contents';
      if (self._updateModeBtn) self._updateModeBtn();
    });
    searchForm.appendChild(winModeBtn);

    var searchSubmit = document.createElement('button');
    searchSubmit.textContent = 'Search';
    searchSubmit.className = 'btn btn-sm';
    searchForm.appendChild(searchSubmit);
    wrap.appendChild(searchForm);

    var doNewSearch = function() {
      var q = searchInput.value.trim();
      if (!q) return;
      var subPath = self._searchThisDir ? self._lastSubPath : null;
      if (self._filenameMode) {
        self._fetchAndShowFiles(q, subPath, 0);
      } else {
        self._fetchAndShow(q, subPath, 0);
      }
    };
    searchSubmit.addEventListener('click', doNewSearch);
    self._acHandledEnter = false;
    searchInput.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        setTimeout(function() {
          if (self._acHandledEnter) { self._acHandledEnter = false; return; }
          doNewSearch();
        }, 0);
      }
    });
    self._attachAutocompletes(searchInput);

    // Summary
    var summary = document.createElement('div');
    summary.className = 'search-results-summary';
    if (results.length) {
      var totalStr = total === -1 ? 'many' : String(total);
      summary.textContent = 'Results ' + (skip + 1) + '-' + (skip + results.length) + ' of ' + totalStr + ' for "' + query + '"';
    } else {
      summary.textContent = 'No results for "' + query + '"';
    }
    wrap.appendChild(summary);

    // Results list
    var list = document.createElement('div');
    list.className = 'search-results-list';
    for (var i = 0; i < results.length; i++) {
      var r = results[i];
      var item = document.createElement('div');
      item.className = 'search-result-item';

      var titleRow = document.createElement('div');
      titleRow.className = 'search-result-title';

      var openLink = document.createElement('a');
      openLink.href = '#';
      openLink.textContent = r.title || r.path;
      openLink.className = 'search-result-open';
      (function(res) {
        openLink.addEventListener('click', function(e) {
          e.preventDefault();
          Viewers.open({
            name: res.path.substring(res.path.lastIndexOf('/') + 1),
            href: res.href,
            isDir: false
          });
        });
      })(r);
      titleRow.appendChild(openLink);

      var showLink = document.createElement('a');
      showLink.href = '#';
      showLink.textContent = 'Show in folder';
      showLink.className = 'search-result-show';
      (function(res) {
        showLink.addEventListener('click', function(e) {
          e.preventDefault();
          var parentPath = res.href.substring(0, res.href.lastIndexOf('/') + 1);
          var fileName = decodeURIComponent(res.href.substring(res.href.lastIndexOf('/') + 1));
          FileList.navigate(parentPath);
          setTimeout(function() {
            FileList.selectByName(fileName);
          }, 500);
        });
      })(r);
      titleRow.appendChild(showLink);

      item.appendChild(titleRow);

      var pathEl = document.createElement('div');
      pathEl.className = 'search-result-path';
      pathEl.textContent = r.path;
      item.appendChild(pathEl);

      if (r.snippet) {
        var snippetEl = document.createElement('div');
        snippetEl.className = 'search-result-snippet';
        snippetEl.innerHTML = r.snippet;
        item.appendChild(snippetEl);
      }

      list.appendChild(item);
    }
    wrap.appendChild(list);

    // Pagination
    var pag = document.createElement('div');
    pag.className = 'search-pagination';
    var savedSubPath = self._searchThisDir ? self._lastSubPath : null;
    if (skip > 0) {
      var prevBtn = document.createElement('a');
      prevBtn.href = '#';
      prevBtn.textContent = 'Prev';
      prevBtn.className = 'search-pag-btn';
      prevBtn.addEventListener('click', function(e) {
        e.preventDefault();
        self._fetchAndShow(query, savedSubPath, skip - self._pageSize);
      });
      pag.appendChild(prevBtn);
    }
    // Show Next only if we got a full page (meaning there might be more)
    if (total === -1 || skip + results.length < total) {
      var nextBtn = document.createElement('a');
      nextBtn.href = '#';
      nextBtn.textContent = 'Next';
      nextBtn.className = 'search-pag-btn';
      nextBtn.addEventListener('click', function(e) {
        e.preventDefault();
        self._fetchAndShow(query, savedSubPath, skip + self._pageSize);
      });
      pag.appendChild(nextBtn);
    }
    if (pag.children.length) wrap.appendChild(pag);

    // Open or reuse window
    if (this._winId && WinManager.getWindow(this._winId)) {
      var win = WinManager.getWindow(this._winId);
      var body = win.el.querySelector('.win-body');
      body.innerHTML = '';
      body.appendChild(wrap);
      WinManager.setTitle(this._winId, this._buildTitle(query));
      searchInput.focus();
    } else {
      this._winId = WinManager.open(this._buildTitle(query), wrap, {});
      var winEl = WinManager.getWindow(this._winId).el;
      winEl.style.width = Math.min(500, window.innerWidth) + 'px';
      winEl.style.height = Math.min(600, window.innerHeight * 0.7) + 'px';
      setTimeout(function() { searchInput.focus(); }, 100);
    }
  },

  // Fetch results and display them
  async _fetchAndShow(query, subPath, skip) {
    try {
      var resp = await fetch(App.davUrl + '_search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ query: query, subPath: subPath, maxRows: this._pageSize, skipRows: skip })
      });
      var data = await resp.json();
      if (!data.ok) {
        Toast.error('Search failed: ' + (data.error || 'Unknown error'));
        return;
      }
      this.showResults(data.query, data.results, data.total, skip);
    } catch(e) {
      Toast.error('Search failed: ' + e.message);
    }
  },

  // --- Filename search ---

  async _fetchAndShowFiles(query, subPath, skip) {
    try {
      var resp = await fetch(App.davUrl + '_search/files', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ query: query, subPath: subPath, maxRows: this._pageSize, skipRows: skip })
      });
      var data = await resp.json();
      if (!data.ok) {
        Toast.error('Search failed: ' + (data.error || 'Unknown error'));
        return;
      }
      this.showFileResults(data.query, data.results, data.total, skip);
    } catch(e) {
      Toast.error('Search failed: ' + e.message);
    }
  },

  showFileResults(query, results, total, skip) {
    var self = this;
    skip = skip || 0;
    this._lastSubPath = this.getCurrentDavPath();

    var wrap = document.createElement('div');
    wrap.className = 'search-results';

    // Search bar
    var searchForm = document.createElement('div');
    searchForm.className = 'search-results-bar';
    var searchInput = document.createElement('input');
    searchInput.type = 'text';
    searchInput.value = query;
    searchInput.placeholder = 'Filename search...';
    searchInput.className = 'search-results-input';
    searchForm.appendChild(searchInput);

    var winScopeBtn = document.createElement('button');
    winScopeBtn.className = 'toolbar-btn search-scope-btn' + (self._searchThisDir ? '' : ' scope-all');
    winScopeBtn.title = self._searchThisDir ? 'Search this directory' : 'Search all files';
    winScopeBtn.addEventListener('click', function() {
      self._searchThisDir = !self._searchThisDir;
      winScopeBtn.classList.toggle('scope-all', !self._searchThisDir);
      winScopeBtn.title = self._searchThisDir ? 'Search this directory' : 'Search all files';
      if (self._updateScopeBtn) self._updateScopeBtn();
    });
    searchForm.appendChild(winScopeBtn);

    var winModeBtn = document.createElement('button');
    winModeBtn.className = 'search-mode-btn';
    winModeBtn.textContent = self._filenameMode ? 'File' : 'Doc';
    winModeBtn.title = self._filenameMode ? 'Search filenames' : 'Search document contents';
    winModeBtn.addEventListener('click', function() {
      self._filenameMode = !self._filenameMode;
      winModeBtn.textContent = self._filenameMode ? 'File' : 'Doc';
      winModeBtn.title = self._filenameMode ? 'Search filenames' : 'Search document contents';
      if (self._updateModeBtn) self._updateModeBtn();
    });
    searchForm.appendChild(winModeBtn);

    var searchSubmit = document.createElement('button');
    searchSubmit.textContent = 'Search';
    searchSubmit.className = 'btn btn-sm';
    searchForm.appendChild(searchSubmit);
    wrap.appendChild(searchForm);

    var doNewSearch = function() {
      var q = searchInput.value.trim();
      if (!q) return;
      var subPath = self._searchThisDir ? self._lastSubPath : null;
      if (self._filenameMode) {
        self._fetchAndShowFiles(q, subPath, 0);
      } else {
        self._fetchAndShow(q, subPath, 0);
      }
    };
    searchSubmit.addEventListener('click', doNewSearch);
    self._acHandledEnter = false;
    searchInput.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        setTimeout(function() {
          if (self._acHandledEnter) { self._acHandledEnter = false; return; }
          doNewSearch();
        }, 0);
      }
    });
    self._attachAutocompletes(searchInput);

    // Summary
    var summary = document.createElement('div');
    summary.className = 'search-results-summary';
    if (results.length) {
      var totalStr = total === -1 ? 'many' : String(total);
      summary.textContent = 'Results ' + (skip + 1) + '-' + (skip + results.length) + ' of ' + totalStr + ' for "' + query + '"';
    } else {
      summary.textContent = 'No results for "' + query + '"';
    }
    wrap.appendChild(summary);

    // Results list
    var list = document.createElement('div');
    list.className = 'search-results-list';
    for (var i = 0; i < results.length; i++) {
      var r = results[i];
      var item = document.createElement('div');
      item.className = 'search-result-item';

      var titleRow = document.createElement('div');
      titleRow.className = 'search-result-title';

      var fileName = r.path.substring(r.path.lastIndexOf('/') + 1);
      var openLink = document.createElement('a');
      openLink.href = '#';
      openLink.textContent = fileName;
      openLink.className = 'search-result-open';
      (function(res, isDir) {
        openLink.addEventListener('click', function(e) {
          e.preventDefault();
          if (isDir) {
            App.navigateTo(App.davUrl.replace(/\/$/, '') + res.path + '/');
          } else {
            Viewers.open({name: res.path.substring(res.path.lastIndexOf('/') + 1), href: res.href, isDir: false});
          }
        });
      })(r, r.isDir);
      titleRow.appendChild(openLink);

      if (r.isDir) {
        var dirBadge = document.createElement('span');
        dirBadge.textContent = 'folder';
        dirBadge.style.cssText = 'font-size:11px;color:var(--color-fg-muted);margin-left:8px';
        titleRow.appendChild(dirBadge);
      }

      var showLink = document.createElement('a');
      showLink.href = '#';
      showLink.textContent = 'Show in folder';
      showLink.className = 'search-result-show';
      (function(res) {
        showLink.addEventListener('click', function(e) {
          e.preventDefault();
          var parentPath = res.href.substring(0, res.href.lastIndexOf('/') + 1);
          var fName = decodeURIComponent(res.href.substring(res.href.lastIndexOf('/') + 1));
          FileList.navigate(parentPath);
          setTimeout(function() { FileList.selectByName(fName); }, 500);
        });
      })(r);
      titleRow.appendChild(showLink);

      item.appendChild(titleRow);

      var pathEl = document.createElement('div');
      pathEl.className = 'search-result-path';
      pathEl.textContent = r.path;
      item.appendChild(pathEl);

      list.appendChild(item);
    }
    wrap.appendChild(list);

    // Pagination
    var pag = document.createElement('div');
    pag.className = 'search-pagination';
    var savedSubPath = self._searchThisDir ? self._lastSubPath : null;
    if (skip > 0) {
      var prevBtn = document.createElement('a');
      prevBtn.href = '#';
      prevBtn.textContent = 'Prev';
      prevBtn.className = 'search-pag-btn';
      prevBtn.addEventListener('click', function(e) {
        e.preventDefault();
        self._fetchAndShowFiles(query, savedSubPath, skip - self._pageSize);
      });
      pag.appendChild(prevBtn);
    }
    if (total === -1 || skip + results.length < total) {
      var nextBtn = document.createElement('a');
      nextBtn.href = '#';
      nextBtn.textContent = 'Next';
      nextBtn.className = 'search-pag-btn';
      nextBtn.addEventListener('click', function(e) {
        e.preventDefault();
        self._fetchAndShowFiles(query, savedSubPath, skip + self._pageSize);
      });
      pag.appendChild(nextBtn);
    }
    if (pag.children.length) wrap.appendChild(pag);

    // Open or reuse window
    if (this._winId && WinManager.getWindow(this._winId)) {
      var win = WinManager.getWindow(this._winId);
      var body = win.el.querySelector('.win-body');
      body.innerHTML = '';
      body.appendChild(wrap);
      WinManager.setTitle(this._winId, this._buildTitle(query));
      searchInput.focus();
    } else {
      this._winId = WinManager.open(this._buildTitle(query), wrap, {});
      var winEl = WinManager.getWindow(this._winId).el;
      winEl.style.width = Math.min(500, window.innerWidth) + 'px';
      winEl.style.height = Math.min(600, window.innerHeight * 0.7) + 'px';
      setTimeout(function() { searchInput.focus(); }, 100);
    }
  },

  init() {
    var self = this;
    var searchBtn = document.getElementById('search-btn');
    var searchBar = document.getElementById('search-bar');
    var searchInput = document.getElementById('search-input');
    var searchCancel = document.getElementById('search-cancel');

    searchBtn.addEventListener('click', function() {
      self.showInput();
    });

    searchCancel.addEventListener('click', function() {
      self.hideInput();
    });

    var scopeBtn = document.getElementById('search-scope-btn');
    self._updateScopeBtn = function() {
      scopeBtn.classList.toggle('scope-all', !self._searchThisDir);
      scopeBtn.title = self._searchThisDir ? 'Search this directory' : 'Search all';
    };
    self._updateScopeBtn();
    scopeBtn.addEventListener('click', function() {
      self._searchThisDir = !self._searchThisDir;
      self._updateScopeBtn();
      self._updatePlaceholder();
    });

    var modeBtn = document.getElementById('search-mode-btn');
    self._updateModeBtn = function() {
      modeBtn.textContent = self._filenameMode ? 'File' : 'Doc';
      modeBtn.title = self._filenameMode ? 'Search filenames' : 'Search document contents';
      self._updatePlaceholder();
    };
    modeBtn.addEventListener('click', function() {
      self._filenameMode = !self._filenameMode;
      self._updateModeBtn();
    });

    self._acHandledEnter = false;

    searchInput.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        // Defer slightly so autocomplete's handler can set the flag
        setTimeout(function() {
          if (self._acHandledEnter) { self._acHandledEnter = false; return; }
          var q = searchInput.value.trim();
          if (q) self.doSearch(q);
        }, 0);
      } else if (e.key === 'Escape') {
        self.hideInput();
        self._updatePlaceholder();
      }
    });

    self._attachAutocompletes(searchInput);
  },

  // Attach all three autocomplete handlers to a search input
  _attachAutocompletes(searchInput) {
    var self = this;

    // Autocomplete for path suggestions (when in filename mode and starts with '/')
    Autocomplete.attach(searchInput, {
      minLength: 2,
      debounceMs: 150,
      onEnterHandled: function() { self._acHandledEnter = true; },
      shouldActivate: function(val) {
        return self._filenameMode && val.indexOf('/') === 0;
      },
      fetchSuggestions: function(query, callback) {
        // Absolute path query — don't filter by current directory
        var sugUrl = App.davUrl + '_search/suggest?q=' + encodeURIComponent(query) + '&max=10';
        fetch(sugUrl, {credentials: 'same-origin'})
          .then(function(r) { return r.json(); })
          .then(function(data) {
            if (data.ok && data.suggestions) {
              callback(data.suggestions.map(function(s) {
                return {label: s.path + (s.isDir ? '/' : ''), value: s.path, isDir: s.isDir};
              }));
            } else {
              callback([]);
            }
          })
          .catch(function() { callback([]); });
      },
      onSelect: function(item, input) {
        if (item.isDir) {
          // Navigate to the directory
          self.hideInput();
          App.navigateTo(App.davUrl.replace(/\/$/, '') + item.value + '/');
        } else {
          // Open the file
          self.hideInput();
          var fileName = item.value.substring(item.value.lastIndexOf('/') + 1);
          Viewers.open({
            name: fileName,
            href: App.davUrl.replace(/\/$/, '') + item.value,
            isDir: false
          });
        }
      }
    });

    // Autocomplete for filename word search (when in filename mode, not starting with '/')
    Autocomplete.attach(searchInput, {
      minLength: 2,
      debounceMs: 200,
      onEnterHandled: function() { self._acHandledEnter = true; },
      shouldActivate: function(val) {
        return self._filenameMode && val.indexOf('/') !== 0;
      },
      fetchSuggestions: function(query, callback) {
        var sugUrl = App.davUrl + '_search/suggest?mode=pathword&q=' + encodeURIComponent(query) + '&max=8';
        if (self._searchThisDir) sugUrl += '&subPath=' + encodeURIComponent(self.getCurrentDavPath());
        fetch(sugUrl, {credentials: 'same-origin'})
          .then(function(r) { return r.json(); })
          .then(function(data) {
            if (data.ok && data.suggestions) {
              callback(data.suggestions.map(function(s) {
                return {label: s.path, value: s.path, isDir: s.isDir};
              }));
            } else {
              callback([]);
            }
          })
          .catch(function() { callback([]); });
      },
      onSelect: function(item, input) {
        self.hideInput();
        if (item.isDir) {
          App.navigateTo(App.davUrl.replace(/\/$/, '') + item.value + '/');
        } else {
          var fileName = item.value.substring(item.value.lastIndexOf('/') + 1);
          Viewers.open({
            name: fileName,
            href: App.davUrl.replace(/\/$/, '') + item.value,
            isDir: false
          });
        }
      }
    });

    // Autocomplete for word suggestions (when in doc mode)
    Autocomplete.attach(searchInput, {
      minLength: 2,
      debounceMs: 200,
      onEnterHandled: function() { self._acHandledEnter = true; },
      shouldActivate: function(val) {
        if (self._filenameMode) return false;
        var words = val.trim().split(/\s+/);
        var last = words[words.length - 1];
        return last.length >= 2;
      },
      fetchSuggestions: function(query, callback) {
        fetch(App.davUrl + '_search/suggest?mode=word&q=' + encodeURIComponent(query) + '&max=8', {credentials: 'same-origin'})
          .then(function(r) { return r.json(); })
          .then(function(data) {
            if (data.ok && data.suggestions) {
              callback(data.suggestions);
            } else {
              callback([]);
            }
          })
          .catch(function() { callback([]); });
      },
      onSelect: function(item, input) {
        input.value = item.value;
        self.doSearch(item.value);
      }
    });
  }
};


/* -----------------------------------------------------------------------
 * Section 6: Tree — Sidebar directory tree
 * ----------------------------------------------------------------------- */

const Tree = {
  _container: null,
  _activePath: null,
  _nodes: {},

  init() {
    this._container = document.getElementById('tree');
    this._container.innerHTML = '';
    this._nodes = {};
  },

  async loadRoot() {
    const homeUrl = Auth.getUserHomeUrl();
    const rootNode = this._createNode(Auth.username, homeUrl, 0);
    this._container.appendChild(rootNode);

    // Try to load shared directory too
    try {
      const rootList = await DavClient.list(App.davUrl, 1);
      rootList.forEach(item => {
        if (!item.isSelf && item.isDir && item.name !== Auth.username) {
          const node = this._createNode(item.name, item.href, 0);
          this._container.appendChild(node);
        }
      });
    } catch (e) {
      // Non-admin may not be able to list root with all dirs
    }

    // Auto-expand user home
    await this.toggle(rootNode);
    this.setActive(homeUrl);
  },

  _createNode(name, href, level) {
    const node = document.createElement('div');
    node.className = 'tree-node';
    node.dataset.href = href;
    node.dataset.level = level;

    const row = document.createElement('div');
    row.className = 'tree-row';
    row.style.paddingLeft = 'calc(' + level + ' * var(--tree-indent) + var(--space-sm))';

    const arrow = document.createElement('span');
    arrow.className = 'tree-arrow';

    const icon = document.createElement('span');
    var iconCls = 'icon-folder';
    if (name === 'trash') iconCls = 'icon-trash';
    else if (Auth.mountNames && Auth.mountNames.indexOf(name) !== -1 &&
             href.replace(/[^/]+\/$/, '') === Auth.getUserHomeUrl()) {
      iconCls = 'icon-folder-cloud';
    }
    icon.className = 'icon ' + iconCls;
    icon.style.width = '16px';
    icon.style.height = '16px';

    const label = document.createElement('span');
    label.className = 'tree-label';
    label.textContent = name;

    row.appendChild(arrow);
    row.appendChild(icon);
    row.appendChild(label);

    const children = document.createElement('div');
    children.className = 'tree-children';

    node.appendChild(row);
    node.appendChild(children);

    // Click label to navigate
    label.addEventListener('click', (e) => {
      e.stopPropagation();
      App.navigateTo(href);
    });

    // Click arrow to expand/collapse
    arrow.addEventListener('click', (e) => {
      e.stopPropagation();
      this.toggle(node);
    });

    // Click row to navigate (clicking on the row itself)
    row.addEventListener('click', () => {
      App.navigateTo(href);
    });

    // Right-click context menu
    row.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      e.stopPropagation();
      // Create a fake item for the context menu
      var dirItem = {
        name: name,
        href: href,
        isDir: true,
        mime: 'httpd/unix-directory'
      };
      // Select this directory in the file list so context menu actions work
      FileList.selected.clear();
      FileList.selected.add(href);
      App.showContextMenu(e, dirItem);
    });

    // Drop target for drag-and-drop into this folder
    DragDrop.makeTarget(row, href);

    this._nodes[href] = node;
    return node;
  },

  async toggle(node) {
    const children = node.querySelector('.tree-children');
    const arrow = node.querySelector('.tree-arrow');
    const isOpen = children.classList.contains('open');

    if (isOpen) {
      children.classList.remove('open');
      arrow.classList.remove('expanded');
      return;
    }

    // If not loaded yet, fetch children
    if (!node.dataset.loaded) {
      await this._loadChildren(node);
      node.dataset.loaded = '1';
    }

    children.classList.add('open');
    arrow.classList.add('expanded');
  },

  async _loadChildren(node) {
    const href = node.dataset.href;
    const level = parseInt(node.dataset.level) + 1;
    const children = node.querySelector('.tree-children');

    const loading = document.createElement('div');
    loading.className = 'tree-loading';
    loading.textContent = 'Loading...';
    children.appendChild(loading);

    try {
      const items = await DavClient.list(href, 1);
      children.innerHTML = '';
      const dirs = items.filter(i => i.isDir && !i.isSelf).sort((a, b) =>
        a.name.toLowerCase().localeCompare(b.name.toLowerCase())
      );

      if (dirs.length === 0) {
        // No subdirectories; hide arrow
        node.querySelector('.tree-arrow').classList.add('hidden');
      }

      dirs.forEach(item => {
        const childHref = item.href.replace(/\/?$/, '/');
        const childNode = this._createNode(item.name, childHref, level);
        children.appendChild(childNode);
      });
    } catch (e) {
      children.innerHTML = '';
    }
  },

  setActive(href) {
    // Remove previous active
    const prev = this._container.querySelector('.tree-node-active');
    if (prev) prev.classList.remove('tree-node-active');

    this._activePath = href;
    const node = this._nodes[href];
    if (node) node.classList.add('tree-node-active');
  },

  async revealPath(href) {
    // Walk the path and expand each segment
    const segments = href.replace(App.davUrl, '').split('/').filter(Boolean);
    let current = App.davUrl;
    for (let i = 0; i < segments.length; i++) {
      current += segments[i] + '/';
      const node = this._nodes[current];
      if (node) {
        const children = node.querySelector('.tree-children');
        if (!children.classList.contains('open')) {
          await this.toggle(node);
        }
      }
    }
    this.setActive(href);
  },

  async refresh(href) {
    const node = this._nodes[href];
    if (!node) return;
    node.dataset.loaded = '';
    const children = node.querySelector('.tree-children');
    children.innerHTML = '';
    children.classList.remove('open');
    node.querySelector('.tree-arrow').classList.remove('expanded', 'hidden');
    await this.toggle(node);
  }
};


/* -----------------------------------------------------------------------
 * Section 7: FileList — Detail table and grid view with sorting
 * ----------------------------------------------------------------------- */

const FileList = {
  items: [],
  currentPath: '',
  viewMode: 'detail',  // 'detail' or 'grid'
  sortKey: 'name',
  sortDesc: false,
  selected: new Set(),
  _container: null,
  _lastClickIndex: -1,
  _arrowAnchor: -1,
  _isMobile: window.matchMedia('(pointer: coarse)').matches && ('ontouchstart' in window),
  _colWidths: {},  // persisted column widths { colKey: px }

  // All possible columns — name is always visible
  ALL_COLUMNS: [
    { key: 'name',        label: 'Name',        cls: 'col-name',        alwaysOn: true },
    { key: 'size',        label: 'Size',         cls: 'col-size' },
    { key: 'type',        label: 'Type',         cls: 'col-type' },
    { key: 'date',        label: 'Modified',     cls: 'col-date' },
    { key: 'owner',       label: 'Owner:Group',   cls: 'col-owner' },
    { key: 'permissions', label: 'Permissions',  cls: 'col-permissions' }
  ],
  _visibleCols: null,

  init() {
    this._container = document.getElementById('file-list');
    // Restore preferences
    this.viewMode = localStorage.getItem('fm_view') || 'detail';
    this.sortKey = localStorage.getItem('fm_sort_key') || 'name';
    this.sortDesc = localStorage.getItem('fm_sort_desc') === 'true';
    this.showHidden = localStorage.getItem('fm_show_hidden') === 'true';
    // Restore visible columns (default: name, size, type, date)
    try {
      const saved = JSON.parse(localStorage.getItem('fm_columns'));
      if (Array.isArray(saved) && saved.length > 0) this._visibleCols = saved;
    } catch(e) {}
    if (!this._visibleCols) this._visibleCols = ['name', 'size', 'type', 'date'];
    // Ensure name is always included
    if (this._visibleCols.indexOf('name') === -1) this._visibleCols.unshift('name');
    // _colWidths kept in memory only — resets on page reload

    // Background context menu on empty space or parent row
    this._container.addEventListener('contextmenu', function(e) {
      // If right-click is on a file row/card, let the normal context menu handle it
      if (e.target.closest('tr[data-href]') || e.target.closest('.grid-item[data-href]')) return;
      // If right-click is on the column header, let the column menu handle it
      if (e.target.closest('thead')) return;
      e.preventDefault();
      App.showBgContextMenu(e);
    });
  },

  _getVisibleColumns() {
    return this.ALL_COLUMNS.filter(c => this._visibleCols.indexOf(c.key) !== -1);
  },

  _setVisibleCols(cols) {
    // Ensure name is always present
    if (cols.indexOf('name') === -1) cols.unshift('name');
    this._visibleCols = cols;
    localStorage.setItem('fm_columns', JSON.stringify(cols));
  },

  _toggleColumn(key) {
    if (key === 'name') return; // name is always visible
    const idx = this._visibleCols.indexOf(key);
    if (idx !== -1) {
      this._visibleCols.splice(idx, 1);
    } else {
      // Insert in canonical order
      const allKeys = this.ALL_COLUMNS.map(c => c.key);
      const newCols = allKeys.filter(k => this._visibleCols.indexOf(k) !== -1 || k === key);
      this._visibleCols = newCols;
    }
    localStorage.setItem('fm_columns', JSON.stringify(this._visibleCols));
    this.render();
  },

  async navigate(url, pushHistory) {
    url = url.replace(/\/?$/, '/');
    this.currentPath = url;
    this.selected.clear();
    this._updateSelectionBar();

    this._container.innerHTML = '<div class="file-list-loading"><span class="spinner"></span> Loading...</div>';

    try {
      const items = await DavClient.list(url, 1);
      this.items = items.filter(i => !i.isSelf);
      this._sortItems();
      this.render();
    } catch (e) {
      this._container.innerHTML = '<div class="file-list-empty">Failed to load directory</div>';
      return;
    }

    Toolbar.renderBreadcrumb(url);
    Toolbar.updateTrashButton(url);
    Tree.setActive(url);

    if (pushHistory !== false) {
      const hash = '#' + url.replace(/^\/dav\//, '/');
      history.pushState(null, '', hash);
      NavHistory.push(url);
    }
  },

  async reload() {
    await this.navigate(this.currentPath, false);
  },

  render() {
    if (this.viewMode === 'grid') {
      this._renderGridView();
    } else {
      this._renderDetailView();
    }
  },

  _renderDetailView() {
    const table = document.createElement('table');
    table.className = 'file-table';

    const columns = this._getVisibleColumns();
    const self = this;

    // Detect flex mode (mobile) — check after table is in DOM; for now peek at media query
    var isFlex = window.matchMedia('(max-width: 1024px)').matches;

    // Apply saved column widths: CSS variables in flex mode, inline styles in table mode
    if (isFlex) {
      columns.forEach(function(c) {
        if (self._colWidths[c.key] && c.key !== 'name') {
          table.style.setProperty('--col-' + c.key + '-w', self._colWidths[c.key] + 'px');
        }
      });
    }

    // Body (created before header so resize closures can reference it)
    const tbody = document.createElement('tbody');

    // Header
    const thead = document.createElement('thead');
    const hrow = document.createElement('tr');

    const thCheck = document.createElement('th');
    thCheck.className = 'col-check';
    const selectAll = document.createElement('input');
    selectAll.type = 'checkbox';
    selectAll.className = 'file-check';
    selectAll.addEventListener('change', () => this.selectAll(selectAll.checked));
    thCheck.appendChild(selectAll);

    hrow.appendChild(thCheck);
    columns.forEach(col => {
      const th = document.createElement('th');
      th.className = col.cls + (this.sortKey === col.key ? ' sorted' : '');
      th.textContent = col.label;

      // Apply saved column width if available (inline style for table-layout: fixed only)
      if (self._colWidths[col.key] && !isFlex) {
        th.style.width = self._colWidths[col.key] + 'px';
      }

      const indicator = document.createElement('span');
      indicator.className = 'sort-indicator icon ' +
        (this.sortDesc ? 'icon-sort-desc' : 'icon-sort-asc');
      indicator.style.width = '12px';
      indicator.style.height = '12px';
      th.appendChild(indicator);

      th.addEventListener('click', () => this.toggleSort(col.key));

      // Resize handle
      const grip = document.createElement('span');
      grip.className = 'col-resize';
      grip.addEventListener('click', function(e) { e.stopPropagation(); });
      grip.addEventListener('dblclick', function(e) {
        e.stopPropagation();
        self._colWidths = {};
        self.render();
      });
      function startResize(startX) {
        grip.classList.add('active');
        var isFlex = getComputedStyle(hrow).display === 'flex';

        if (isFlex) {
          // Flex mode: freeze widths as CSS variables on the table
          // so all rows (th and td) stay in sync
          columns.forEach(function(c) {
            var cTh = hrow.querySelector('.' + c.cls);
            if (cTh) table.style.setProperty('--col-' + c.key + '-w', cTh.getBoundingClientRect().width + 'px');
          });
        } else {
          // Fixed table layout: freeze header cell widths
          var allThs = hrow.children;
          for (var ci = 0; ci < allThs.length; ci++) {
            if (!allThs[ci].style.width)
              allThs[ci].style.width = allThs[ci].getBoundingClientRect().width + 'px';
          }
        }

        var startW = th.getBoundingClientRect().width;
        var lastTh = hrow.children[hrow.children.length - 1];
        var lastCol = columns[columns.length - 1];

        function doResize(clientX) {
          var delta = clientX - startX;
          var minW = col.key === 'name' ? 120 : 50;
          var newW = Math.max(minW, startW + delta);

          if (isFlex) {
            table.style.setProperty('--col-' + col.key + '-w', newW + 'px');
            // Absorb delta from last column
            if (lastCol && lastCol.key !== col.key) {
              var containerW = table.parentNode.clientWidth;
              var totalW = 32; // col-check
              columns.forEach(function(c) {
                var v = table.style.getPropertyValue('--col-' + c.key + '-w');
                if (v) totalW += parseFloat(v);
              });
              var diff = containerW - totalW;
              var lastW = parseFloat(table.style.getPropertyValue('--col-' + lastCol.key + '-w')) || 50;
              var newLastW = lastW + diff;
              if (newLastW >= 50) table.style.setProperty('--col-' + lastCol.key + '-w', newLastW + 'px');
            }
          } else {
            th.style.width = newW + 'px';
            // Absorb delta from the last column to keep table flush
            if (lastTh && lastTh !== th) {
              var containerW = table.parentNode.clientWidth;
              var tableW = table.getBoundingClientRect().width;
              if (tableW !== containerW) {
                var lastW = lastTh.getBoundingClientRect().width;
                var adjust = containerW - tableW;
                var newLastW = lastW + adjust;
                if (newLastW >= 50) lastTh.style.width = newLastW + 'px';
              }
            }
          }
        }
        function endResize() {
          grip.classList.remove('active');
          columns.forEach(function(c) {
            var cTh = hrow.querySelector('.' + c.cls);
            if (cTh) self._colWidths[c.key] = cTh.getBoundingClientRect().width;
          });
          document.removeEventListener('mousemove', onMouseMove);
          document.removeEventListener('mouseup', onMouseUp);
          document.removeEventListener('touchmove', onTouchMove);
          document.removeEventListener('touchend', onTouchEnd);
        }
        function onMouseMove(ev) { doResize(ev.clientX); }
        function onMouseUp() { endResize(); }
        function onTouchMove(ev) { ev.preventDefault(); doResize(ev.touches[0].clientX); }
        function onTouchEnd() { endResize(); }

        document.addEventListener('mousemove', onMouseMove);
        document.addEventListener('mouseup', onMouseUp);
        document.addEventListener('touchmove', onTouchMove, { passive: false });
        document.addEventListener('touchend', onTouchEnd);
      }
      grip.addEventListener('mousedown', function(e) {
        e.preventDefault();
        e.stopPropagation();
        startResize(e.clientX);
      });
      grip.addEventListener('touchstart', function(e) {
        e.preventDefault();
        e.stopPropagation();
        startResize(e.touches[0].clientX);
      }, { passive: false });
      th.appendChild(grip);

      hrow.appendChild(th);
    });

    // Right-click on header row to show column chooser
    hrow.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      this._showColumnMenu(e);
    });

    thead.appendChild(hrow);
    table.appendChild(thead);

    // Parent directory row
    const parentUrl = this._getParentUrl();
    if (parentUrl) {
      const ptr = this._createParentRow(parentUrl);
      tbody.appendChild(ptr);
    }

    const visible = this._getVisibleItems();
    visible.forEach((item) => {
      const realIdx = this.items.indexOf(item);
      const tr = this._createDetailRow(item, realIdx);
      tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    this._container.innerHTML = '';
    this._container.appendChild(table);

    if (visible.length === 0 && !parentUrl) {
      const empty = document.createElement('div');
      empty.className = 'file-list-empty';
      empty.innerHTML = '<span class="icon icon-folder" style="width:48px;height:48px;background-color:var(--color-fg-muted)"></span>Empty folder';
      this._container.appendChild(empty);
    }
  },

  _createDetailRow(item, idx) {
    const tr = document.createElement('tr');
    tr.dataset.idx = idx;
    tr.dataset.href = item.href;

    if (this.selected.has(item.href)) tr.classList.add('selected');

    // Checkbox
    const tdCheck = document.createElement('td');
    tdCheck.className = 'col-check';
    const check = document.createElement('input');
    check.type = 'checkbox';
    check.className = 'file-check';
    check.checked = this.selected.has(item.href);
    check.addEventListener('change', (e) => {
      e.stopPropagation();
      this.toggleItemSelection(item.href, check.checked, idx, e);
    });
    tdCheck.appendChild(check);

    tr.appendChild(tdCheck);

    // Dynamic columns
    const visCols = this._getVisibleColumns();
    for (const col of visCols) {
      const td = document.createElement('td');
      td.className = col.cls;

      switch (col.key) {
        case 'name': {
          const nameDiv = document.createElement('div');
          nameDiv.className = 'file-name';
          const iconWrap = document.createElement('span');
          iconWrap.className = 'icon-wrap';
          const iconSpan = document.createElement('span');
          iconSpan.className = 'icon ' + this._getIconClass(item);
          iconWrap.appendChild(iconSpan);
          const access = this._getAccessLevel(item);
          if (access === 'none') {
            const badge = document.createElement('span');
            badge.className = 'icon icon-no-access perm-overlay';
            badge.title = item.fsReadable === false ? 'No access (mounted filesystem)' : 'No read access';
            iconWrap.appendChild(badge);
          } else if (access === 'readonly') {
            const badge = document.createElement('span');
            badge.className = 'icon icon-lock perm-overlay';
            badge.title = item.fsWritable === false ? 'Read only (mounted filesystem)' : 'Read only';
            iconWrap.appendChild(badge);
          }
          const nameText = document.createElement('span');
          nameText.className = 'file-name-text';
          nameText.textContent = item.name;
          nameDiv.appendChild(iconWrap);
          nameDiv.appendChild(nameText);
          if (item.isSymlink) {
            const linkBadge = document.createElement('span');
            linkBadge.className = 'symlink-badge';
            linkBadge.textContent = item.symlinkBroken ? 'broken link' : 'link';
            linkBadge.title = item.symlinkBroken
              ? 'Broken symlink (target missing)'
              : 'Symlink \u2192 ' + item.symlinkTarget;
            nameDiv.appendChild(linkBadge);
          }
          if (item.shared) {
            const shBadge = document.createElement('span');
            shBadge.className = 'shared-badge';
            shBadge.textContent = 'shared';
            shBadge.title = 'Shared to web';
            nameDiv.appendChild(shBadge);
          }
          td.appendChild(nameDiv);
          break;
        }
        case 'size':
          td.textContent = item.isDir ? '' : this._formatSize(item.size);
          break;
        case 'type':
          if (item.isSymlink) {
            td.textContent = item.isDir ? 'Folder Link' : 'File Link';
            td.title = item.symlinkTarget || '';
          } else {
            td.textContent = item.isDir ? 'Folder' : this._getTypeName(item);
          }
          break;
        case 'date':
          td.textContent = item.modified ? this._formatDate(item.modified) : '';
          if (item.modified) td.title = item.modified.toLocaleString();
          break;
        case 'owner':
          td.textContent = (item.owner || '') + ':' + (item.group || 'nogroup');
          break;
        case 'permissions':
          td.textContent = item.permissions != null ? this._formatPermissions(item.permissions) : '';
          break;
      }
      tr.appendChild(td);
    }

    // Double click to open
    tr.addEventListener('dblclick', (e) => {
      if (e.target.closest('.col-check')) return;
      this._openItem(item);
    });

    // Single click: desktop = select row, mobile = keep checkbox behavior
    tr.addEventListener('click', (e) => {
      if (e.target.closest('.col-check')) return; // checkbox handles itself
      if (this._isMobile) return; // mobile uses checkboxes
      e.preventDefault();
      this._handleRowClick(item.href, idx, e);
    });

    // Context menu
    tr.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      App.showContextMenu(e, item);
    });

    // Drag source
    DragDrop.makeSource(tr, item);
    // Drop target (directories only)
    if (item.isDir) DragDrop.makeTarget(tr, item.href);

    return tr;
  },

  _createParentRow(parentUrl) {
    const tr = document.createElement('tr');
    tr.className = 'file-row-parent';

    const tdCheck = document.createElement('td');
    tdCheck.className = 'col-check';

    const tdName = document.createElement('td');
    tdName.className = 'col-name';
    const nameDiv = document.createElement('div');
    nameDiv.className = 'file-name';
    const iconSpan = document.createElement('span');
    iconSpan.className = 'icon icon-arrow-left';
    const nameText = document.createElement('span');
    nameText.className = 'file-name-text';
    nameText.textContent = '..';
    nameDiv.appendChild(iconSpan);
    nameDiv.appendChild(nameText);
    tdName.appendChild(nameDiv);

    tr.appendChild(tdCheck);
    tr.appendChild(tdName);
    // Empty cells for remaining visible columns
    const visCols = this._getVisibleColumns();
    for (let i = 1; i < visCols.length; i++) {
      tr.appendChild(document.createElement('td'));
    }

    tr.addEventListener('click', () => App.navigateTo(parentUrl));
    tr.style.cursor = 'pointer';
    DragDrop.makeTarget(tr, parentUrl);

    return tr;
  },

  _renderGridView() {
    const grid = document.createElement('div');
    grid.className = 'grid-container';

    // Parent directory
    const parentUrl = this._getParentUrl();
    if (parentUrl) {
      const card = document.createElement('div');
      card.className = 'grid-item';
      card.innerHTML = '<div class="grid-thumb"><span class="icon icon-arrow-left"></span></div>' +
        '<span class="grid-label">..</span>';
      card.addEventListener('click', () => App.navigateTo(parentUrl));
      grid.appendChild(card);
    }

    const visible = this._getVisibleItems();
    visible.forEach((item) => {
      const realIdx = this.items.indexOf(item);
      const card = document.createElement('div');
      card.className = 'grid-item' + (this.selected.has(item.href) ? ' selected' : '');
      card.dataset.idx = realIdx;
      card.dataset.href = item.href;

      // Checkbox
      const check = document.createElement('input');
      check.type = 'checkbox';
      check.className = 'file-check grid-check';
      check.checked = this.selected.has(item.href);
      check.addEventListener('change', (e) => {
        e.stopPropagation();
        this.toggleItemSelection(item.href, check.checked, realIdx, e);
      });

      // Thumbnail
      const thumb = document.createElement('div');
      thumb.className = 'grid-thumb';

      if (this._hasThumb(item)) {
        const img = document.createElement('img');
        img.src = App.davUrl + '_thumb' + item.href.substring(App.davUrl.length - 1);
        img.loading = 'lazy';
        img.alt = item.name;
        img.onerror = () => {
          fetch(img.src, {credentials: 'same-origin'}).then(function(r) {
            if (!r.ok) console.error('Thumbnail failed for', item.name, r.status, r.statusText);
          }).catch(function() {});
          img.remove();
          const icon = document.createElement('span');
          icon.className = 'icon ' + this._getIconClass(item);
          thumb.appendChild(icon);
        };
        thumb.appendChild(img);
      } else {
        const icon = document.createElement('span');
        icon.className = 'icon ' + this._getIconClass(item);
        thumb.appendChild(icon);
      }

      if (item.isSymlink) {
        card.classList.add('symlink-item');
        const linkOverlay = document.createElement('span');
        linkOverlay.className = 'icon icon-link symlink-overlay';
        thumb.appendChild(linkOverlay);
      }

      const gridAccess = this._getAccessLevel(item);
      if (gridAccess === 'none') {
        const badge = document.createElement('span');
        badge.className = 'icon icon-no-access perm-overlay-grid';
        badge.title = item.fsReadable === false ? 'No access (mounted filesystem)' : 'No read access';
        thumb.appendChild(badge);
      } else if (gridAccess === 'readonly') {
        const badge = document.createElement('span');
        badge.className = 'icon icon-lock perm-overlay-grid';
        badge.title = item.fsWritable === false ? 'Read only (mounted filesystem)' : 'Read only';
        thumb.appendChild(badge);
      }

      if (item.shared) {
        card.classList.add('shared-item');
        const shareOverlay = document.createElement('span');
        shareOverlay.className = 'shared-overlay';
        shareOverlay.title = 'Shared to web';
        thumb.appendChild(shareOverlay);
      }

      const label = document.createElement('span');
      label.className = 'grid-label';
      label.textContent = item.name;
      if (item.isSymlink) {
        label.title = item.symlinkBroken
          ? 'Broken symlink (target missing)'
          : 'Symlink \u2192 ' + item.symlinkTarget;
      }

      card.appendChild(check);
      card.appendChild(thumb);
      card.appendChild(label);

      card.addEventListener('dblclick', (e) => {
        if (e.target === check) return;
        this._openItem(item);
      });
      card.addEventListener('click', (e) => {
        if (e.target === check) return;
        if (this._isMobile) return;
        e.preventDefault();
        this._handleRowClick(item.href, realIdx, e);
      });
      card.addEventListener('contextmenu', (e) => {
        e.preventDefault();
        App.showContextMenu(e, item);
      });

      // Drag source + drop target (directories)
      DragDrop.makeSource(card, item);
      if (item.isDir) DragDrop.makeTarget(card, item.href);

      grid.appendChild(card);
    });

    this._container.innerHTML = '';
    this._container.appendChild(grid);
  },

  _openItem(item) {
    if (item.isDir) {
      App.navigateTo(item.href);
    } else {
      const viewerType = Viewers.getType(item);
      if (viewerType) {
        Viewers.open(item);
      }
    }
  },

  _sortItems() {
    const key = this.sortKey;
    const desc = this.sortDesc;

    this.items.sort((a, b) => {
      // Directories first
      if (a.isDir !== b.isDir) return a.isDir ? -1 : 1;

      let va, vb;
      switch (key) {
        case 'name':
          va = a.name.toLowerCase();
          vb = b.name.toLowerCase();
          return desc ? vb.localeCompare(va) : va.localeCompare(vb);
        case 'size':
          va = a.size || 0;
          vb = b.size || 0;
          break;
        case 'type':
          va = this._getTypeName(a).toLowerCase();
          vb = this._getTypeName(b).toLowerCase();
          return desc ? vb.localeCompare(va) : va.localeCompare(vb);
        case 'date':
          va = a.modified ? a.modified.getTime() : 0;
          vb = b.modified ? b.modified.getTime() : 0;
          break;
        case 'owner':
          va = ((a.owner || '') + ':' + (a.group || 'nogroup')).toLowerCase();
          vb = ((b.owner || '') + ':' + (b.group || 'nogroup')).toLowerCase();
          return desc ? vb.localeCompare(va) : va.localeCompare(vb);
        case 'permissions':
          va = a.permissions || 0;
          vb = b.permissions || 0;
          break;
        default:
          return 0;
      }
      return desc ? vb - va : va - vb;
    });
  },

  toggleSort(key) {
    if (this.sortKey === key) {
      this.sortDesc = !this.sortDesc;
    } else {
      this.sortKey = key;
      this.sortDesc = false;
    }
    localStorage.setItem('fm_sort_key', this.sortKey);
    localStorage.setItem('fm_sort_desc', String(this.sortDesc));
    this._sortItems();
    this.render();
    Toolbar.updateSortIndicator(this.sortKey, this.sortDesc);
  },

  toggleView() {
    this.viewMode = this.viewMode === 'detail' ? 'grid' : 'detail';
    localStorage.setItem('fm_view', this.viewMode);
    this.render();
    Toolbar.updateViewToggle(this.viewMode);
  },

  toggleHidden() {
    this.showHidden = !this.showHidden;
    localStorage.setItem('fm_show_hidden', String(this.showHidden));
    this.render();
    Toolbar.updateDotfilesToggle(this.showHidden);
  },

  _getVisibleItems() {
    if (this.showHidden) return this.items;
    return this.items.filter(i => !i.name.startsWith('.'));
  },

  getSelected() {
    return this.items.filter(i => this.selected.has(i.href));
  },

  clearSelection() {
    this.selected.clear();
    this._lastClickIndex = -1;
    this._arrowAnchor = -1;
    this._updateSelectionBar();
    this.render();
  },

  toggleItemSelection(href, checked, idx, evt) {
    // Shift-click for range selection
    if (evt && evt.shiftKey && this._lastClickIndex >= 0) {
      const start = Math.min(this._lastClickIndex, idx);
      const end = Math.max(this._lastClickIndex, idx);
      for (let i = start; i <= end; i++) {
        this.selected.add(this.items[i].href);
      }
    } else {
      if (checked) {
        this.selected.add(href);
      } else {
        this.selected.delete(href);
      }
    }
    this._lastClickIndex = idx;
    this._updateSelectionBar();
    this.render();
  },

  // Desktop click: plain = select one, Ctrl = toggle, Shift = range
  _handleRowClick(href, idx, e) {
    // Second click of a double-click — don't change selection
    if (e.detail >= 2) return;
    this._arrowAnchor = idx;
    if (e.shiftKey && this._lastClickIndex >= 0) {
      // Range select: keep existing selection, add range
      const start = Math.min(this._lastClickIndex, idx);
      const end = Math.max(this._lastClickIndex, idx);
      if (!e.ctrlKey && !e.metaKey) {
        this.selected.clear();
      }
      for (let i = start; i <= end; i++) {
        this.selected.add(this.items[i].href);
      }
    } else if (e.ctrlKey || e.metaKey) {
      // Toggle single item
      if (this.selected.has(href)) {
        this.selected.delete(href);
      } else {
        this.selected.add(href);
      }
      this._lastClickIndex = idx;
    } else {
      // Plain click: select only this item
      this.selected.clear();
      this.selected.add(href);
      this._lastClickIndex = idx;
    }
    this._updateSelectionBar();
    this._updateSelectionVisual();
  },

  // Arrow key navigation: dir = 1 (down) or -1 (up), shift extends selection
  _handleArrowKey(dir, shift) {
    var visible = this._getVisibleItems();
    if (visible.length === 0) return;

    // _lastClickIndex is a real this.items index; find its position in visible
    var curVisIdx = -1;
    if (this._lastClickIndex >= 0) {
      var curItem = this.items[this._lastClickIndex];
      if (curItem) curVisIdx = visible.indexOf(curItem);
    }
    if (curVisIdx < 0 && this.selected.size > 0) {
      var lastHref = Array.from(this.selected).pop();
      curVisIdx = visible.findIndex(function(i) { return i.href === lastHref; });
    }

    var newVisIdx;
    if (curVisIdx < 0) {
      newVisIdx = dir > 0 ? 0 : visible.length - 1;
    } else {
      newVisIdx = curVisIdx + dir;
    }
    if (newVisIdx < 0 || newVisIdx >= visible.length) return;

    // Convert anchor from real index to visible index for range selection
    var anchorVisIdx = -1;
    if (this._arrowAnchor >= 0) {
      var anchorItem = this.items[this._arrowAnchor];
      if (anchorItem) anchorVisIdx = visible.indexOf(anchorItem);
    }

    // Convert back to real index for storage
    var newRealIdx = this.items.indexOf(visible[newVisIdx]);

    if (shift) {
      var anchor = anchorVisIdx >= 0 ? anchorVisIdx : curVisIdx;
      if (anchor < 0) anchor = newVisIdx;
      this._arrowAnchor = this.items.indexOf(visible[anchor]);
      this.selected.clear();
      var start = Math.min(anchor, newVisIdx);
      var end = Math.max(anchor, newVisIdx);
      for (var i = start; i <= end; i++) {
        this.selected.add(visible[i].href);
      }
    } else {
      this._arrowAnchor = newRealIdx;
      this.selected.clear();
      this.selected.add(visible[newVisIdx].href);
    }
    this._lastClickIndex = newRealIdx;
    this._updateSelectionBar();
    this._updateSelectionVisual();
    this._scrollToIndex(newRealIdx);
  },

  _scrollToIndex(idx) {
    var row = this._container.querySelector('tr[data-idx="' + idx + '"], .grid-item[data-idx="' + idx + '"]');
    if (row) row.scrollIntoView({ block: 'nearest' });
  },

  // Inline rename: replace name text with an input field
  _inlineRename(item) {
    var row = this._container.querySelector('[data-href="' + CSS.escape(item.href) + '"]');
    if (!row) return;
    var nameEl = row.querySelector('.file-name-text');
    if (!nameEl) return;

    var origName = item.name;
    var input = document.createElement('input');
    input.type = 'text';
    input.className = 'inline-rename-input';
    input.value = origName;
    input.style.cssText = 'font-size:inherit;font-family:inherit;padding:1px 4px;border:1px solid var(--color-accent);border-radius:3px;outline:none;width:100%;box-sizing:border-box;background:var(--color-bg);color:var(--color-fg)';

    nameEl.style.display = 'none';
    nameEl.parentNode.insertBefore(input, nameEl.nextSibling);
    input.focus();

    // Select name without extension for files
    if (!item.isDir) {
      var dot = origName.lastIndexOf('.');
      if (dot > 0) {
        input.setSelectionRange(0, dot);
      } else {
        input.select();
      }
    } else {
      input.select();
    }

    var committed = false;
    var commit = async function() {
      if (committed) return;
      committed = true;
      var newName = input.value.trim();
      input.remove();
      nameEl.style.display = '';
      if (!newName || newName === origName) return;
      if (!isValidFileName(newName)) { Toast.error('Invalid file name'); return; }
      var destUrl = FileList.currentPath + encodeURIComponent(newName) + (item.isDir ? '/' : '');
      try {
        var resp = await DavClient.copyMove('MOVE', item.href, destUrl, false);
        if (resp.status === 412) { Toast.error('A file with that name already exists'); return; }
        if (!isDavOk(resp)) { Toast.error('Rename failed'); return; }
        Toast.success('Renamed to ' + newName);
        FileList.reload();
        Tree.refresh(FileList.currentPath);
      } catch (e) { Toast.error('Rename failed'); }
    };

    input.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') { e.preventDefault(); commit(); }
      if (e.key === 'Escape') { e.preventDefault(); committed = true; input.remove(); nameEl.style.display = ''; }
      e.stopPropagation();
    });
    input.addEventListener('blur', function() { commit(); });
  },

  // Update selected/unselected visual state without full re-render
  _updateSelectionVisual() {
    const rows = this._container.querySelectorAll('tr[data-href], .grid-item[data-href]');
    rows.forEach(row => {
      const href = row.dataset.href;
      if (!href) return;
      row.classList.toggle('selected', this.selected.has(href));
      const check = row.querySelector('.file-check');
      if (check) check.checked = this.selected.has(href);
    });
  },

  selectAll(checked) {
    if (checked) {
      this.items.forEach(i => this.selected.add(i.href));
    } else {
      this.selected.clear();
    }
    this._updateSelectionBar();
    this.render();
  },

  selectByName(name) {
    for (var i = 0; i < this.items.length; i++) {
      if (this.items[i].name === name) {
        this.selected.clear();
        this.selected.add(this.items[i].href);
        this._updateSelectionBar();
        this.render();
        // Scroll to the item
        var row = document.querySelector('[data-href="' + this.items[i].href + '"]');
        if (row) row.scrollIntoView({ block: 'center', behavior: 'smooth' });
        return;
      }
    }
  },

  _updateSelectionBar() {
    const hasSelection = this.selected.size > 0;
    document.getElementById('sel-copy').disabled = !hasSelection;
    document.getElementById('sel-cut').disabled = !hasSelection;
    var onMount = DragDrop._getZone(this.currentPath).indexOf('mount:') === 0;
    document.getElementById('sel-trash').disabled = !hasSelection || onMount;
    document.getElementById('sel-restore').disabled = !hasSelection;
    document.getElementById('sel-delete').disabled = !hasSelection;
    document.getElementById('sel-download').disabled = !hasSelection;

    // Update statusbar info
    if (this.selected.size === 0) {
      var count = this.items ? this.items.length : 0;
      var path = decodeURIComponent(this.currentPath || '').replace(/^\/dav\//, '/');
      WinManager.updateStatusInfo(path + ' \u2014 ' + count + ' item' + (count !== 1 ? 's' : ''));
    } else if (this.selected.size === 1) {
      var href = this.selected.values().next().value;
      var item = this.items.find(function(i) { return i.href === href; });
      if (item) {
        var path = decodeURIComponent(item.href).replace(/^\/dav\//, '/');
        var info = path;
        if (!item.isDir && item.size !== undefined) {
          info += ' \u2014 ' + this._formatSize(item.size);
        }
        WinManager.updateStatusInfo(info);
      }
    } else {
      var total = 0;
      var self = this;
      this.selected.forEach(function(href) {
        var item = self.items.find(function(i) { return i.href === href; });
        if (item && !item.isDir && item.size) total += item.size;
      });
      var info = this.selected.size + ' items selected';
      if (total > 0) info += ' \u2014 ' + this._formatSize(total);
      WinManager.updateStatusInfo(info);
    }
  },


  _getParentUrl() {
    const home = Auth.getUserHomeUrl();
    if (this.currentPath === App.davUrl) return null;
    if (this.currentPath === home) return App.davUrl;
    const parts = this.currentPath.replace(/\/$/, '').split('/');
    parts.pop();
    return parts.join('/') + '/';
  },

  _getIconClass(item) {
    if (item.isSymlink && item.symlinkBroken) return 'icon-link-broken';
    if (item.isSymlink && item.isDir) return 'icon-folder-link';
    if (item.isSymlink) return 'icon-file-link';
    if (item.isDir) {
      if (item.name === 'trash') return 'icon-trash';
      if (Auth.mountNames && Auth.mountNames.indexOf(item.name) !== -1) {
        // Check if this folder is directly inside user home (i.e. a mount root)
        var home = Auth.getUserHomeUrl();
        var parent = item.href.replace(/[^/]+\/$/, '');
        if (parent === home) return 'icon-folder-cloud';
      }
      return 'icon-folder';
    }
    const ext = this._getExt(item.name).toLowerCase();
    if (/^(js|ts|tsx|jsx|py|rb|c|h|java|go|rs|php|sh|bash|lua|pl|r|swift|kt|scala|zig|asm|css|html?|xml|json|yaml|yml|toml|ini|conf|sql|makefile|dockerfile)$/.test(ext)) return 'icon-file-code';
    if (/^(png|jpe?g|gif|svg|webp|bmp|ico|tiff?)$/.test(ext)) return 'icon-file-image';
    if (/^(mp4|webm|ogg|ogv|mkv|avi|mov)$/.test(ext)) return 'icon-file-video';
    if (/^(mp3|wav|flac|aac|m4a|opus|oga|wma)$/.test(ext)) return 'icon-file-audio';
    if (ext === 'pdf') return 'icon-file-pdf';
    if (ext === 'epub') return 'icon-file-text';
    if (/^(docx?|odt|rtf)$/.test(ext)) return 'icon-file-text';
    if (/^(xlsx?|ods|csv)$/.test(ext)) return 'icon-file-code';
    if (/^(pptx?|odp)$/.test(ext)) return 'icon-file-image';
    if (/^(zip|gz|tar|rar|7z|bz2|xz|tgz)$/.test(ext)) return 'icon-file-archive';
    if (/^(txt|md|csv|log|rtf)$/.test(ext)) return 'icon-file-text';
    // Fall back to MIME type
    if (item.mime) {
      if (item.mime.indexOf('text/') === 0) return 'icon-file-text';
      if (item.mime.indexOf('image/') === 0) return 'icon-file-image';
      if (item.mime.indexOf('video/') === 0) return 'icon-file-video';
      if (item.mime.indexOf('audio/') === 0) return 'icon-file-audio';
      if (item.mime === 'application/pdf') return 'icon-file-pdf';
      if (item.mime === 'application/json' || item.mime === 'application/xml' ||
          item.mime === 'application/javascript' || item.mime === 'application/x-sh' ||
          item.mime === 'application/x-shellscript') return 'icon-file-code';
      if (item.mime === 'application/zip' || item.mime === 'application/gzip' ||
          item.mime === 'application/x-tar' || item.mime === 'application/x-rar-compressed' ||
          item.mime === 'application/x-7z-compressed' || item.mime === 'application/x-bzip2' ||
          item.mime === 'application/x-xz') return 'icon-file-archive';
    }
    return 'icon-file';
  },

  _hasThumb(item) {
    if (item.isDir) return false;
    const ext = this._getExt(item.name).toLowerCase();
    if (/^(png|jpe?g|gif|webp|bmp|tiff?|mp4|webm|ogg|ogv|mkv|avi|mov)$/.test(ext)) return true;
    if (item.mime) {
      if (item.mime.indexOf('image/') === 0) return true;
      if (item.mime.indexOf('video/') === 0) return true;
    }
    return false;
  },

  _getExt(name) {
    const dot = name.lastIndexOf('.');
    return dot > 0 ? name.substring(dot + 1) : '';
  },

  // Friendly names for MIME types — add entries here as needed
  MIME_FRIENDLY: {
    'text/plain': 'Text File',
    'text/html': 'HTML File',
    'text/css': 'CSS File',
    'text/csv': 'CSV File',
    'text/markdown': 'Markdown File',
    'text/xml': 'XML File',
    'text/x-python': 'Python File',
    'text/x-ruby': 'Ruby File',
    'text/x-c': 'C Source File',
    'text/x-java-source': 'Java File',
    'application/javascript': 'JavaScript File',
    'application/json': 'JSON File',
    'application/xml': 'XML File',
    'application/pdf': 'PDF Document',
    'application/zip': 'ZIP Archive',
    'application/gzip': 'GZIP Archive',
    'application/x-tar': 'TAR Archive',
    'application/x-rar-compressed': 'RAR Archive',
    'application/x-7z-compressed': '7-Zip Archive',
    'application/x-bzip2': 'BZIP2 Archive',
    'application/x-xz': 'XZ Archive',
    'application/x-sh': 'Shell Script',
    'application/x-shellscript': 'Shell Script',
    'application/epub+zip': 'EPUB Book',
    'application/msword': 'Word Document',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'Word Document',
    'application/vnd.ms-excel': 'Excel Spreadsheet',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'Excel Spreadsheet',
    'application/vnd.ms-powerpoint': 'PowerPoint Presentation',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'PowerPoint Presentation',
    'application/octet-stream': 'Binary File',
    'image/jpeg': 'JPEG Image',
    'image/png': 'PNG Image',
    'image/gif': 'GIF Image',
    'image/svg+xml': 'SVG Image',
    'image/webp': 'WebP Image',
    'image/bmp': 'BMP Image',
    'image/tiff': 'TIFF Image',
    'video/mp4': 'MP4 Video',
    'video/webm': 'WebM Video',
    'video/x-matroska': 'MKV Video',
    'video/x-msvideo': 'AVI Video',
    'video/quicktime': 'QuickTime Video',
    'audio/mpeg': 'MP3 Audio',
    'audio/wav': 'WAV Audio',
    'audio/ogg': 'OGG Audio',
    'audio/flac': 'FLAC Audio',
    'audio/aac': 'AAC Audio',
    'audio/mp4': 'M4A Audio',
    'font/woff': 'WOFF Font',
    'font/woff2': 'WOFF2 Font',
    'font/ttf': 'TrueType Font'
  },

  _getTypeName(item) {
    if (item.isDir) return 'Folder';
    var mime = item.mime;
    if (mime && this.MIME_FRIENDLY[mime]) return this.MIME_FRIENDLY[mime];
    if (mime) return mime;
    var ext = this._getExt(item.name).toUpperCase();
    return ext ? ext + ' File' : 'File';
  },

  _formatSize(bytes) {
    if (bytes === null || bytes === undefined) return '';
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    const val = bytes / Math.pow(1024, i);
    return (i === 0 ? val : val.toFixed(1)) + ' ' + units[i];
  },

  _formatDate(d) {
    const now = new Date();
    const diff = now - d;
    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return Math.floor(diff / 60000) + ' min ago';
    if (diff < 86400000) return Math.floor(diff / 3600000) + ' hr ago';
    if (diff < 604800000) return Math.floor(diff / 86400000) + ' days ago';

    const month = d.toLocaleString('default', { month: 'short' });
    const day = d.getDate();
    const year = d.getFullYear();
    if (year === now.getFullYear()) return month + ' ' + day;
    return month + ' ' + day + ', ' + year;
  },

  _formatPermissions(mode) {
    if (mode == null) return '';
    const str = String(mode).padStart(3, '0');
    const chars = 'rwx';
    let result = '';
    for (let i = 0; i < 3; i++) {
      const digit = parseInt(str[i], 10);
      for (let b = 2; b >= 0; b--) {
        result += (digit & (1 << b)) ? chars[2 - b] : '-';
      }
    }
    return result + ' (' + str + ')';
  },

  // Returns 'full', 'readonly', or 'none' for the current user's access to an item
  _getAccessLevel(item) {
    // Filesystem-level restrictions (e.g. mounted volumes) override everything
    if (item.fsReadable === false) return 'none';
    if (item.fsWritable === false) {
      // Still check DAV permissions — might be further restricted
      var davLevel = this._getDavAccessLevel(item);
      return davLevel === 'none' ? 'none' : 'readonly';
    }
    return this._getDavAccessLevel(item);
  },

  _getDavAccessLevel(item) {
    if (Auth.admin) return 'full';
    if (item.permissions == null) return 'full';
    var mode = item.permissions;
    var bits;
    if (item.owner === Auth.username) {
      bits = Math.floor(mode / 100) % 10;
    } else if (item.group === 'everyone' ||
               (item.group !== 'nogroup' && Auth.groups && Auth.groups.indexOf(item.group) !== -1)) {
      bits = Math.floor(mode / 10) % 10;
    } else {
      bits = mode % 10;
    }
    if ((bits & 4) === 0) return 'none';
    if ((bits & 2) === 0) return 'readonly';
    return 'full';
  },

  _showColumnMenu(e) {
    // Remove any existing column menu
    const old = document.getElementById('col-ctx-menu');
    if (old) old.remove();

    const menu = document.createElement('div');
    menu.id = 'col-ctx-menu';
    menu.className = 'context-menu col-context-menu';
    menu.setAttribute('role', 'menu');

    // Detect which columns are CSS-hidden at current viewport
    var testRow = document.createElement('tr');
    testRow.style.cssText = 'position:absolute;visibility:hidden;pointer-events:none';
    for (const col of this.ALL_COLUMNS) {
      var testTd = document.createElement('td');
      testTd.className = col.cls;
      testRow.appendChild(testTd);
    }
    var testTable = document.querySelector('.file-table') || document.createElement('table');
    testTable.appendChild(testRow);
    var hiddenCols = {};
    for (const col of this.ALL_COLUMNS) {
      var testTd = testRow.querySelector('.' + col.cls);
      if (testTd && getComputedStyle(testTd).display === 'none') hiddenCols[col.key] = true;
    }
    testRow.remove();

    for (const col of this.ALL_COLUMNS) {
      if (hiddenCols[col.key]) continue;
      const btn = document.createElement('button');
      btn.className = 'ctx-item';
      const isVisible = this._visibleCols.indexOf(col.key) !== -1;

      if (col.alwaysOn) {
        btn.textContent = col.label;
        btn.disabled = true;
        btn.classList.add('ctx-always-on');
      } else {
        btn.textContent = (isVisible ? '\u2713 ' : '    ') + col.label;
        btn.addEventListener('click', () => {
          this._toggleColumn(col.key);
          menu.remove();
        });
      }
      menu.appendChild(btn);
    }

    // Separator + reset option
    var sep = document.createElement('hr');
    sep.className = 'ctx-separator';
    menu.appendChild(sep);

    var resetBtn = document.createElement('button');
    resetBtn.className = 'ctx-item';
    resetBtn.textContent = 'Reset Column Widths';
    resetBtn.addEventListener('click', () => {
      this._colWidths = {};
      this.render();
      menu.remove();
    });
    menu.appendChild(resetBtn);

    document.body.appendChild(menu);

    // Position near the click
    const mw = menu.offsetWidth;
    const mh = menu.offsetHeight;
    let x = e.clientX, y = e.clientY;
    if (x + mw > window.innerWidth) x = window.innerWidth - mw - 4;
    if (y + mh > window.innerHeight) y = window.innerHeight - mh - 4;
    menu.style.left = x + 'px';
    menu.style.top = y + 'px';
    menu.hidden = false;

    // Close on click outside or Escape
    const close = (ev) => {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('mousedown', close); }
    };
    const closeKey = (ev) => {
      if (ev.key === 'Escape') { menu.remove(); document.removeEventListener('keydown', closeKey); }
    };
    setTimeout(() => {
      document.addEventListener('mousedown', close);
      document.addEventListener('keydown', closeKey);
    }, 0);
  }
};


/* -----------------------------------------------------------------------
 * Section 8: Toolbar — Breadcrumb, view/sort, action buttons
 * ----------------------------------------------------------------------- */

// Navigation history for back/forward buttons
const NavHistory = {
  _stack: [],
  _index: -1,
  _skipNext: false,

  push(url) {
    // Trim forward history when navigating to a new page
    this._stack = this._stack.slice(0, this._index + 1);
    this._stack.push(url);
    this._index = this._stack.length - 1;
    this._updateButtons();
  },

  canGoBack() { return this._index > 0; },
  canGoForward() { return this._index < this._stack.length - 1; },

  goBack() {
    if (!this.canGoBack()) return;
    this._skipNext = true;
    this._index--;
    this._updateButtons();
    App.navigateTo(this._stack[this._index], false);
  },

  goForward() {
    if (!this.canGoForward()) return;
    this._skipNext = true;
    this._index++;
    this._updateButtons();
    App.navigateTo(this._stack[this._index], false);
  },

  onPopState(url) {
    // Find url in stack near current index
    if (this._index > 0 && this._stack[this._index - 1] === url) {
      this._index--;
    } else if (this._index < this._stack.length - 1 && this._stack[this._index + 1] === url) {
      this._index++;
    }
    this._updateButtons();
  },

  _updateButtons() {
    var back = document.getElementById('nav-back');
    var fwd = document.getElementById('nav-forward');
    if (back) back.disabled = !this.canGoBack();
    if (fwd) fwd.disabled = !this.canGoForward();
  }
};

const Toolbar = {
  _initialized: false,
  init() {
    if (this._initialized) return;
    this._initialized = true;
    // Navigation buttons
    document.getElementById('nav-back').addEventListener('click', () => NavHistory.goBack());
    document.getElementById('nav-forward').addEventListener('click', () => NavHistory.goForward());
    document.getElementById('nav-refresh').addEventListener('click', () => {
      FileList.reload();
      Tree.init();
      Tree.loadRoot();
    });

    document.getElementById('dotfiles-toggle').addEventListener('click', () => FileList.toggleHidden());
    document.getElementById('view-toggle').addEventListener('click', () => FileList.toggleView());
    document.getElementById('upload-btn').addEventListener('click', () => Upload.pickFiles());
    document.getElementById('newfolder-btn').addEventListener('click', () => App.newFolder());
    document.getElementById('newfile-btn').addEventListener('click', () => App.newFile());
    document.getElementById('empty-trash-btn').addEventListener('click', () => App._emptyTrash());
    document.getElementById('terminal-btn').addEventListener('click', () => App.openTerminal());
    document.getElementById('vnc-btn').addEventListener('click', () => App.openVnc());
    document.getElementById('settings-btn').addEventListener('click', () => App.openSettings());
    document.getElementById('logout-btn').addEventListener('click', () => Auth.logout());

    // Sort menu
    const sortBtn = document.getElementById('sort-btn');
    const sortMenu = document.getElementById('sort-menu');
    sortBtn.addEventListener('click', () => { sortMenu.hidden = !sortMenu.hidden; });
    sortMenu.querySelectorAll('.sort-option').forEach(opt => {
      opt.addEventListener('click', () => {
        FileList.toggleSort(opt.dataset.sort);
        sortMenu.hidden = true;
      });
    });
    document.addEventListener('click', (e) => {
      if (!e.target.closest('.sort-control')) sortMenu.hidden = true;
    });

    // Selection action buttons (in toolbar, disabled when nothing selected)
    document.getElementById('sel-download').addEventListener('click', () => App.downloadSelected());
    document.getElementById('sel-trash').addEventListener('click', () => App.trashSelected());
    document.getElementById('sel-restore').addEventListener('click', () => App.restoreSelected());
    document.getElementById('sel-delete').addEventListener('click', () => App.deleteSelected());
    document.getElementById('sel-copy').addEventListener('click', () => {
      Clipboard.set(FileList.getSelected(), 'copy', FileList.currentPath);
      FileList.clearSelection();
    });
    document.getElementById('sel-cut').addEventListener('click', () => {
      Clipboard.set(FileList.getSelected(), 'cut', FileList.currentPath);
      FileList.clearSelection();
    });

    // Paste bar
    document.getElementById('paste-here').addEventListener('click', () => {
      Clipboard.paste(FileList.currentPath);
    });
    document.getElementById('paste-cancel').addEventListener('click', () => {
      Clipboard.clear();
    });

    // Sidebar toggle
    document.getElementById('sidebar-toggle').addEventListener('click', () => {
      const app = document.getElementById('app');
      app.classList.toggle('sidebar-collapsed');
      if (app.classList.contains('sidebar-collapsed')) {
        app.style.gridTemplateColumns = '';
      }
    });

    // Hamburger: on mobile opens overlay sidebar, on desktop uncollapse sidebar
    document.getElementById('sidebar-hamburger').addEventListener('click', () => {
      const app = document.getElementById('app');
      if (app.classList.contains('sidebar-collapsed')) {
        app.classList.remove('sidebar-collapsed');
      } else {
        document.getElementById('sidebar').classList.add('sidebar-open');
        document.getElementById('sidebar-overlay').hidden = false;
      }
    });
    document.getElementById('sidebar-overlay').addEventListener('click', () => {
      document.getElementById('sidebar').classList.remove('sidebar-open');
      document.getElementById('sidebar-overlay').hidden = true;
    });

    // Swipe left on sidebar to close it
    (function() {
      var sidebar = document.getElementById('sidebar');
      var touchStartX = 0;
      var touchStartY = 0;
      sidebar.addEventListener('touchstart', function(e) {
        if (e.touches.length !== 1) return;
        touchStartX = e.touches[0].clientX;
        touchStartY = e.touches[0].clientY;
      }, {passive: true});
      sidebar.addEventListener('touchend', function(e) {
        if (!sidebar.classList.contains('sidebar-open')) return;
        var dx = e.changedTouches[0].clientX - touchStartX;
        var dy = e.changedTouches[0].clientY - touchStartY;
        if (dx < -50 && Math.abs(dy) < Math.abs(dx)) {
          sidebar.classList.remove('sidebar-open');
          document.getElementById('sidebar-overlay').hidden = true;
        }
      }, {passive: true});
    })();

    // Sidebar resize drag
    const resizeHandle = document.getElementById('sidebar-resize');
    const appEl = document.getElementById('app');
    let resizing = false;
    resizeHandle.addEventListener('mousedown', (e) => {
      if (appEl.classList.contains('sidebar-collapsed')) return;
      resizing = true;
      resizeHandle.classList.add('active');
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';
      e.preventDefault();
    });
    document.addEventListener('mousemove', (e) => {
      if (!resizing) return;
      const w = Math.max(120, Math.min(e.clientX, window.innerWidth - 200));
      appEl.style.gridTemplateColumns = w + 'px 1fr';
    });
    document.addEventListener('mouseup', () => {
      if (!resizing) return;
      resizing = false;
      resizeHandle.classList.remove('active');
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    });

    this.updateViewToggle(FileList.viewMode);
    this.updateSortIndicator(FileList.sortKey, FileList.sortDesc);
    this.updateDotfilesToggle(FileList.showHidden);
  },

  renderBreadcrumb(url) {
    const bc = document.getElementById('breadcrumb');
    bc.innerHTML = '';

    // Parse path segments starting from DAV prefix
    const rel = url.replace(App.davUrl, '');
    const segments = rel.split('/').filter(Boolean);

    // Home link
    const homeLink = document.createElement('a');
    homeLink.className = 'breadcrumb-item';
    homeLink.textContent = Auth.username;
    homeLink.addEventListener('click', (e) => {
      e.preventDefault();
      App.navigateTo(Auth.getUserHomeUrl());
    });
    bc.appendChild(homeLink);

    let accumulated = Auth.getUserHomeUrl();
    // Skip the username segment since we already showed it as Home
    const startIdx = segments[0] === Auth.username ? 1 : 0;

    for (let i = startIdx; i < segments.length; i++) {
      const sep = document.createElement('span');
      sep.className = 'breadcrumb-sep';
      sep.textContent = '/';
      bc.appendChild(sep);

      accumulated += (i > startIdx || startIdx === 0 ? '' : '') +
        segments[i] + '/';
      // Rebuild from davUrl
      const segUrl = App.davUrl + segments.slice(0, i + 1).join('/') + '/';

      const link = document.createElement('a');
      link.className = 'breadcrumb-item';
      link.textContent = decodeURIComponent(segments[i]);
      link.addEventListener('click', ((u) => (e) => {
        e.preventDefault();
        App.navigateTo(u);
      })(segUrl));
      bc.appendChild(link);
    }
    Search.updateButton();
    Search._lastSubPath = Search.getCurrentDavPath();
  },

  updateViewToggle(mode) {
    const btn = document.getElementById('view-toggle');
    btn.classList.toggle('grid-active', mode === 'detail');
    btn.title = mode === 'detail' ? 'Switch to grid view' : 'Switch to detail view';
  },

  updateDotfilesToggle(showing) {
    const btn = document.getElementById('dotfiles-toggle');
    btn.classList.toggle('showing', showing);
    btn.title = showing ? 'Hide hidden files' : 'Show hidden files';
  },

  updateSortIndicator(key, desc) {
    const btn = document.getElementById('sort-btn');
    btn.classList.toggle('desc', desc);
    btn.title = 'Sort by ' + key + (desc ? ' (descending)' : ' (ascending)');

    // Update sort menu active state
    document.querySelectorAll('.sort-option').forEach(opt => {
      opt.classList.toggle('active', opt.dataset.sort === key);
    });
  },

  updateTrashButton(url) {
    const trashUrl = App.getTrashUrl();
    const inTrash = url.indexOf(trashUrl) === 0;
    document.getElementById('empty-trash-btn').hidden = !inTrash;
    document.getElementById('sel-trash').hidden = inTrash;
    document.getElementById('sel-restore').hidden = !inTrash;
  }
};


/* -----------------------------------------------------------------------
 * Section 9: Upload — Drag-drop and file picker with progress
 * ----------------------------------------------------------------------- */

const Upload = {
  _queue: [],
  _uploading: false,
  _initialized: false,

  init() {
    if (this._initialized) return;
    this._initialized = true;
    const mainPanel = document.getElementById('main-panel');
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    let dragCounter = 0;

    const isInternal = (e) => DragDrop._dragItems !== null;

    mainPanel.addEventListener('dragenter', (e) => {
      e.preventDefault();
      if (isInternal(e)) return;
      dragCounter++;
      dropZone.hidden = false;
    });

    mainPanel.addEventListener('dragleave', (e) => {
      e.preventDefault();
      if (isInternal(e)) return;
      dragCounter--;
      if (dragCounter <= 0) { dropZone.hidden = true; dragCounter = 0; }
    });

    mainPanel.addEventListener('dragover', (e) => {
      if (isInternal(e)) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
    });

    mainPanel.addEventListener('drop', (e) => {
      if (isInternal(e)) return;
      e.preventDefault();
      dragCounter = 0;
      dropZone.hidden = true;

      // Check for URL drop (text/uri-list or text/plain with URL)
      // Only accept http/https URLs, not file:// URIs
      var droppedUrl = '';
      if (e.dataTransfer.types.indexOf('text/uri-list') !== -1) {
        var uriList = (e.dataTransfer.getData('text/uri-list') || '').trim().split('\n')[0].trim();
        if (/^https?:\/\//i.test(uriList)) droppedUrl = uriList;
      }
      if (!droppedUrl && e.dataTransfer.types.indexOf('text/plain') !== -1) {
        var txt = (e.dataTransfer.getData('text/plain') || '').trim();
        if (/^https?:\/\//i.test(txt)) droppedUrl = txt.split('\n')[0].trim();
      }
      // Only treat as URL if there are no files being dropped
      if (droppedUrl && (!e.dataTransfer.files || e.dataTransfer.files.length === 0)) {
        // Check drop plugins first
        var dropPlugins = this._findDropPlugins(droppedUrl);
        if (dropPlugins.length > 0) {
          this._handlePluginDrop(droppedUrl, FileList.currentPath, dropPlugins, 0);
        } else {
          this._fetchUrl(droppedUrl, FileList.currentPath);
        }
        return;
      }

      if (e.dataTransfer.items) {
        this._processDropItems(e.dataTransfer.items, FileList.currentPath);
      } else if (e.dataTransfer.files.length > 0) {
        // Copy FileList to array synchronously before any async work
        this.uploadFiles(Array.from(e.dataTransfer.files), FileList.currentPath);
      }
    });

    fileInput.addEventListener('change', () => {
      if (fileInput.files.length > 0) {
        var files = Array.from(fileInput.files);
        fileInput.value = '';
        this.uploadFiles(files, FileList.currentPath);
      }
    });
  },

  pickFiles() {
    document.getElementById('file-input').click();
  },

  _abortCtrl: null,

  async upload(files, targetUrl, overwriteState) {
    if (!overwriteState) overwriteState = {value: null};
    if (!this._abortCtrl) this._initUploadPanel('Uploading');
    const panel = document.getElementById('upload-progress');

    let completed = 0, skipped = 0, cancelled = false;
    for (let i = 0; i < files.length; i++) {
      if (this._abortCtrl && this._abortCtrl.aborted) { cancelled = true; break; }
      const file = files[i];
      var finalUrl = targetUrl + encodeURIComponent(file.name);
      var uploadName = file.name;

      // Check if file already exists
      try {
        const headResp = await DavClient.send('HEAD', finalUrl);
        if (headResp.status >= 200 && headResp.status < 300) {
          // File exists — ask to overwrite
          var action;
          if (overwriteState.value === 'cancel') { skipped++; break; }
          else if (overwriteState.value === true) action = 'yes';
          else if (overwriteState.value === false) action = 'no';
          else if (overwriteState.value === 'rename') action = 'rename';
          else {
            var remaining = files.length - i - 1;
            action = await confirmOverwrite(file.name, remaining);
            if (action === 'yesAll') { overwriteState.value = true; action = 'yes'; }
            else if (action === 'noAll') { overwriteState.value = false; action = 'no'; }
            else if (action === 'renameAll') { overwriteState.value = 'rename'; action = 'rename'; }
            else if (action === 'cancel') { overwriteState.value = 'cancel'; skipped++; break; }
          }
          if (action === 'no') { skipped++; continue; }
          if (action === 'rename') {
            uploadName = await autoRename(targetUrl, file.name);
            finalUrl = targetUrl + encodeURIComponent(uploadName);
          }
        }
      } catch (e) { /* HEAD failed = doesn't exist, proceed */ }

      // Upload to a hidden temp file, then move into place on success
      var tempName = '.~upload-' + Date.now() + '-' + Math.random().toString(36).slice(2);
      var tempUrl = targetUrl + encodeURIComponent(tempName);

      const itemEl = document.createElement('div');
      itemEl.className = 'upload-item';
      itemEl.innerHTML = '<div class="upload-item-name">' + this._esc(uploadName) + '</div>' +
        '<progress value="0" max="100"></progress>';
      panel.appendChild(itemEl);
      const bar = itemEl.querySelector('progress');

      try {
        await DavClient.put(tempUrl, file, (loaded, total) => {
          bar.value = Math.round(loaded / total * 100);
        }, this._abortCtrl);
        // Move temp file to final destination (overwrite if exists)
        var moveResp = await DavClient.copyMove('MOVE', tempUrl, finalUrl, true);
        if (!isDavOk(moveResp)) {
          // Could not replace destination — rename the upload instead
          var altName = await autoRename(targetUrl, uploadName);
          var altUrl = targetUrl + encodeURIComponent(altName);
          var altResp = await DavClient.copyMove('MOVE', tempUrl, altUrl, false);
          if (!isDavOk(altResp)) {
            try { await DavClient.del(tempUrl); } catch (ignore) {}
            itemEl.classList.add('error');
            itemEl.querySelector('.upload-item-name').textContent = uploadName + ' (failed to save)';
          } else {
            itemEl.querySelector('.upload-item-name').textContent = altName;
            bar.value = 100;
            itemEl.classList.add('done');
            completed++;
            await Dialog.alert('Could not overwrite "' + uploadName + '". File was saved as "' + altName + '" instead.');
          }
        } else {
          itemEl.classList.add('done');
          bar.value = 100;
          completed++;
        }
      } catch (e) {
        // Clean up temp file on any failure
        try { await DavClient.del(tempUrl); } catch (ignore) {}
        if (this._abortCtrl && this._abortCtrl.aborted) {
          itemEl.classList.add('error');
          itemEl.querySelector('.upload-item-name').textContent = uploadName + ' (cancelled)';
          cancelled = true;
          break;
        }
        itemEl.classList.add('error');
      }
    }

    return {completed, skipped, cancelled};
  },

  _initUploadPanel(totalLabel) {
    const panel = document.getElementById('upload-progress');
    panel.hidden = false;
    this._abortCtrl = {aborted: false, xhr: null};
    panel.innerHTML = '<div class="upload-header">' + this._esc(totalLabel) +
      '<button class="modal-close upload-close-btn">&times;</button></div>';
    const self = this;
    panel.querySelector('.upload-close-btn').onclick = async function() {
      if (!self._abortCtrl || self._abortCtrl.aborted) {
        panel.hidden = true;
        return;
      }
      var yes = await Dialog.confirm('Cancel the upload in progress?');
      if (yes) {
        self._abortCtrl.aborted = true;
        if (self._abortCtrl.xhr) {
          self._abortCtrl.xhr.abort();
        }
      }
    };
  },

  // Pre-check: can we write to targetUrl? Try creating+deleting a probe file.
  async _checkWriteAccess(targetUrl) {
    var probeName = '.~writetest-' + Date.now();
    var probeUrl = targetUrl + encodeURIComponent(probeName);
    try {
      var resp = await DavClient.send('PUT', probeUrl, '');
      if (resp.status === 403) return false;
      if (resp.ok || resp.status === 201 || resp.status === 204) {
        try { await DavClient.del(probeUrl); } catch (ignore) {}
        return true;
      }
      return false;
    } catch (e) { return false; }
  },

  // Top-level upload entry point — wraps upload with summary toast and reload
  async uploadFiles(files, targetUrl) {
    if (!await this._checkWriteAccess(targetUrl)) {
      Dialog.alert('You do not have write permission for this folder.');
      return;
    }
    this._initUploadPanel('Uploading ' + files.length + ' file(s)');

    var result = await this.upload(files, targetUrl);
    if (result.cancelled) {
      Toast.warning('Upload cancelled (' + result.completed + ' file(s) completed)');
    } else {
      var msg = 'Uploaded ' + result.completed + '/' + files.length + ' file(s)';
      if (result.skipped) msg += ' (' + result.skipped + ' skipped)';
      Toast.success(msg);
    }
    this._abortCtrl = null;
    var panel = document.getElementById('upload-progress');
    setTimeout(() => { panel.hidden = true; }, 2000);
    FileList.reload();
    Tree.refresh(targetUrl);
  },

  async _processDropItems(items, targetUrl) {
    // Extract files/dirs synchronously — DataTransferItemList is cleared after the event handler returns
    const files = [];
    const dirs = [];

    for (let i = 0; i < items.length; i++) {
      const entry = items[i].webkitGetAsEntry ? items[i].webkitGetAsEntry() : null;
      if (entry) {
        if (entry.isDirectory) {
          dirs.push(entry);
        } else {
          const file = items[i].getAsFile();
          if (file) files.push(file);
        }
      } else {
        const file = items[i].getAsFile();
        if (file) files.push(file);
      }
    }

    if (!await this._checkWriteAccess(targetUrl)) {
      Dialog.alert('You do not have write permission for this folder.');
      return;
    }

    var totalCount = files.length + dirs.length;
    this._initUploadPanel('Uploading ' + totalCount + ' item(s)');

    const overwriteState = {value: null};
    let completed = 0, skipped = 0, cancelled = false;

    // Upload regular files
    if (files.length > 0) {
      var r = await this.upload(files, targetUrl, overwriteState);
      completed += r.completed;
      skipped += r.skipped;
      if (r.cancelled) cancelled = true;
    }

    // Upload directories recursively (merge with existing)
    if (!cancelled) {
      for (const dirEntry of dirs) {
        if (this._abortCtrl && this._abortCtrl.aborted) { cancelled = true; break; }
        var dr = await this._uploadDirectory(dirEntry, targetUrl, overwriteState);
        completed += dr.completed;
        skipped += dr.skipped;
        if (dr.cancelled) { cancelled = true; break; }
      }
    }

    if (cancelled) {
      Toast.warning('Upload cancelled (' + completed + ' file(s) completed)');
    } else {
      var msg = 'Uploaded ' + completed + ' file(s)';
      if (skipped) msg += ' (' + skipped + ' skipped)';
      Toast.success(msg);
    }
    this._abortCtrl = null;
    var panel = document.getElementById('upload-progress');
    setTimeout(() => { panel.hidden = true; }, 2000);
    FileList.reload();
    Tree.refresh(targetUrl);
  },

  async _uploadDirectory(dirEntry, parentUrl, overwriteState) {
    const dirUrl = parentUrl + encodeURIComponent(dirEntry.name) + '/';
    try { await DavClient.mkcol(dirUrl); } catch (e) { /* may exist — merge */ }

    const entries = await this._readDirEntries(dirEntry);
    const files = [];
    const subdirs = [];

    for (const entry of entries) {
      if (entry.isFile) {
        const file = await this._entryToFile(entry);
        if (file) files.push(file);
      } else if (entry.isDirectory) {
        subdirs.push(entry);
      }
    }

    let completed = 0, skipped = 0, cancelled = false;
    if (files.length > 0) {
      var r = await this.upload(files, dirUrl, overwriteState);
      completed += r.completed;
      skipped += r.skipped;
      if (r.cancelled) cancelled = true;
    }

    if (!cancelled) {
      for (const sub of subdirs) {
        if (this._abortCtrl && this._abortCtrl.aborted) { cancelled = true; break; }
        var sr = await this._uploadDirectory(sub, dirUrl, overwriteState);
        completed += sr.completed;
        skipped += sr.skipped;
        if (sr.cancelled) { cancelled = true; break; }
      }
    }
    return {completed, skipped, cancelled};
  },

  _readDirEntries(dirEntry) {
    return new Promise((resolve) => {
      const reader = dirEntry.createReader();
      const all = [];
      const readBatch = () => {
        reader.readEntries((entries) => {
          if (entries.length === 0) { resolve(all); return; }
          all.push(...entries);
          readBatch();
        }, () => resolve(all));
      };
      readBatch();
    });
  },

  _entryToFile(entry) {
    return new Promise((resolve) => {
      entry.file(f => resolve(f), () => resolve(null));
    });
  },

  _findDropPlugins(url) {
    var matches = [];
    if (!this._dropPlugins) return matches;
    for (var i = 0; i < this._dropPlugins.length; i++) {
      var patterns = this._dropPlugins[i].patterns;
      for (var j = 0; j < patterns.length; j++) {
        if (patterns[j].test(url)) {
          matches.push(this._dropPlugins[i].name);
          break;
        }
      }
    }
    return matches;
  },

  async _handlePluginDrop(url, targetDir, pluginList, pluginIndex, choice) {
    var pluginName = pluginList[pluginIndex];
    Toast.show('Processing URL...');
    try {
      var resp = await fetch(App.davUrl + '_plugin/drop', {
        method: 'POST', credentials: 'same-origin',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ url: url, dir: targetDir, plugin: pluginName, choice: choice || null })
      });
      var data = await resp.json();

      // Plugin says it can't handle this URL — try next plugin
      if (data.pass) {
        if (pluginIndex + 1 < pluginList.length) {
          this._handlePluginDrop(url, targetDir, pluginList, pluginIndex + 1);
        } else {
          // No more plugins — fall back to URL fetch
          this._fetchUrl(url, targetDir);
        }
        return;
      }

      if (data.ok && data.prompt) {
        var wrap = document.createElement('div');
        wrap.style.cssText = 'display:flex;flex-direction:column;gap:8px;padding:8px';
        var title = document.createElement('p');
        title.textContent = data.title || 'Choose an option';
        wrap.appendChild(title);
        var self = this;
        for (var i = 0; i < data.choices.length; i++) {
          (function(ch) {
            var btn = document.createElement('button');
            btn.className = 'btn btn-sm';
            btn.textContent = ch.label;
            btn.addEventListener('click', function() {
              Dialog.close();
              self._handlePluginDrop(url, targetDir, pluginList, pluginIndex, ch.value);
            });
            wrap.appendChild(btn);
          })(data.choices[i]);
        }
        Dialog.open('Plugin: ' + pluginName, wrap);
        return;
      }

      if (data.ok && data.background) {
        this._pollBackgroundJob(data.jobId, targetDir);
        return;
      }

      if (data.ok && data.created) {
        Toast.success('Created: ' + data.name);
        await FileList.reload();
        if (data.open) {
          var createdHref = targetDir + encodeURIComponent(data.name);
          var createdItem = { name: data.name, href: createdHref, isDir: false };
          if (data.openPlugin) {
            Viewers._openPlugin(createdItem, data.openPlugin);
          } else {
            Viewers.open(createdItem);
          }
        }
      } else if (data.error) {
        Toast.error(data.error);
      }
    } catch(e) {
      Toast.error('Plugin drop failed: ' + (e.message || 'connection error'));
    }
  },

  _pollBackgroundJob(jobId, targetDir) {
    // Create a persistent progress toast
    var c = Toast._getContainer();
    var el = document.createElement('div');
    el.className = 'toast toast-info';
    el.style.minWidth = '250px';
    el.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center">' +
      '<span id="job-msg-' + jobId + '">Downloading...</span>' +
      '<span id="job-size-' + jobId + '" style="font-size:12px;color:#888;margin-left:12px"></span>' +
      '</div>';
    c.appendChild(el);

    var msgEl = document.getElementById('job-msg-' + jobId);
    var sizeEl = document.getElementById('job-size-' + jobId);

    function formatSize(bytes) {
      if (bytes < 1024) return bytes + ' B';
      if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
      return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }

    var self = this;
    var pollTimer = setInterval(async function() {
      try {
        var resp = await fetch(App.davUrl + '_plugin/job?id=' + encodeURIComponent(jobId), { credentials: 'same-origin' });
        var data = await resp.json();
        if (!data.ok || !data.job) {
          clearInterval(pollTimer);
          el.remove();
          Toast.error('Job lost');
          return;
        }

        var job = data.job;
        sizeEl.textContent = formatSize(job.bytes || 0);

        if (job.status === 'complete') {
          clearInterval(pollTimer);
          el.remove();

          // Re-check directory for new files one final time
          try {
            var finalResp = await fetch(App.davUrl + '_plugin/job?id=' + encodeURIComponent(jobId), { credentials: 'same-origin' });
            var finalData = await finalResp.json();
            if (finalData.ok && finalData.job) job = finalData.job;
          } catch(e) {}

          if (job.files && job.files.length > 0) {
            Toast.success('Downloaded: ' + job.files.join(', '));
            await FileList.reload();
            if (job.open) {
              var bestFile = job.files[0];
              var bestBytes = 0;
              // Pick the largest file (likely the video)
              for (var jfi = 0; jfi < job.files.length; jfi++) {
                // files are just names; we don't have sizes here
                // but video files are typically listed last
                bestFile = job.files[jfi];
              }
              var createdHref = targetDir + encodeURIComponent(bestFile);
              var createdItem = { name: bestFile, href: createdHref, isDir: false };
              Viewers.open(createdItem);
            }
          } else {
            Toast.error('Download failed: ' + (job.output || 'no files created').substring(0, 500));
            await FileList.reload();
          }
        } else if (job.status === 'error') {
          clearInterval(pollTimer);
          el.remove();
          Toast.error('Download failed: ' + (job.error || 'unknown'));
        } else {
          // Still running
          msgEl.textContent = 'Downloading... ' +
            (job.files && job.files.length ? job.files.length + ' file(s)' : '');
        }
      } catch(e) {
        // Network error — keep polling
      }
    }, 2000);
  },

  async _fetchUrl(url, targetDir) {
    if (!await this._checkWriteAccess(targetDir)) {
      Dialog.alert('You do not have write permission for this folder.');
      return;
    }
    Toast.show('Downloading ' + url.substring(0, 80) + (url.length > 80 ? '...' : ''));
    try {
      const resp = await fetch(App.davUrl + '_fetchurl', {
        method: 'POST',
        credentials: 'same-origin',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url: url, dir: targetDir})
      });
      const data = await resp.json();
      if (data.ok) {
        Toast.success('Saved ' + data.filename + ' (' + this._formatSize(data.size) + ')');
        FileList.reload();
      } else {
        Toast.error(data.error || 'Download failed');
      }
    } catch (e) {
      Toast.error('Download failed: ' + (e.message || 'connection error'));
    }
  },

  _formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    return (bytes / 1073741824).toFixed(1) + ' GB';
  },

  _esc(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }
};


/* -----------------------------------------------------------------------
 * Section 10: Viewers — Image, code, video, audio, PDF, markdown
 * ----------------------------------------------------------------------- */

const Viewers = {
  TYPES: {
    office: /\.(docx?|xlsx?|pptx?|odt|ods|odp|csv|rtf|fodp|fods|fodt)$/i,
    code: /\.(js|mjs|ts|tsx|jsx|py|rb|c|h|cpp|hpp|java|go|rs|php|sh|bash|lua|pl|r|swift|kt|scala|zig|asm|css|scss|less|html?|xml|json|yaml|yml|toml|ini|conf|sql|makefile|dockerfile|txt|log|md|diff|patch|env|gitignore|editorconfig|srt|vtt)$/i,
    image: /\.(png|jpe?g|gif|svg|webp|bmp|ico|tiff?)$/i,
    video: /\.(mp4|webm|ogg|ogv)$/i,
    audio: /\.(mp3|wav|ogg|oga|flac|aac|m4a|opus|wma)$/i,
    playlist: /\.m3u8?$/i,
    pdf: /\.pdf$/i,
    epub: /\.epub$/i,
  },

  _loaded: {},

  // Session-level cache-bust map: clean href -> timestamp.
  // After saving an edited image, the URL is tagged so all subsequent
  // loads (viewer, background-image, etc.) bypass the browser cache.
  _cacheBust: {},

  _bustUrl: function(href) {
    var ts = this._cacheBust[href];
    if (!ts) return href;
    return href + (href.indexOf('?') < 0 ? '?' : '&') + '_t=' + ts;
  },

  // Map MIME type prefixes/patterns to viewer types
  MIME_MAP: {
    'application/vnd.openxmlformats-officedocument': 'office',
    'application/vnd.oasis.opendocument': 'office',
    'application/msword': 'office',
    'application/vnd.ms-excel': 'office',
    'application/vnd.ms-powerpoint': 'office',
    'text/csv': 'office',
    'application/rtf': 'office',
    'text/': 'code',
    'image/': 'image',
    'video/': 'video',
    'audio/x-mpegurl': 'playlist',
    'audio/': 'audio',
    'application/pdf': 'pdf',
    'application/epub': 'epub',
    'application/json': 'code',
    'application/xml': 'code',
    'application/javascript': 'code',
    'application/x-sh': 'code',
    'application/x-shellscript': 'code'
  },

  getType(item) {
    if (item.isDir) return null;
    // 1. Try extension-based match
    for (const [type, re] of Object.entries(this.TYPES)) {
      if (re.test(item.name)) return type;
    }
    // 2. Fall back to MIME type from server
    if (item.mime) {
      for (const [prefix, type] of Object.entries(this.MIME_MAP)) {
        if (item.mime.indexOf(prefix) === 0) return type;
      }
    }
    return null;
  },

  async open(item) {
    // Check plugins first (extension, then MIME type)
    if (!item.isDir) {
      var pluginName = null;
      if (this._pluginExtMap) {
        var ext = (item.name.match(/\.([^.]+)$/) || [])[1];
        if (ext) pluginName = this._pluginExtMap[ext.toLowerCase()];
      }
      if (!pluginName && this._pluginMimeMap && item.mime) {
        pluginName = this._pluginMimeMap[item.mime.toLowerCase()];
      }
      if (pluginName) {
        this._openPlugin(item, pluginName);
        return true;
      }
    }

    const type = this.getType(item);
    if (!type) return false;

    switch (type) {
      case 'image': this._openImage(item); break;
      case 'video': this._openVideo(item); break;
      case 'audio': this._openAudio(item); break;
      case 'office': this._openOffice(item); break;
      case 'pdf': await this._openPdf(item); break;
      case 'epub': await this._openEpub(item); break;
      case 'code': await this._openCode(item); break;
      case 'playlist': this._openPlaylist(item); break;
    }
    return true;
  },

  // adapted from snappygoat.com:
  _openImage(item) {
    const wrap = document.createElement('div');
    wrap.className = 'image-viewer';

    // 3-panel carousel track
    var track = document.createElement('div');
    track.className = 'imgview-track';
    track.style.transform = 'translateX(-33.3333%)';
    var panels = [];
    for (var pi = 0; pi < 3; pi++) {
      var panel = document.createElement('div');
      panel.className = 'imgview-panel';
      track.appendChild(panel);
      panels.push(panel);
    }
    wrap.appendChild(track);
    // panels[0]=prev, panels[1]=current, panels[2]=next
    var curPanel = panels[1];

    // Load image to get natural dimensions (use cache-busted URL if edited)
    const probe = new Image();
    probe.src = Viewers._bustUrl(item.href);

    // State
    let natW = 0, natH = 0;       // natural image dimensions
    let imgW = 0, imgH = 0;       // current rendered size
    let panX = 0, panY = 0;       // current position
    let fitW = 0, fitH = 0;       // fit-to-view dimensions
    let fitX = 0, fitY = 0;       // fit-to-view position

    // Get container dimensions (modal body)
    const viewSize = () => {
      const r = wrap.getBoundingClientRect();
      return { w: r.width, h: r.height };
    };

    // Constrain pan so image stays viewable
    const constrain = () => {
      const { w: vw, h: vh } = viewSize();
      if (vw < imgW) {
        if (panX > 0) panX = 0;
        if (panX < vw - imgW) panX = vw - imgW;
      } else {
        panX = (vw - imgW) / 2;
      }
      if (vh < imgH) {
        if (panY > 0) panY = 0;
        if (panY < vh - imgH) panY = vh - imgH;
      } else {
        panY = (vh - imgH) / 2;
      }
      curPanel.style.backgroundPosition = panX + 'px ' + panY + 'px';
    };

    // Zoom toward a point (or center)
    const zoom = (factor, evt) => {
      const { w: vw, h: vh } = viewSize();
      if (imgW > natW * 4 && factor > 1) return;
      if (imgW < vw * 0.25 && factor < 1) return;
      let fx = 0.5, fy = 0.5;
      if (evt) {
        const rect = wrap.getBoundingClientRect();
        fx = (evt.clientX - rect.left - panX) / imgW;
        fy = (evt.clientY - rect.top - panY) / imgH;
      }
      panX -= (imgW * factor - imgW) * fx;
      panY -= (imgH * factor - imgH) * fy;
      imgW *= factor;
      imgH *= factor;
      constrain();
      curPanel.style.backgroundSize = imgW + 'px ' + imgH + 'px';
    };

    // Reset to fit-to-view
    const resetFit = () => {
      const { w: vw, h: vh } = viewSize();
      const vAspect = vw / vh;
      const iAspect = natW / natH;
      if (vAspect > iAspect) {
        imgH = vh; imgW = vh * iAspect;
      } else {
        imgW = vw; imgH = vw / iAspect;
      }
      fitW = imgW; fitH = imgH;
      panX = (vw - imgW) / 2;
      panY = (vh - imgH) / 2;
      fitX = panX; fitY = panY;
      curPanel.style.backgroundSize = imgW + 'px ' + imgH + 'px';
      curPanel.style.backgroundPosition = panX + 'px ' + panY + 'px';
    };

    // Set a panel's image fitted to the viewport (only shows after dimensions are known)
    function setPanelImage(panel, imgItem) {
      if (!imgItem) { panel.style.backgroundImage = ''; return; }
      var url = Viewers._bustUrl(imgItem.href);
      var img = new Image();
      img.src = url;
      var applyFit = function() {
        var nw = img.naturalWidth || img.width;
        var nh = img.naturalHeight || img.height;
        if (!nw || !nh) return;
        var vs = viewSize();
        var vAspect = vs.w / vs.h;
        var iAspect = nw / nh;
        var w, h;
        if (vAspect > iAspect) { h = vs.h; w = vs.h * iAspect; }
        else { w = vs.w; h = vs.w / iAspect; }
        panel.style.backgroundSize = w + 'px ' + h + 'px';
        panel.style.backgroundPosition = ((vs.w - w) / 2) + 'px ' + ((vs.h - h) / 2) + 'px';
        panel.style.backgroundImage = 'url("' + url.replace(/"/g, '\\"') + '")';
      };
      if (img.complete && img.naturalWidth) applyFit();
      else img.onload = applyFit;
    }

    // Update all 3 panels based on current index
    function updatePanels() {
      setPanelImage(panels[0], curIdx > 0 ? imageItems[curIdx - 1] : null);
      setPanelImage(panels[2], curIdx < imageItems.length - 1 ? imageItems[curIdx + 1] : null);
      preloadAdjacent();
    }

    // Initialize once image loads
    const initViewer = () => {
      natW = probe.naturalWidth || probe.width;
      natH = probe.naturalHeight || probe.height;
      if (!natW || !natH) return;
      curPanel.style.backgroundImage = 'url("' + probe.src.replace(/"/g, '\\"') + '")';
      resetFit();
    };

    if (probe.complete && probe.naturalWidth) {
      setTimeout(function() { initViewer(); updatePanels(); }, 0);
    } else {
      probe.onload = function() { initViewer(); updatePanels(); };
    }

    // Adaptive mousewheel zoom (from showfres)
    let wheelCalib = true, wheelCount = 0, wheelAvg = 0;
    let ups = 0.0037, downs = 0.033333333334;
    wrap.addEventListener('wheel', (e) => {
      e.preventDefault();
      const d = Math.abs(e.deltaY);
      if (!wheelAvg) wheelAvg = d;
      else wheelAvg = 0.9 * wheelAvg + 0.1 * d;

      if (wheelCalib) {
        wheelCount++;
        if (wheelCalib === true) {
          wheelCalib = 'x';
          setTimeout(() => {
            wheelCalib = false;
            ups = 0.1111111111 / wheelCount;
            downs = 0.1 / wheelCount;
            if (ups > 0.11111111111) { ups = 0.111111111111; downs = 0.1; }
          }, 50);
        }
      }

      if (wheelAvg === 1) {
        zoom(e.deltaY < 0 ? 1.111111111 : 0.9, e);
      } else {
        if (e.deltaY < 0) zoom(1 + (ups * d / wheelAvg), e);
        else zoom(1 - (downs * d / wheelAvg), e);
      }
    });

    // Mouse drag to pan
    wrap.addEventListener('mousedown', (e) => {
      if (e.target.closest('.imgview-toolbar')) return;
      let lastX = e.pageX, lastY = e.pageY;
      wrap.style.cursor = 'grabbing';
      const onMove = (me) => {
        panX += me.pageX - lastX;
        panY += me.pageY - lastY;
        lastX = me.pageX;
        lastY = me.pageY;
        constrain();
      };
      const onUp = () => {
        wrap.removeEventListener('mousemove', onMove);
        wrap.removeEventListener('mouseup', onUp);
        wrap.style.cursor = '';
      };
      wrap.addEventListener('mousemove', onMove);
      wrap.addEventListener('mouseup', onUp);
      e.preventDefault();
    });

    // Touch support: swipe to navigate (with live drag feedback), drag to pan when zoomed
    var touchStartX = 0, touchStartY = 0, touchStartTime = 0;
    var touchLastX = 0, touchLastY = 0;
    var isTouchPan = false;
    var isSwiping = false;
    var swipeOffset = 0;
    var swipeLocked = false;  // locked to horizontal once direction is determined

    wrap.addEventListener('touchstart', function(e) {
      if (e.touches.length !== 1) return;
      var t = e.touches[0];
      touchStartX = touchLastX = t.clientX;
      touchStartY = touchLastY = t.clientY;
      touchStartTime = Date.now();
      isTouchPan = false;
      isSwiping = false;
      swipeOffset = 0;
      swipeLocked = false;
      track.style.transition = 'none';
    }, { passive: true });

    wrap.addEventListener('touchmove', function(e) {
      if (e.touches.length !== 1) return;
      var t = e.touches[0];
      var isZoomed = (imgW > fitW * 1.05 || imgH > fitH * 1.05);
      if (isZoomed && !isSwiping) {
        panX += t.clientX - touchLastX;
        panY += t.clientY - touchLastY;
        constrain();
        isTouchPan = true;
        touchLastX = t.clientX;
        touchLastY = t.clientY;
        e.preventDefault();
        return;
      }
      // Not zoomed: determine direction then lock
      var totalDx = t.clientX - touchStartX;
      var totalDy = t.clientY - touchStartY;
      if (!swipeLocked && (Math.abs(totalDx) > 10 || Math.abs(totalDy) > 10)) {
        swipeLocked = true;
        isSwiping = Math.abs(totalDx) > Math.abs(totalDy);
      }
      if (isSwiping) {
        // Check if swiping past the edges (no more images in that direction)
        var atStart = curIdx <= 0 && totalDx > 0;
        var atEnd = (curIdx < 0 || curIdx >= imageItems.length - 1) && totalDx < 0;
        if (atStart || atEnd) {
          // Rubber-band: diminishing offset
          swipeOffset = totalDx * 0.3;
        } else {
          swipeOffset = totalDx;
        }
        // Slide the track: base position is -33.3333% (showing center panel)
        var baseOffset = -(wrap.getBoundingClientRect().width);
        track.style.transform = 'translateX(' + (baseOffset + swipeOffset) + 'px)';
        e.preventDefault();
      }
      touchLastX = t.clientX;
      touchLastY = t.clientY;
    }, { passive: false });

    wrap.addEventListener('touchend', function(e) {
      if (isTouchPan) return;
      var threshold = viewSize().w * 0.2;  // 20% of viewport width
      var navigated = false;

      var ww = wrap.getBoundingClientRect().width;
      if (isSwiping && Math.abs(swipeOffset) > threshold) {
        if (swipeOffset < 0 && curIdx < imageItems.length - 1) {
          stopSlideshow();
          navigated = true;
          // Slide track to show next panel (panels[2])
          track.style.transition = 'transform 0.2s ease-out';
          track.style.transform = 'translateX(' + (-ww * 2) + 'px)';
          setTimeout(function() {
            curIdx = curIdx + 1;
            curItem = imageItems[curIdx];
            updateNavState();
            WinManager.setTitle(winId, curItem.name);
            // Rotate panels: move first panel to end
            var first = panels.shift();
            panels.push(first);
            track.appendChild(first);
            curPanel = panels[1];
            // Update natW/natH from cached image
            var tmp = new Image();
            tmp.src = Viewers._bustUrl(curItem.href);
            if (tmp.complete) {
              natW = tmp.naturalWidth; natH = tmp.naturalHeight;
              var sz = curPanel.style.backgroundSize.split(' ');
              imgW = fitW = parseFloat(sz[0]) || imgW;
              imgH = fitH = parseFloat(sz[1]) || imgH;
              var ps = curPanel.style.backgroundPosition.split(' ');
              panX = fitX = parseFloat(ps[0]) || 0;
              panY = fitY = parseFloat(ps[1]) || 0;
            }
            // Snap track back to center (panel order changed, so this shows new curPanel)
            track.style.transition = 'none';
            track.style.transform = 'translateX(' + (-ww) + 'px)';
            void track.offsetHeight;
            // Update the new last panel (next image)
            setPanelImage(panels[2], curIdx < imageItems.length - 1 ? imageItems[curIdx + 1] : null);
            setPanelImage(panels[0], curIdx > 0 ? imageItems[curIdx - 1] : null);
          }, 200);
        } else if (swipeOffset > 0 && curIdx > 0) {
          stopSlideshow();
          navigated = true;
          // Slide track to show prev panel (panels[0])
          track.style.transition = 'transform 0.2s ease-out';
          track.style.transform = 'translateX(0)';
          setTimeout(function() {
            curIdx = curIdx - 1;
            curItem = imageItems[curIdx];
            updateNavState();
            WinManager.setTitle(winId, curItem.name);
            // Rotate panels: move last panel to beginning
            var last = panels.pop();
            panels.unshift(last);
            track.insertBefore(last, track.firstChild);
            curPanel = panels[1];
            var tmp = new Image();
            tmp.src = Viewers._bustUrl(curItem.href);
            if (tmp.complete) {
              natW = tmp.naturalWidth; natH = tmp.naturalHeight;
              var sz = curPanel.style.backgroundSize.split(' ');
              imgW = fitW = parseFloat(sz[0]) || imgW;
              imgH = fitH = parseFloat(sz[1]) || imgH;
              var ps = curPanel.style.backgroundPosition.split(' ');
              panX = fitX = parseFloat(ps[0]) || 0;
              panY = fitY = parseFloat(ps[1]) || 0;
            }
            track.style.transition = 'none';
            track.style.transform = 'translateX(' + (-ww) + 'px)';
            void track.offsetHeight;
            setPanelImage(panels[0], curIdx > 0 ? imageItems[curIdx - 1] : null);
            setPanelImage(panels[2], curIdx < imageItems.length - 1 ? imageItems[curIdx + 1] : null);
          }, 200);
        }
      }

      if (!navigated) {
        // Snap back to center
        track.style.transition = 'transform 0.2s ease-out';
        track.style.transform = 'translateX(' + (-ww) + 'px)';
      }
    });

    // Resize handler — scale relative view with window
    // Track zoom as ratio of image size to fit-to-view size
    let prevVW = 0, prevVH = 0;
    const onResize = () => {
      if (!natW) return;
      const { w: vw, h: vh } = viewSize();
      if (!prevVW || !prevVH) { resetFit(); prevVW = vw; prevVH = vh; return; }
      // Remember zoom ratio relative to old fit size
      var zoomRatio = imgW / fitW;
      // Remember where the viewport center was in image-relative coords
      var relCX = (prevVW / 2 - panX) / imgW;
      var relCY = (prevVH / 2 - panY) / imgH;
      // Recompute fit size for new viewport
      var iAspect = natW / natH;
      var vAspect = vw / vh;
      if (vAspect > iAspect) {
        fitH = vh; fitW = vh * iAspect;
      } else {
        fitW = vw; fitH = vw / iAspect;
      }
      // Apply same zoom ratio to new fit size
      imgW = fitW * zoomRatio;
      imgH = fitH * zoomRatio;
      // Restore viewport center to same relative image point
      panX = vw / 2 - relCX * imgW;
      panY = vh / 2 - relCY * imgH;
      prevVW = vw;
      prevVH = vh;
      constrain();
      wrap.style.backgroundSize = imgW + 'px ' + imgH + 'px';
    };
    const resizeObs = new ResizeObserver(onResize);
    resizeObs.observe(wrap);

    // --- Prev/Next navigation ---
    var imageItems = FileList.items.filter(function(it) { return Viewers.getType(it) === 'image'; });
    var curIdx = imageItems.findIndex(function(it) { return it.href === item.href; });
    var curItem = curIdx >= 0 ? imageItems[curIdx] : item;

    var _preloaded = {};
    function preloadAdjacent() {
      [curIdx - 1, curIdx + 1].forEach(function(i) {
        if (i >= 0 && i < imageItems.length) {
          var url = Viewers._bustUrl(imageItems[i].href);
          if (!_preloaded[url]) {
            var p = new Image();
            p.src = url;
            _preloaded[url] = true;
          }
        }
      });
    }

    function swapImage(newItem, preloadedImg) {
      curItem = newItem;
      curIdx = imageItems.indexOf(newItem);
      updateNavState();
      WinManager.setTitle(winId, newItem.name);
      var img = preloadedImg || new Image();
      if (!preloadedImg) img.src = Viewers._bustUrl(newItem.href);
      var doSwap = function() {
        natW = img.naturalWidth || img.width;
        natH = img.naturalHeight || img.height;
        if (!natW || !natH) return;
        curPanel.style.backgroundImage = 'url("' + img.src.replace(/"/g, '\\"') + '")';
        resetFit();
        updatePanels();
      };
      if (img.complete && img.naturalWidth) doSwap();
      else img.onload = doSwap;
    }

    // --- Slideshow ---
    var slideshowInterval = parseFloat(localStorage.getItem('fm_slideshow_interval') || '5');
    var slideshowOn = false;
    var slideshowRepeat = localStorage.getItem('fm_slideshow_repeat') === 'true';
    var slideshowTimer = null;

    function slideshowTick() {
      if (!slideshowOn) return;
      if (curIdx < imageItems.length - 1) {
        swapImage(imageItems[curIdx + 1]);
        slideshowTimer = setTimeout(slideshowTick, slideshowInterval * 1000);
      } else if (slideshowRepeat && imageItems.length > 1) {
        swapImage(imageItems[0]);
        slideshowTimer = setTimeout(slideshowTick, slideshowInterval * 1000);
      } else {
        slideshowOn = false;
        updatePlayBtn();
      }
    }

    function startSlideshow() {
      slideshowOn = true;
      updatePlayBtn();
      resetHideTimer();
      slideshowTimer = setTimeout(slideshowTick, slideshowInterval * 1000);
    }

    function stopSlideshow() {
      slideshowOn = false;
      updatePlayBtn();
      wrap.classList.remove('cursor-hidden');
      if (slideshowTimer) { clearTimeout(slideshowTimer); slideshowTimer = null; }
    }

    // Toolbar
    const toolbar = document.createElement('div');
    toolbar.className = 'imgview-toolbar';
    const mkBtn = (cls, title, fn) => {
      const b = document.createElement('button');
      b.className = 'imgview-btn';
      b.title = title;
      if (cls) b.innerHTML = '<span class="icon ' + cls + '"></span>';
      b.addEventListener('click', (e) => { e.stopPropagation(); fn(); });
      return b;
    };
    const mkTextBtn = (text, title, fn) => {
      const b = document.createElement('button');
      b.className = 'imgview-btn';
      b.title = title;
      b.textContent = text;
      b.style.cssText = 'font-size:16px;font-weight:bold';
      b.addEventListener('click', (e) => { e.stopPropagation(); fn(); });
      return b;
    };

    // Nav buttons
    var prevBtn = mkTextBtn('\u25C0', 'Previous image (P)', function() {
      if (curIdx > 0) { stopSlideshow(); swapImage(imageItems[curIdx - 1]); }
    });
    var nextBtn = mkTextBtn('\u25B6', 'Next image (N)', function() {
      if (curIdx < imageItems.length - 1) { stopSlideshow(); swapImage(imageItems[curIdx + 1]); }
    });

    // Play/pause button
    var playBtn = mkTextBtn('\u25B6\uFE0E', 'Slideshow (Space)', function() {
      if (slideshowOn) stopSlideshow(); else startSlideshow();
    });

    function updatePlayBtn() {
      playBtn.textContent = slideshowOn ? '\u23F8\uFE0E' : '\u25B6\uFE0E';
      playBtn.title = slideshowOn ? 'Pause slideshow (Space)' : 'Slideshow (Space)';
    }

    // Repeat button
    var repeatBtn = mkTextBtn('\uD83D\uDD01', 'Repeat (R)', function() {
      slideshowRepeat = !slideshowRepeat;
      localStorage.setItem('fm_slideshow_repeat', String(slideshowRepeat));
      updateRepeatBtn();
    });

    function updateRepeatBtn() {
      repeatBtn.style.opacity = slideshowRepeat ? '1' : '0.4';
    }
    updateRepeatBtn();

    // Interval control
    var intervalWrap = document.createElement('span');
    intervalWrap.style.cssText = 'display:inline-flex;align-items:center;gap:2px';
    var intervalDown = document.createElement('button');
    intervalDown.className = 'imgview-btn';
    intervalDown.textContent = '\u2212';
    intervalDown.title = 'Decrease interval (Shift+Down)';
    intervalDown.style.cssText = 'font-size:16px;font-weight:bold;width:28px;height:28px';
    var intervalUp = document.createElement('button');
    intervalUp.className = 'imgview-btn';
    intervalUp.textContent = '+';
    intervalUp.title = 'Increase interval (Shift+Up)';
    intervalUp.style.cssText = 'font-size:16px;font-weight:bold;width:28px;height:28px';
    var intervalLabel = document.createElement('span');
    intervalLabel.style.cssText = 'min-width:42px;text-align:center;color:#fff;font-size:12px;font-variant-numeric:tabular-nums';

    function updateIntervalLabel() {
      intervalLabel.textContent = slideshowInterval.toFixed(1) + 's';
    }
    updateIntervalLabel();

    function changeInterval(delta) {
      slideshowInterval = Math.max(0.5, Math.round((slideshowInterval + delta) * 10) / 10);
      localStorage.setItem('fm_slideshow_interval', String(slideshowInterval));
      updateIntervalLabel();
    }

    intervalDown.addEventListener('click', function(e) { e.stopPropagation(); changeInterval(-0.5); });
    intervalUp.addEventListener('click', function(e) { e.stopPropagation(); changeInterval(0.5); });
    intervalWrap.appendChild(intervalDown);
    intervalWrap.appendChild(intervalLabel);
    intervalWrap.appendChild(intervalUp);

    // Separator
    var sep1 = document.createElement('span');
    sep1.style.cssText = 'width:1px;height:24px;background:rgba(255,255,255,0.25);margin:0 2px';
    var sep2 = document.createElement('span');
    sep2.style.cssText = 'width:1px;height:24px;background:rgba(255,255,255,0.25);margin:0 2px';

    function updateNavState() {
      prevBtn.disabled = curIdx <= 0;
      nextBtn.disabled = curIdx < 0 || curIdx >= imageItems.length - 1;
      playBtn.disabled = imageItems.length < 2;
    }
    updateNavState();

    toolbar.appendChild(prevBtn);
    toolbar.appendChild(playBtn);
    toolbar.appendChild(nextBtn);
    toolbar.appendChild(repeatBtn);
    toolbar.appendChild(sep1);
    toolbar.appendChild(intervalWrap);
    toolbar.appendChild(sep2);
    toolbar.appendChild(mkBtn('icon-zoom-in',  'Zoom in (+)',       () => zoom(1.25)));
    toolbar.appendChild(mkBtn('icon-zoom-out', 'Zoom out (-)',      () => zoom(0.8)));
    toolbar.appendChild(mkBtn('icon-zoom-fit', 'Reset to fit (0)',  () => resetFit()));

    var fullscreenBtn = mkBtn('icon-zoom-fullscreen', 'Fullscreen (F)', function() {
      WinManager.toggleMaximize(winId);
      var w = WinManager.getWindow(winId);
      var isMax = w ? w.maximized : false;
      fullscreenBtn.querySelector('.icon').className = 'icon ' + (isMax ? 'icon-zoom-restore' : 'icon-zoom-fullscreen');
      fullscreenBtn.title = isMax ? 'Restore (F)' : 'Fullscreen (F)';
    });
    toolbar.appendChild(fullscreenBtn);
    wrap.appendChild(toolbar);

    // Auto-hide toolbar after 2s of inactivity; hide cursor too during slideshow
    var hideTimer = null;
    function resetHideTimer() {
      toolbar.classList.remove('toolbar-hidden');
      wrap.classList.remove('cursor-hidden');
      clearTimeout(hideTimer);
      hideTimer = setTimeout(function() {
        toolbar.classList.add('toolbar-hidden');
        if (slideshowOn) wrap.classList.add('cursor-hidden');
      }, 2000);
    }
    wrap.addEventListener('mousemove', resetHideTimer);
    wrap.addEventListener('mousedown', resetHideTimer);
    wrap.addEventListener('touchstart', resetHideTimer);
    resetHideTimer();

    // Download button in header
    const dlBtn = document.createElement('button');
    dlBtn.className = 'btn btn-sm';
    dlBtn.textContent = 'Download';
    dlBtn.addEventListener('click', () => App.downloadFile(curItem));

    // Edit button — only for FIE-supported formats
    var headerActions = [dlBtn];
    var isEditable = /\.(png|jpe?g|webp)$/i.test(item.name);
    if (isEditable) {
      var editBtn = document.createElement('button');
      editBtn.className = 'btn btn-sm';
      editBtn.textContent = 'Edit';
      editBtn.addEventListener('click', function() {
        var editItem = curItem;
        WinManager.close(winId);
        Viewers._openImageEditor(editItem, function(savedItem) {
          Viewers._openImage(savedItem);
        });
      });
      headerActions.unshift(editBtn);
    }

    var winId = WinManager.open(item.name, wrap, {
      type: 'image', full: true, noPadding: true,
      headerActions: headerActions,
      onClose: () => {
        stopSlideshow();
        clearTimeout(hideTimer);
        document.removeEventListener('keydown', onKey);
        var w = WinManager.getWindow(winId);
        if (w) w.body.style.background = '';
        resizeObs.disconnect();
      }
    });

    // Set window body background black so swipe transitions don't show white
    var winObj = WinManager.getWindow(winId);
    if (winObj) winObj.body.style.background = '#000';

    // Keyboard handler
    const onKey = (e) => {
      var w = WinManager.getWindow(winId);
      if (!w || w.minimized) return;
      var focused = WinManager.getFocusedWindow();
      if (!focused || focused.id !== winId) return;
      const k = e.key;
      switch (k) {
        case '+': case '=': zoom(1.111111111); break;
        case '-': case '_': zoom(0.9); break;
        case '0': case '1': resetFit(); break;
        case 'ArrowLeft':
          if (curIdx > 0) { stopSlideshow(); swapImage(imageItems[curIdx - 1]); }
          break;
        case 'ArrowRight':
          if (curIdx < imageItems.length - 1) { stopSlideshow(); swapImage(imageItems[curIdx + 1]); }
          break;
        case 'ArrowUp':
          if (e.shiftKey) { changeInterval(0.5); }
          else { panY += 40; constrain(); }
          break;
        case 'ArrowDown':
          if (e.shiftKey) { changeInterval(-0.5); }
          else { panY -= 40; constrain(); }
          break;
        case ' ':
          if (slideshowOn) stopSlideshow(); else startSlideshow();
          break;
        case 'p': case 'P':
          if (curIdx > 0) { stopSlideshow(); swapImage(imageItems[curIdx - 1]); }
          break;
        case 'n': case 'N':
          if (curIdx < imageItems.length - 1) { stopSlideshow(); swapImage(imageItems[curIdx + 1]); }
          break;
        case 'r': case 'R':
          slideshowRepeat = !slideshowRepeat;
          localStorage.setItem('fm_slideshow_repeat', String(slideshowRepeat));
          updateRepeatBtn();
          break;
        case 'f': case 'F':
          fullscreenBtn.click();
          break;
        default: return;
      }
      e.preventDefault();
      e.stopPropagation();
    };
    document.addEventListener('keydown', onKey);
  },

  _openVideo(item, autoplay) {
    if (!autoplay && localStorage.getItem('fm_video_autoplay') === 'true') autoplay = true;
    const mimeMap = { mp4: 'video/mp4', webm: 'video/webm', ogg: 'video/ogg', ogv: 'video/ogg', mkv: 'video/x-matroska', avi: 'video/x-msvideo', mov: 'video/quicktime' };

    function getVideoMime(name) {
      var ext = name.split('.').pop().toLowerCase();
      return mimeMap[ext] || 'video/' + ext;
    }

    // --- Subtitle helpers (stable across source swaps) ---
    function parseVtt(text) {
      var cues = [];
      var blocks = text.replace(/\r\n/g, '\n').split(/\n\n+/);
      for (var i = 0; i < blocks.length; i++) {
        var m = blocks[i].match(/(\d{2}:\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[.,]\d{3})(.*)/);
        if (!m) continue;
        var lineEnd = blocks[i].indexOf('\n', blocks[i].indexOf('-->'));
        var txt = lineEnd !== -1 ? blocks[i].substring(lineEnd + 1) : '';
        cues.push({start: parseTs(m[1]), end: parseTs(m[2]), text: txt.trim()});
      }
      return cues;
    }

    function parseTs(s) {
      var p = s.replace(',', '.').split(':');
      return (+p[0]) * 3600 + (+p[1]) * 60 + parseFloat(p[2]);
    }

    function formatTs(sec) {
      if (sec < 0) sec = 0;
      var h = Math.floor(sec / 3600);
      var m = Math.floor((sec % 3600) / 60);
      var s = (sec % 60).toFixed(3);
      if (s < 10) s = '0' + s;
      return (h < 10 ? '0' : '') + h + ':' + (m < 10 ? '0' : '') + m + ':' + s;
    }

    // --- Mutable state ---
    var curItem = item;
    var subCues = [];
    var subBlobUrl = null;
    var subTrackEl = null;
    var subOffset = 0;
    var player = null;

    // --- Build DOM (once) ---
    const wrap = document.createElement('div');
    wrap.className = 'video-viewer';

    const video = document.createElement('video');
    video.className = 'video-js vjs-big-play-centered';
    video.controls = true;
    video.preload = 'auto';

    const source = document.createElement('source');
    source.src = item.href;
    source.type = getVideoMime(item.name);
    video.appendChild(source);
    wrap.appendChild(video);

    // --- Subtitle track management ---
    function clearSubs() {
      if (subTrackEl) {
        if (player) {
          try { player.removeRemoteTextTrack(subTrackEl); } catch(e) {}
        }
        if (subTrackEl.parentNode) subTrackEl.parentNode.removeChild(subTrackEl);
        subTrackEl = null;
      }
      if (subBlobUrl) { URL.revokeObjectURL(subBlobUrl); subBlobUrl = null; }
      subCues = [];
      subOffset = 0;
      subLabel.textContent = 'Sub: 0.0s';
      subSyncWrap.style.display = 'none';
    }

    function buildSubTrack(offset) {
      if (!subCues.length) return;
      // Remove existing track
      if (subTrackEl) {
        if (player) {
          try { player.removeRemoteTextTrack(subTrackEl); } catch(e) {}
        }
        if (subTrackEl.parentNode) subTrackEl.parentNode.removeChild(subTrackEl);
        subTrackEl = null;
      }
      if (subBlobUrl) URL.revokeObjectURL(subBlobUrl);
      var lines = ['WEBVTT', ''];
      for (var i = 0; i < subCues.length; i++) {
        var c = subCues[i];
        lines.push(formatTs(c.start + offset) + ' --> ' + formatTs(c.end + offset));
        lines.push(c.text);
        lines.push('');
      }
      var blob = new Blob([lines.join('\n')], {type: 'text/vtt'});
      subBlobUrl = URL.createObjectURL(blob);
      if (player) {
        subTrackEl = player.addRemoteTextTrack({kind: 'subtitles', label: 'Subtitles', src: subBlobUrl, default: true}, false);
        var tt = subTrackEl.track || subTrackEl;
        tt.mode = 'showing';
      } else {
        subTrackEl = document.createElement('track');
        subTrackEl.kind = 'subtitles';
        subTrackEl.label = 'Subtitles';
        subTrackEl.src = subBlobUrl;
        subTrackEl.default = true;
        video.appendChild(subTrackEl);
        subTrackEl.track.mode = 'showing';
      }
    }

    function loadSubsForItem(it) {
      var baseHref = it.href.replace(/\.[^.]+$/, '');
      fetch(baseHref + '.vtt', {method: 'HEAD', credentials: 'same-origin'}).then(function(r) {
        if (r.ok) {
          return fetch(baseHref + '.vtt', {credentials: 'same-origin'}).then(function(r2) {
            return r2.text();
          }).then(function(text) {
            if (it !== curItem) return; // stale
            subCues = parseVtt(text);
            if (subCues.length) { buildSubTrack(0); subSyncWrap.style.display = 'inline-flex'; }
          });
        } else {
          return fetch(baseHref + '.srt', {method: 'HEAD', credentials: 'same-origin'}).then(function(r2) {
            if (!r2.ok) return;
            return fetch(baseHref + '.srt', {credentials: 'same-origin'}).then(function(r3) {
              return r3.text();
            }).then(function(text) {
              if (it !== curItem) return;
              subCues = parseVtt(text);
              if (subCues.length) { buildSubTrack(0); subSyncWrap.style.display = 'inline-flex'; }
            });
          });
        }
      }).catch(function() {});
    }

    // --- Header buttons ---
    const dlBtn = document.createElement('button');
    dlBtn.className = 'btn btn-sm';
    dlBtn.textContent = 'Download';
    dlBtn.addEventListener('click', () => App.downloadFile(curItem));

    // Subtitle sync controls
    var subSyncWrap = document.createElement('span');
    subSyncWrap.style.cssText = 'display:none;align-items:center;gap:2px;font-size:12px';
    var subDelayBtn = document.createElement('button');
    subDelayBtn.className = 'btn btn-sm';
    subDelayBtn.textContent = '\u2212';
    subDelayBtn.title = 'Subtitle earlier (H)';
    var subAdvBtn = document.createElement('button');
    subAdvBtn.className = 'btn btn-sm';
    subAdvBtn.textContent = '+';
    subAdvBtn.title = 'Subtitle later (G)';
    var subLabel = document.createElement('span');
    subLabel.style.cssText = 'min-width:56px;text-align:center;color:var(--color-fg-secondary);font-variant-numeric:tabular-nums';
    subLabel.textContent = 'Sub: 0.0s';

    function shiftSubs(delta) {
      subOffset += delta;
      subLabel.textContent = 'Sub: ' + (subOffset >= 0 ? '+' : '') + subOffset.toFixed(1) + 's';
      buildSubTrack(subOffset);
    }

    subDelayBtn.addEventListener('click', function() { shiftSubs(-0.5); });
    subAdvBtn.addEventListener('click', function() { shiftSubs(0.5); });
    subSyncWrap.appendChild(subDelayBtn);
    subSyncWrap.appendChild(subLabel);
    subSyncWrap.appendChild(subAdvBtn);

    // Prev/Next navigation
    var videoItems = FileList.items.filter(function(it) { return Viewers.getType(it) === 'video'; });
    var curIdx = videoItems.findIndex(function(it) { return it.href === item.href; });

    var prevBtn = document.createElement('button');
    prevBtn.className = 'btn btn-sm';
    prevBtn.textContent = '\u25C0';
    prevBtn.title = 'Previous video';

    var nextBtn = document.createElement('button');
    nextBtn.className = 'btn btn-sm';
    nextBtn.textContent = '\u25B6';
    nextBtn.title = 'Next video';

    var autoplayOn = localStorage.getItem('fm_video_autoplay') === 'true';
    var autoplayBtn = document.createElement('button');
    autoplayBtn.className = 'btn btn-sm';
    autoplayBtn.title = 'Auto-play next video';

    function updateNavState() {
      prevBtn.disabled = curIdx <= 0;
      nextBtn.disabled = curIdx < 0 || curIdx >= videoItems.length - 1;
      autoplayBtn.disabled = false;
    }

    var updateAutoplayBtn = function() {
      autoplayBtn.textContent = autoplayOn ? '\u27F3 On' : '\u27F3 Off';
      autoplayBtn.style.opacity = autoplayOn ? '1' : '';
    };
    updateAutoplayBtn();
    updateNavState();

    autoplayBtn.addEventListener('click', function() {
      autoplayOn = !autoplayOn;
      localStorage.setItem('fm_video_autoplay', String(autoplayOn));
      updateAutoplayBtn();
    });

    // --- Source swap (preserves dialog/fullscreen) ---
    function swapTo(newItem, shouldPlay) {
      curItem = newItem;
      curIdx = videoItems.indexOf(newItem);
      clearSubs();
      updateNavState();
      WinManager.setTitle(winId, newItem.name);

      if (player) {
        if (shouldPlay) player.autoplay(true);
        // doesn't seem to help
        //player.reset();
        player.src({src: newItem.href, type: getVideoMime(newItem.name)});
        // Explicit play on loadstart — some browsers (e.g. Firefox) don't
        // honor autoplay(true) after a source change.
        if (shouldPlay) {
          player.one('loadstart', function() {
            player.play().catch(function(){});
          });
        }
        if (!shouldPlay) player.autoplay(false);
      } else {
        video.autoplay = !!shouldPlay;
        video.src = newItem.href;
        video.load();
      }
      loadSubsForItem(newItem);
    }

    prevBtn.addEventListener('click', function() {
      if (curIdx > 0) swapTo(videoItems[curIdx - 1], true);
    });
    nextBtn.addEventListener('click', function() {
      if (curIdx < videoItems.length - 1) swapTo(videoItems[curIdx + 1], true);
    });

    // Auto-advance on ended
    video.addEventListener('ended', function() {
      if (autoplayOn && curIdx >= 0 && curIdx < videoItems.length - 1) {
        swapTo(videoItems[curIdx + 1], true);
      }
    });

    var winId = WinManager.open(item.name, wrap, {
      type: 'video', singleton: true,
      full: true,
      noPadding: true,
      headerActions: [subSyncWrap, prevBtn, nextBtn, autoplayBtn, dlBtn],
      onClose: () => {
        if (player && player.dispose) {
          try { player.dispose(); } catch (e) {}
        } else {
          video.pause();
        }
      }
    });

    // --- Keyboard controls ---
    var seekAccum = 0;
    var seekTimer = null;
    wrap.tabIndex = -1;
    wrap.addEventListener('keydown', (e) => {
      if (e.key === 'g' || e.key === 'G') { shiftSubs(0.5); e.preventDefault(); return; }
      if (e.key === 'h' || e.key === 'H') { shiftSubs(-0.5); e.preventDefault(); return; }
      if (e.key === ' ') {
        e.preventDefault();
        e.stopPropagation();
        var el = player ? player.el().querySelector('video') : video;
        if (el) { el.paused ? el.play() : el.pause(); }
        return;
      }
      if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
        e.preventDefault();
        e.stopPropagation();
        var el = player ? player.el().querySelector('video') : video;
        if (el) {
          el.volume = Math.max(0, Math.min(1, el.volume + (e.key === 'ArrowUp' ? 0.05 : -0.05)));
          if (el.muted && e.key === 'ArrowUp') el.muted = false;
        }
        return;
      }
      if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
      e.preventDefault();
      e.stopPropagation();
      var skip = e.ctrlKey && e.shiftKey ? 60 : e.ctrlKey ? 30 : 10;
      if (e.key === 'ArrowLeft') skip = -skip;
      seekAccum += skip;
      if (!seekTimer) {
        seekTimer = setTimeout(() => {
          var el = player ? player.el().querySelector('video') : video;
          if (el && el.duration) {
            el.currentTime = Math.max(0, Math.min(el.duration, el.currentTime + seekAccum));
          }
          seekAccum = 0;
          seekTimer = null;
        }, 200);
      }
    });
    // --- Mouse wheel: vertical = volume, horizontal = seek ---
    var wheelSeekAccum = 0;
    var wheelSeekTimer = null;
    wrap.addEventListener('wheel', function(e) {
      e.preventDefault();
      var el = player ? player.el().querySelector('video') : video;
      if (!el) return;

      if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        // Horizontal scroll — seek, scaled by delta magnitude
        // Mouse wheels give large deltas (~100+), trackpads give small ones (~2-10)
        // Scale so ~120 delta = 15 seconds, cap accumulation at ±60s per window
        var skip = (e.deltaX / 8);
        wheelSeekAccum += skip;
        wheelSeekAccum = Math.max(-60, Math.min(60, wheelSeekAccum));
        if (!wheelSeekTimer) {
          wheelSeekTimer = setTimeout(function() {
            if (el.duration) {
              el.currentTime = Math.max(0, Math.min(el.duration, el.currentTime + wheelSeekAccum));
            }
            wheelSeekAccum = 0;
            wheelSeekTimer = null;
          }, 400);
        }
      } else {
        // Vertical scroll — volume
        var delta = e.deltaY > 0 ? -0.05 : 0.05;
        el.volume = Math.max(0, Math.min(1, el.volume + delta));
        if (el.muted && delta > 0) el.muted = false;
      }
    }, { passive: false });

    wrap.focus();

    // --- Initialize video.js ---
    loadSubsForItem(item);

    this._loadVideoJs().then(() => {
      if (window.videojs) {
        player = window.videojs(video, { fill: true, autoplay: !!autoplay });

        // Add maximize/minimize and close buttons to the video.js control bar
        const controlBar = player.controlBar.el();

        const vjsMaxBtn = document.createElement('button');
        vjsMaxBtn.className = 'vjs-control vjs-button vjs-maximize-btn';
        vjsMaxBtn.title = 'Maximize';
        const syncMaxBtn = () => {
          var w = WinManager.getWindow(winId);
          var isMax = w ? w.maximized : false;
          vjsMaxBtn.classList.toggle('vjs-restore', isMax);
          vjsMaxBtn.title = isMax ? 'Restore' : 'Maximize';
        };
        vjsMaxBtn.addEventListener('click', () => {
          WinManager.toggleMaximize(winId);
          syncMaxBtn();
        });
        var winState = WinManager.getWindow(winId);
        if (winState) winState.maxBtn.addEventListener('click', syncMaxBtn);
        controlBar.appendChild(vjsMaxBtn);

        const closeBtn = document.createElement('button');
        closeBtn.className = 'vjs-control vjs-button vjs-close-btn';
        closeBtn.title = 'Close';
        closeBtn.addEventListener('click', () => WinManager.close(winId));
        controlBar.appendChild(closeBtn);

        player.on('useractive', () => { wrap.style.cursor = ''; });
        player.on('userinactive', () => {
          if (!player.paused()) wrap.style.cursor = 'none';
        });

        // Touch: tap video to toggle controls (scoped to player element only)
        // TODO: video controls on mobile need fixing — controls are off-screen

        // Re-hook ended event on the video.js managed element
        player.on('ended', function() {
          if (autoplayOn && curIdx >= 0 && curIdx < videoItems.length - 1) {
            swapTo(videoItems[curIdx + 1], true);
          }
        });
      } else {
        if (autoplay) {
          video.autoplay = true;
          video.play().catch(() => {});
        }
      }
    }).catch(() => {
      if (autoplay) {
        video.autoplay = true;
        video.play().catch(() => {});
      }
    });
  },

  _openAudio(item, itemList, listTitle, sourceDir) {
    var audioItems;
    if (itemList) {
      audioItems = itemList;
    } else {
      audioItems = FileList.items.filter(function(it) { return Viewers.getType(it) === 'audio'; });
    }
    var startIdx = audioItems.indexOf(item);
    if (startIdx < 0) startIdx = 0;

    var audioMimes = { mp3: 'audio/mpeg', wav: 'audio/wav', ogg: 'audio/ogg', oga: 'audio/ogg', flac: 'audio/flac', aac: 'audio/aac', m4a: 'audio/mp4', opus: 'audio/opus', wma: 'audio/x-ms-wma' };

    // Build songs array for Amplitude
    var songs = audioItems.map(function(it) {
      var ext = it.name.split('.').pop().toLowerCase();
      return { name: it.name, url: it.href, _item: it, _mime: audioMimes[ext] || 'audio/' + ext };
    });

    // Build player UI
    var wrap = document.createElement('div');
    wrap.className = 'amp-player';

    wrap.innerHTML =
      '<div class="amp-left">' +
        '<div class="amp-art-wrap"><img class="amp-art" src="" alt=""></div>' +
        '<div class="amp-track-info">' +
          '<div class="amp-track-name"></div>' +
          '<div class="amp-track-artist"></div>' +
          '<div class="amp-track-album"></div>' +
          '<div class="amp-track-location"></div>' +
        '</div>' +
      '</div>' +
      '<div class="amp-right">' +
        '<div class="amp-controls">' +
          '<button class="amp-btn amp-prev" title="Previous">&#9664;&#9664;</button>' +
          '<button class="amp-btn amp-play" title="Play/Pause">&#9654;</button>' +
          '<button class="amp-btn amp-next" title="Next">&#9654;&#9654;</button>' +
          '<div class="amp-time"><span class="amp-cur">0:00</span> / <span class="amp-dur">0:00</span></div>' +
          '<button class="amp-btn amp-shuffle-btn" title="Shuffle">&#8645;</button>' +
          '<button class="amp-btn amp-repeat-btn" title="Repeat">&#8635;</button>' +
          '<button class="amp-btn amp-mute-btn" title="Mute">&#128264;</button>' +
          '<input type="range" class="amp-volume" min="0" max="100" value="100">' +
        '</div>' +
        '<div class="amp-progress-wrap">' +
          '<div class="amp-progress"><div class="amp-progress-fill"></div></div>' +
        '</div>' +
        '<div class="amp-playlist"></div>' +
      '</div>';

    var artImg = wrap.querySelector('.amp-art');
    var artWrap = wrap.querySelector('.amp-art-wrap');
    var trackName = wrap.querySelector('.amp-track-name');
    var trackArtist = wrap.querySelector('.amp-track-artist');
    var trackAlbum = wrap.querySelector('.amp-track-album');
    var trackLocation = wrap.querySelector('.amp-track-location');
    trackLocation.addEventListener('click', function() {
      var dir = trackLocation.dataset.dir;
      var file = trackLocation.dataset.file;
      if (!dir) return;
      FileList.navigate(dir, true).then(function() {
        if (file) {
          FileList.selected.clear();
          FileList.selected.add(file);
          FileList._updateSelectionBar();
          FileList._updateSelectionVisual();
          // Scroll selected item into view
          var sel = FileList._container.querySelector('[data-href="' + file + '"]');
          if (sel) sel.scrollIntoView({ block: 'nearest' });
        }
      });
    });
    var playBtn = wrap.querySelector('.amp-play');
    var prevBtn = wrap.querySelector('.amp-prev');
    var nextBtn = wrap.querySelector('.amp-next');
    var shuffleBtn = wrap.querySelector('.amp-shuffle-btn');
    var repeatBtn = wrap.querySelector('.amp-repeat-btn');
    var muteBtn = wrap.querySelector('.amp-mute-btn');
    var volSlider = wrap.querySelector('.amp-volume');
    var curTime = wrap.querySelector('.amp-cur');
    var durTime = wrap.querySelector('.amp-dur');
    var progressWrap = wrap.querySelector('.amp-progress-wrap');
    var progressFill = wrap.querySelector('.amp-progress-fill');
    var playlistEl = wrap.querySelector('.amp-playlist');

    // State
    var audio = document.createElement('audio');
    audio.preload = 'auto';
    wrap.appendChild(audio);
    var curIdx = startIdx;
    var playing = false;
    var shuffleOn = localStorage.getItem('fm_audio_shuffle') === 'true';
    var repeatMode = parseInt(localStorage.getItem('fm_audio_repeat')) || 0;
    var artUrlCache = {}; // index -> blob URL
    var metaCache = {}; // index -> {title, artist, album}
    var seeking = false;
    var shuffleQueue = [];
    var shuffleHistory = [];

    function fmtTime(s) {
      if (!s || !isFinite(s)) return '0:00';
      s = Math.floor(s);
      var m = Math.floor(s / 60);
      var sec = s % 60;
      return m + ':' + (sec < 10 ? '0' : '') + sec;
    }

    function updateNowPlaying() {
      var song = songs[curIdx];
      var meta = metaCache[curIdx];
      trackName.textContent = (meta && meta.title) || song.name;
      trackArtist.textContent = (meta && meta.artist) || '';
      trackAlbum.textContent = (meta && meta.album) || '';
      var loc = decodeURIComponent(song.url.substring(0, song.url.lastIndexOf('/')));
      trackLocation.textContent = '\u2066' + loc + '\u2069';
      var fullPath = decodeURIComponent(song.url.replace(/\/$/, '')).replace(/^\/dav\//, '/');
      trackLocation.title = '\u2066' + fullPath + '\u2069';
      trackLocation.dataset.dir = song.url.substring(0, song.url.lastIndexOf('/') + 1);
      trackLocation.dataset.file = song.url;
      WinManager.setTitle(winId, (meta && meta.title) || song.name);

      if (artUrlCache[curIdx]) {
        artImg.src = artUrlCache[curIdx];
        artWrap.classList.add('has-art');
      } else {
        artImg.src = '';
        artWrap.classList.remove('has-art');
      }

      // Highlight in playlist
      var items = playlistEl.querySelectorAll('.amp-pl-item');
      for (var i = 0; i < items.length; i++) {
        items[i].classList.toggle('active', i === curIdx);
      }

      updateNavState();
    }

    function updateNavState() {
      if (shuffleOn) {
        prevBtn.disabled = false;
        nextBtn.disabled = false;
      } else {
        prevBtn.disabled = curIdx <= 0;
        nextBtn.disabled = curIdx >= songs.length - 1;
      }
    }

    function updatePlayBtn() {
      playBtn.innerHTML = playing ? '&#9646;&#9646;' : '&#9654;';
      playBtn.title = playing ? 'Pause' : 'Play';
    }

    function loadTrack(idx, autoplay) {
      // Skip missing tracks
      if (isMissing(idx)) { playNext(); return; }
      curIdx = idx;
      // Use <source> element with MIME type for better browser compatibility
      audio.innerHTML = '';
      var source = document.createElement('source');
      source.src = songs[idx].url;
      source.type = songs[idx]._mime || '';
      audio.appendChild(source);
      audio.load();
      updateNowPlaying();
      extractMeta(idx);
      if (autoplay) {
        var tryPlay = function() {
          audio.removeEventListener('canplay', tryPlay);
          var p = audio.play();
          if (p && p.catch) p.catch(function() {});
          playing = true;
          updatePlayBtn();
        };
        if (audio.readyState >= 3) {
          tryPlay();
        } else {
          audio.addEventListener('canplay', tryPlay);
        }
      }
    }

    function buildShuffleQueue() {
      shuffleQueue = [];
      for (var i = 0; i < songs.length; i++) {
        if (i !== curIdx && !isMissing(i)) shuffleQueue.push(i);
      }
      // Fisher-Yates shuffle
      for (var j = shuffleQueue.length - 1; j > 0; j--) {
        var k = Math.floor(Math.random() * (j + 1));
        var tmp = shuffleQueue[j];
        shuffleQueue[j] = shuffleQueue[k];
        shuffleQueue[k] = tmp;
      }
    }

    function isMissing(idx) { return audioItems[idx] && audioItems[idx]._missing; }

    function playNext() {
      if (repeatMode === 2 && !isMissing(curIdx)) {
        audio.currentTime = 0;
        (function(p){if(p&&p.catch)p.catch(function(){});})(audio.play());
        return;
      }
      var next;
      if (shuffleOn) {
        shuffleHistory.push(curIdx);
        if (shuffleQueue.length === 0) {
          if (repeatMode === 1) { buildShuffleQueue(); }
          else return;
        }
        if (shuffleQueue.length === 0) return;
        next = shuffleQueue.shift();
        // Skip missing in shuffle
        var attempts = 0;
        while (isMissing(next) && attempts < songs.length) {
          if (shuffleQueue.length === 0) {
            if (repeatMode === 1) buildShuffleQueue();
            else return;
          }
          if (shuffleQueue.length === 0) return;
          next = shuffleQueue.shift();
          attempts++;
        }
        if (isMissing(next)) return;
      } else {
        next = curIdx + 1;
        // Skip missing tracks
        while (next < songs.length && isMissing(next)) next++;
        if (next >= songs.length) {
          if (repeatMode === 1) {
            next = 0;
            while (next < songs.length && isMissing(next)) next++;
            if (next >= songs.length || isMissing(next)) return;
          } else return;
        }
      }
      loadTrack(next, true);
    }

    function playPrev() {
      if (audio.currentTime > 3) {
        audio.currentTime = 0;
        return;
      }
      if (shuffleOn) {
        if (shuffleHistory.length > 0) {
          shuffleQueue.unshift(curIdx);
          var prev = shuffleHistory.pop();
          while (isMissing(prev) && shuffleHistory.length > 0) {
            prev = shuffleHistory.pop();
          }
          if (!isMissing(prev)) loadTrack(prev, true);
        } else {
          audio.currentTime = 0;
        }
        return;
      }
      var prev = curIdx - 1;
      while (prev >= 0 && isMissing(prev)) prev--;
      if (prev < 0) {
        if (repeatMode === 1) {
          prev = songs.length - 1;
          while (prev >= 0 && isMissing(prev)) prev--;
          if (prev < 0) return;
        } else { audio.currentTime = 0; return; }
      }
      loadTrack(prev, true);
    }

    // Extract ID3 metadata via jsmediatags
    function extractMeta(idx) {
      if (metaCache[idx] !== undefined) return;
      metaCache[idx] = null; // mark as loading
      if (!window.jsmediatags) return;
      window.jsmediatags.read(window.location.origin + songs[idx].url, {
        onSuccess: function(tag) {
          var t = tag.tags || {};
          metaCache[idx] = { title: t.title || '', artist: t.artist || '', album: t.album || '' };
          // Extract artwork
          var pic = t.picture;
          if (pic && pic.data && pic.format) {
            var bytes = new Uint8Array(pic.data);
            var blob = new Blob([bytes], { type: pic.format });
            artUrlCache[idx] = URL.createObjectURL(blob);
          }
          if (idx === curIdx) updateNowPlaying();
          // Update playlist entry
          updatePlaylistEntry(idx);
        },
        onError: function() {
          metaCache[idx] = {};
        }
      });
    }

    function updatePlaylistEntry(idx) {
      var el = playlistEl.children[idx];
      if (!el) return;
      var meta = metaCache[idx];
      var title = (meta && meta.title) || songs[idx].name;
      var artist = (meta && meta.artist) || '';
      el.querySelector('.amp-pl-title').textContent = title;
      el.querySelector('.amp-pl-artist').textContent = artist;
    }

    // Build playlist
    function rebuildPlaylist() {
      playlistEl.innerHTML = '';
      for (var i = 0; i < songs.length; i++) {
        var plItem = document.createElement('div');
        var trackMissing = isMissing(i);
        plItem.className = 'amp-pl-item' + (i === curIdx ? ' active' : '') + (trackMissing ? ' amp-pl-missing' : '');
        plItem.draggable = !trackMissing;
        plItem.dataset.idx = i;
        var displayName = (audioItems[i]._plTitle || songs[i].name).replace(/</g, '&lt;');
        plItem.innerHTML = '<span class="amp-pl-grip">&#9776;</span>' +
          '<span class="amp-pl-num">' + (i + 1) + '</span>' +
          '<span class="amp-pl-title">' + displayName + '</span>' +
          '<span class="amp-pl-artist"></span>' +
          '<button class="amp-pl-remove" data-idx="' + i + '" title="Remove">&times;</button>';
        if (!trackMissing) {
          plItem.addEventListener('click', (function(idx) {
            return function(e) {
              if (e.target.closest('.amp-pl-remove') || e.target.closest('.amp-pl-grip')) return;
              loadTrack(idx, true);
            };
          })(i));
        }
        plItem.addEventListener('dragstart', (function(idx) {
          return function(e) {
            e.dataTransfer.setData('text/x-amp-reorder', idx);
            e.dataTransfer.effectAllowed = 'move';
          };
        })(i));
        playlistEl.appendChild(plItem);
      }
      // Wire remove buttons
      var removeBtns = playlistEl.querySelectorAll('.amp-pl-remove');
      for (var r = 0; r < removeBtns.length; r++) {
        removeBtns[r].addEventListener('click', (function(idx) {
          return function(e) {
            e.stopPropagation();
            var wasPlaying = (idx === curIdx && playing);
            songs.splice(idx, 1);
            audioItems.splice(idx, 1);
            // Fix curIdx
            if (idx < curIdx) curIdx--;
            else if (idx === curIdx) {
              if (wasPlaying) { audio.pause(); playing = false; updatePlayBtn(); }
              if (curIdx >= songs.length) curIdx = songs.length - 1;
              if (curIdx >= 0 && wasPlaying) loadTrack(curIdx, true);
            }
            // Rebuild caches
            var newArt = {}, newMeta = {};
            for (var ci = 0; ci < songs.length; ci++) {
              var oldIdx = ci >= idx ? ci + 1 : ci;
              if (artUrlCache[oldIdx] !== undefined) newArt[ci] = artUrlCache[oldIdx];
              if (metaCache[oldIdx] !== undefined) newMeta[ci] = metaCache[oldIdx];
            }
            artUrlCache = newArt;
            metaCache = newMeta;
            rebuildPlaylist();
            updateNavState();
          };
        })(parseInt(removeBtns[r].dataset.idx)));
      }
    }
    rebuildPlaylist();

    // Drag reorder in playlist
    playlistEl.addEventListener('dragover', function(e) {
      if (!e.dataTransfer.types.includes('text/x-amp-reorder')) return;
      e.preventDefault();
      var target = e.target.closest('.amp-pl-item');
      if (!target) return;
      // Clear all indicators first
      var all = playlistEl.querySelectorAll('.amp-pl-item');
      for (var a = 0; a < all.length; a++) { all[a].classList.remove('drag-above', 'drag-below'); }
      var rect = target.getBoundingClientRect();
      var mid = rect.top + rect.height / 2;
      target.classList.toggle('drag-above', e.clientY < mid);
      target.classList.toggle('drag-below', e.clientY >= mid);
    });
    playlistEl.addEventListener('dragleave', function(e) {
      if (!playlistEl.contains(e.relatedTarget)) {
        var all = playlistEl.querySelectorAll('.amp-pl-item');
        for (var a = 0; a < all.length; a++) { all[a].classList.remove('drag-above', 'drag-below'); }
      }
    });
    playlistEl.addEventListener('drop', function(e) {
      e.preventDefault();
      var all = playlistEl.querySelectorAll('.amp-pl-item');
      for (var a = 0; a < all.length; a++) { all[a].classList.remove('drag-above', 'drag-below'); }
      var fromIdx = parseInt(e.dataTransfer.getData('text/x-amp-reorder'));
      if (isNaN(fromIdx)) return;
      var target = e.target.closest('.amp-pl-item');
      if (!target) return;
      var toIdx = parseInt(target.dataset.idx);
      var rect = target.getBoundingClientRect();
      if (e.clientY >= rect.top + rect.height / 2) toIdx++;
      if (fromIdx === toIdx || fromIdx + 1 === toIdx) return;
      // Move in arrays
      var movedSong = songs.splice(fromIdx, 1)[0];
      var movedItem = audioItems.splice(fromIdx, 1)[0];
      if (toIdx > fromIdx) toIdx--;
      songs.splice(toIdx, 0, movedSong);
      audioItems.splice(toIdx, 0, movedItem);
      // Fix curIdx
      if (curIdx === fromIdx) { curIdx = toIdx; }
      else if (fromIdx < curIdx && toIdx >= curIdx) { curIdx--; }
      else if (fromIdx > curIdx && toIdx <= curIdx) { curIdx++; }
      // Rebuild caches with new indices
      var newArt = {}, newMeta = {};
      for (var ci = 0; ci < songs.length; ci++) {
        // Find old index — this is complex, just re-extract
      }
      artUrlCache = {};
      metaCache = {};
      rebuildPlaylist();
      updateNavState();
    });

    // Controls
    playBtn.addEventListener('click', function() {
      if (playing) { audio.pause(); playing = false; }
      else { (function(p){if(p&&p.catch)p.catch(function(){});})(audio.play()); playing = true; }
      updatePlayBtn();
    });

    prevBtn.addEventListener('click', function() { playPrev(); });
    nextBtn.addEventListener('click', function() { playNext(); });

    // Apply saved shuffle/repeat state to UI
    if (shuffleOn) {
      buildShuffleQueue();
      shuffleBtn.classList.add('active');
    }
    if (repeatMode > 0) {
      repeatBtn.classList.add('active');
      if (repeatMode === 2) repeatBtn.innerHTML = '&#8635;1';
      repeatBtn.title = repeatMode === 1 ? 'Repeat All' : 'Repeat One';
    }

    shuffleBtn.addEventListener('click', function() {
      shuffleOn = !shuffleOn;
      localStorage.setItem('fm_audio_shuffle', shuffleOn);
      if (shuffleOn) {
        buildShuffleQueue();
        shuffleHistory = [];
      }
      shuffleBtn.classList.toggle('active', shuffleOn);
      updateNavState();
    });

    repeatBtn.addEventListener('click', function() {
      repeatMode = (repeatMode + 1) % 3;
      localStorage.setItem('fm_audio_repeat', repeatMode);
      repeatBtn.classList.toggle('active', repeatMode > 0);
      repeatBtn.innerHTML = repeatMode === 2 ? '&#8635;1' : '&#8635;';
      repeatBtn.title = repeatMode === 0 ? 'Repeat' : repeatMode === 1 ? 'Repeat All' : 'Repeat One';
    });

    muteBtn.addEventListener('click', function() {
      audio.muted = !audio.muted;
      muteBtn.innerHTML = audio.muted ? '&#128263;' : '&#128264;';
    });

    volSlider.addEventListener('input', function() {
      audio.volume = this.value / 100;
      audio.muted = false;
      muteBtn.innerHTML = audio.volume === 0 ? '&#128263;' : '&#128264;';
    });

    // Progress bar click-to-seek
    progressWrap.addEventListener('mousedown', function(e) {
      seeking = true;
      seekTo(e);
    });
    document.addEventListener('mousemove', function onMove(e) {
      if (seeking) seekTo(e);
    });
    document.addEventListener('mouseup', function onUp() {
      seeking = false;
    });
    function seekTo(e) {
      var rect = progressWrap.getBoundingClientRect();
      var pct = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
      if (audio.duration && isFinite(audio.duration)) {
        audio.currentTime = pct * audio.duration;
      }
    }

    // Audio events
    audio.addEventListener('timeupdate', function() {
      curTime.textContent = fmtTime(audio.currentTime);
      if (audio.duration && isFinite(audio.duration)) {
        progressFill.style.width = (audio.currentTime / audio.duration * 100) + '%';
      }
    });
    audio.addEventListener('loadedmetadata', function() {
      durTime.textContent = fmtTime(audio.duration);
    });
    audio.addEventListener('play', function() {
      playing = true; updatePlayBtn();
    });
    audio.addEventListener('pause', function() {
      playing = false; updatePlayBtn();
    });
    audio.addEventListener('ended', function() {
      playing = false;
      updatePlayBtn();
      playNext();
    });

    // Keyboard
    wrap.tabIndex = -1;
    wrap.addEventListener('keydown', function(e) {
      if (e.key === ' ') {
        e.preventDefault();
        e.stopPropagation();
        if (playing) { audio.pause(); } else { (function(p){if(p&&p.catch)p.catch(function(){});})(audio.play()); }
        return;
      }
      if (e.key === 'ArrowLeft') { e.preventDefault(); audio.currentTime = Math.max(0, audio.currentTime - 10); return; }
      if (e.key === 'ArrowRight') { e.preventDefault(); audio.currentTime = Math.min(audio.duration || 0, audio.currentTime + 10); return; }
    });

    // Header buttons
    var dlBtn = document.createElement('button');
    dlBtn.className = 'btn btn-sm';
    dlBtn.textContent = 'Download';
    dlBtn.addEventListener('click', function() { App.downloadFile(audioItems[curIdx]); });

    var saveBtn = document.createElement('button');
    saveBtn.className = 'btn btn-sm';
    saveBtn.textContent = 'Save Playlist';
    saveBtn.addEventListener('click', async function() {
      var plName = listTitle || 'New Playlist';
      var defaultDir = sourceDir || App.davUrl + Auth.username + '/Music/';
      var defaultName = plName.replace(/\.m3u$/i, '') + '.m3u';
      var result = await FilePicker.save('Save Playlist', defaultDir, defaultName);
      if (!result) return;
      var filename = result.filename;
      if (!/\.m3u$/i.test(filename)) filename += '.m3u';
      var fullPath = result.dir + encodeURIComponent(filename);
      var content = '#EXTM3U\n';
      for (var si = 0; si < songs.length; si++) {
        if (isMissing(si)) continue;
        var meta = metaCache[si];
        var title = (meta && meta.title) || audioItems[si]._plTitle || songs[si].name;
        var dur = audio.duration && si === curIdx ? Math.round(audio.duration) : -1;
        content += '#EXTINF:' + dur + ',' + title + '\n' + songs[si].url + '\n';
      }
      try {
        var resp = await fetch(fullPath, { method: 'PUT', body: content, headers: { 'Content-Type': 'audio/x-mpegurl' } });
        if (!resp.ok) saveBtn.textContent = 'Save Failed';
        else { saveBtn.textContent = 'Saved!'; setTimeout(function() { saveBtn.textContent = 'Save Playlist'; }, 2000); }
      } catch(e) { saveBtn.textContent = 'Save Failed'; }
    });

    var editBtn = document.createElement('button');
    editBtn.className = 'btn btn-sm';
    editBtn.textContent = 'Edit Playlist';
    editBtn.addEventListener('click', function() {
      var plTracks = [];
      for (var ei = 0; ei < songs.length; ei++) {
        if (isMissing(ei)) continue;
        var meta = metaCache[ei];
        plTracks.push({
          path: songs[ei].url,
          name: songs[ei].name,
          title: (meta && meta.title) || audioItems[ei]._plTitle || songs[ei].name,
          duration: 0
        });
      }
      audio.pause();
      audio.src = '';
      WinManager.close(winId);
      PlaylistBuilder.open(listTitle || 'New Playlist', plTracks);
    });

    // Responsive layout via ResizeObserver
    var resizeObs = new ResizeObserver(function(entries) {
      var w = entries[0].contentRect.width;
      wrap.classList.toggle('amp-narrow', w < 500);
    });

    var winId = WinManager.open(listTitle || item.name, wrap, {
      type: 'audio', singleton: true,
      wide: true,
      headerActions: [editBtn, saveBtn, dlBtn],
      onClose: function() {
        resizeObs.disconnect();
        audio.pause();
        audio.src = '';
        for (var k in artUrlCache) {
          if (artUrlCache[k]) URL.revokeObjectURL(artUrlCache[k]);
        }
      }
    });

    resizeObs.observe(wrap);
    wrap.focus();

    // Load libraries then start
    this._loadAudioLibs().then(function() {
      loadTrack(startIdx, true);
      // Pre-fetch metadata for all tracks (populates artist column)
      for (var i = 0; i < songs.length; i++) extractMeta(i);
    }).catch(function() {
      loadTrack(startIdx, true);
    });
  },

  async _loadAudioLibs() {
    if (this._loaded.jsmediatags) return;
    if (!window.jsmediatags) {
      await new Promise(function(resolve, reject) {
        var s = document.createElement('script');
        s.src = CDN.jsmediatags;
        s.onload = resolve;
        s.onerror = reject;
        document.head.appendChild(s);
      });
    }
    this._loaded.jsmediatags = true;
  },

  // Open a .m3u playlist file
  async _openPlaylist(item) {
    try {
      var resp = await fetch(item.href);
      if (!resp.ok) { Dialog.alert('Failed to load playlist'); return; }
      var text = await resp.text();
      var lines = text.split(/\r?\n/);
      var tracks = [];
      var nextTitle = '';
      var nextDur = -1;
      for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();
        if (!line || line === '#EXTM3U') continue;
        if (line.indexOf('#EXTINF:') === 0) {
          var info = line.substring(8);
          var comma = info.indexOf(',');
          if (comma !== -1) {
            nextDur = parseInt(info.substring(0, comma)) || -1;
            nextTitle = info.substring(comma + 1).trim();
          }
          continue;
        }
        if (line[0] === '#') continue;
        var path = line;
        var name = path.split('/').pop();
        tracks.push({ path: path, name: name, title: nextTitle || name, duration: nextDur > 0 ? nextDur : 0 });
        nextTitle = '';
        nextDur = -1;
      }
      if (!tracks.length) {
        var plName = item.name.replace(/\.m3u8?$/i, '');
        var plDir = item.href.substring(0, item.href.lastIndexOf('/') + 1);
        PlaylistBuilder.open(plName, [], plDir);
        return;
      }
      // Build audio items, mark missing ones
      var audioItems = [];
      for (var j = 0; j < tracks.length; j++) {
        audioItems.push({
          name: tracks[j].name,
          href: tracks[j].path,
          isDir: false,
          _plTitle: tracks[j].title,
          _plDuration: tracks[j].duration
        });
      }
      var plName = item.name.replace(/\.m3u8?$/i, '');
      var plDir = item.href.substring(0, item.href.lastIndexOf('/') + 1);
      this._openAudioFromList(audioItems, plName, plDir);
    } catch(e) {
      Dialog.alert('Failed to load playlist: ' + e.message);
    }
  },

  // Open the audio player with an arbitrary list of items (from playlist or builder)
  _openAudioFromList: function(audioItems, title, sourceDir) {
    // Check which tracks exist by issuing HEAD requests, mark missing
    var self = this;
    var checked = 0;
    var total = audioItems.length;

    function checkDone() {
      checked++;
      if (checked >= total) {
        self._openAudio(audioItems[0], audioItems, title, sourceDir);
      }
    }

    for (var i = 0; i < audioItems.length; i++) {
      (function(item) {
        fetch(item.href, { method: 'HEAD' }).then(function(r) {
          if (!r.ok) item._missing = true;
          checkDone();
        }).catch(function() {
          item._missing = true;
          checkDone();
        });
      })(audioItems[i]);
    }
  },

  async _loadVideoJs() {
    if (this._loaded.videojs) return;
    // Load CSS
    if (!document.getElementById('videojs-css')) {
      const link = document.createElement('link');
      link.id = 'videojs-css';
      link.rel = 'stylesheet';
      link.href = CDN.videojsCss;
      document.head.appendChild(link);
    }
    // Load JS
    if (!window.videojs) {
      await new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = CDN.videojsJs;
        s.onload = resolve;
        s.onerror = reject;
        document.head.appendChild(s);
      });
    }
    this._loaded.videojs = true;
  },

  _openOffice(item) {
    // Build the DAV path for the ONLYOFFICE editor endpoint
    var davPath = item.href;
    var editorUrl = '/dav/_office?file=' + encodeURIComponent(davPath);
    var dirty = false;
    var winId;

    var iframe = document.createElement('iframe');
    iframe.className = 'pdf-viewer';
    iframe.src = editorUrl;
    iframe.setAttribute('allowfullscreen', 'true');

    function onMessage(e) {
      if (!e.data || typeof e.data.type !== 'string') return;
      if (e.data.type === 'oo-dirty') {
        dirty = e.data.dirty;
        var title = dirty ? '● ' + item.name : item.name;
        WinManager.setTitle(winId, title);
      }
    }
    window.addEventListener('message', onMessage);

    var isDemoFile = Auth.demoMode && item.href.indexOf('/dav/demo-files/') === 0;
    var winTitle = isDemoFile ? item.name + ' (read only — copy out of demo-files to edit)' : item.name;

    winId = WinManager.open(winTitle, iframe, {
      type: 'office', full: true, noPadding: true,
      beforeClose: function() {
        if (!dirty) return true;
        return Dialog.confirm('You have unsaved changes. Close anyway?', 'Close', true);
      },
      onClose: function() {
        window.removeEventListener('message', onMessage);
      }
    });
  },

  _openPlugin(item, pluginName) {
    var renderUrl = '/dav/_plugin/render?file=' + encodeURIComponent(item.href) +
        '&plugin=' + encodeURIComponent(pluginName);
    var dirty = false;
    var winId;
    var plugin = this._plugins ? this._plugins[pluginName] : null;

    var iframe = document.createElement('iframe');
    iframe.className = 'pdf-viewer';
    iframe.src = renderUrl;
    iframe.setAttribute('allowfullscreen', 'true');

    // Inject custom statusbar icon if plugin provides one
    var winType = 'plugin';
    if (plugin && plugin.icon) {
      winType = 'plugin-' + pluginName.replace(/[^a-zA-Z0-9]/g, '-').toLowerCase();
      if (!document.getElementById('plugin-icon-' + winType)) {
        var style = document.createElement('style');
        style.id = 'plugin-icon-' + winType;
        style.textContent = '.statusbar-win-btn[data-type="' + winType + '"]::after { ' +
          '-webkit-mask-image: url("' + plugin.icon + '"); ' +
          'mask-image: url("' + plugin.icon + '"); }';
        document.head.appendChild(style);
      }
    }

    function onMessage(e) {
      if (!e.data || typeof e.data.type !== 'string') return;
      if (e.data.type === 'oo-dirty') {
        dirty = e.data.dirty;
        var title = dirty ? '● ' + item.name : item.name;
        WinManager.setTitle(winId, title);
      }
    }
    window.addEventListener('message', onMessage);

    winId = WinManager.open(item.name, iframe, {
      type: winType, full: true, noPadding: true,
      singleton: !!(plugin && plugin.singleton),
      beforeClose: function() {
        if (!dirty) return true;
        return Dialog.confirm('You have unsaved changes. Close anyway?', 'Close', true);
      },
      onClose: function() {
        window.removeEventListener('message', onMessage);
      }
    });
  },

  async _openPdf(item) {
    // Fetch PDF as blob, create object URL
    // On desktop, iframe renders it natively
    // On mobile, use PDF.js from CDN for rendering
    try {
      var resp = await fetch(item.href, {credentials: 'same-origin'});
      if (!resp.ok) { Toast.error('Failed to load PDF'); return; }
      var blob = await resp.blob();
      var url = URL.createObjectURL(blob);

      var isMobile = 'ontouchstart' in window;
      if (!isMobile) {
        // Desktop: native PDF rendering in iframe
        var iframe = document.createElement('iframe');
        iframe.className = 'pdf-viewer';
        iframe.src = url;
        WinManager.open(item.name, iframe, {
          type: 'pdf', full: true,
          onClose: function() { URL.revokeObjectURL(url); }
        });
      } else {
        // Mobile: use PDF.js
        var wrap = document.createElement('div');
        wrap.style.cssText = 'width:100%;height:100%;overflow:auto;background:#333';
        var winId = WinManager.open(item.name, wrap, {
          type: 'pdf', full: true, noPadding: true,
          onClose: function() { URL.revokeObjectURL(url); }
        });
        // Load PDF.js if not already loaded
        if (!window.pdfjsLib) {
          var script = document.createElement('script');
          script.src = CDN.pdfjsJs;
          script.type = 'module';
          // Use a simpler approach: load the legacy build
          script.src = CDN.pdfjsJs;
          script.type = 'text/javascript';
          script.onload = function() {
            window.pdfjsLib.GlobalWorkerOptions.workerSrc = CDN.pdfjsWorker;
            renderPdfToWrap(url, wrap);
          };
          document.head.appendChild(script);
        } else {
          renderPdfToWrap(url, wrap);
        }
      }
    } catch(e) {
      Toast.error('Failed to load PDF: ' + e.message);
    }

    function renderPdfToWrap(pdfUrl, container) {
      window.pdfjsLib.getDocument(pdfUrl).promise.then(function(pdf) {
        for (var p = 1; p <= pdf.numPages; p++) {
          (function(pageNum) {
            pdf.getPage(pageNum).then(function(page) {
              var scale = Math.min(
                (container.clientWidth || 400) / page.getViewport({scale: 1}).width,
                2
              );
              var viewport = page.getViewport({scale: scale});
              var canvas = document.createElement('canvas');
              canvas.style.cssText = 'display:block;margin:4px auto;';
              canvas.width = viewport.width;
              canvas.height = viewport.height;
              container.appendChild(canvas);
              page.render({canvasContext: canvas.getContext('2d'), viewport: viewport});
            });
          })(p);
        }
      });
    }
  },

  _loadFilerobot() {
    if (this._loaded.filerobot) return Promise.resolve();
    return new Promise(function(resolve, reject) {
      var s = document.createElement('script');
      s.src = CDN.filerobot;
      s.onload = function() {
        Viewers._loaded.filerobot = true;
        resolve();
      };
      s.onerror = reject;
      document.head.appendChild(s);
    });
  },

  _loadJodit() {
    if (this._loaded.jodit) return Promise.resolve();
    return new Promise(function(resolve, reject) {
      // Load CSS first
      var link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = CDN.joditCss;
      document.head.appendChild(link);
      // Then JS
      var s = document.createElement('script');
      s.src = CDN.joditJs;
      s.onload = function() {
        Viewers._loaded.jodit = true;
        resolve();
      };
      s.onerror = reject;
      document.head.appendChild(s);
    });
  },

  async _openHtmlEditor(item, onSaveReturn) {
    try {
      await this._loadJodit();
    } catch(e) {
      Toast.error('Failed to load HTML editor');
      return;
    }

    var content;
    try {
      content = await DavClient.getText(item.href);
    } catch(e) {
      Toast.error('Failed to load file');
      return;
    }

    var currentHref = item.href;
    var currentName = item.name;
    var dirty = false;

    // Extract body content and preserve the document shell.
    // If the file has no <body>, treat the entire content as the body.
    var headContent = '';
    var bodyContent = content;
    var hasDocStructure = /<html[\s>]/i.test(content);

    if (hasDocStructure) {
      // Extract everything between <body...> and </body>
      var bodyMatch = content.match(/<body[^>]*>([\s\S]*?)<\/body>/i);
      if (bodyMatch) {
        bodyContent = bodyMatch[1];
      }
      // Extract <head>...</head> contents
      var headMatch = content.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      if (headMatch) {
        headContent = headMatch[1];
      }
    }

    // Reconstruct full document from head + edited body
    function wrapDocument(editedBody) {
      // Extract or generate title
      var titleMatch = headContent.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
      var title = titleMatch ? titleMatch[1].trim() : currentName.replace(/\.html?$/i, '');

      if (hasDocStructure) {
        // Rebuild preserving original head content
        return '<!DOCTYPE html>\n<html>\n<head>\n' +
          headContent.trim() + '\n</head>\n<body>\n' +
          editedBody + '\n</body>\n</html>\n';
      } else {
        // Wrap bare content in a minimal document
        return '<!DOCTYPE html>\n<html>\n<head>\n  <meta charset="utf-8">\n  <title>' +
          title + '</title>\n</head>\n<body>\n' +
          editedBody + '\n</body>\n</html>\n';
      }
    }

    var wrap = document.createElement('div');
    wrap.style.cssText = 'width:100%;height:100%;display:flex;flex-direction:column';

    var editorArea = document.createElement('textarea');
    editorArea.style.display = 'none';
    wrap.appendChild(editorArea);

    // Header buttons
    var saveBtn = document.createElement('button');
    saveBtn.className = 'btn btn-primary btn-sm';
    saveBtn.textContent = 'Save';

    var saveAsBtn = document.createElement('button');
    saveAsBtn.className = 'btn btn-sm';
    saveAsBtn.textContent = 'Save As';

    var joditInstance = null;

    function getFullDocument() {
      if (!joditInstance) return content;
      return wrapDocument(joditInstance.value);
    }

    saveBtn.addEventListener('click', async function() {
      saveBtn.disabled = true;
      try {
        var doc = getFullDocument();
        var resp = await DavClient.put(currentHref, new Blob([doc], { type: 'text/html' }));
        if (resp.status >= 200 && resp.status < 300) {
          Toast.show('Saved');
          dirty = false;
          if (onSaveReturn) {
            WinManager.close(winId);
            onSaveReturn({ name: currentName, href: currentHref, isDir: false });
          }
        } else {
          Toast.error('Save failed (' + resp.status + ')');
        }
      } catch(e) {
        Toast.error('Save failed: ' + e.message);
      }
      saveBtn.disabled = false;
    });

    saveAsBtn.addEventListener('click', async function() {
      var dir = currentHref.substring(0, currentHref.lastIndexOf('/') + 1);
      var result = await FilePicker.save('Save As', dir, currentName);
      if (!result) return;
      var destUrl = result.dir + encodeURIComponent(result.filename);
      saveAsBtn.disabled = true;
      try {
        var doc = getFullDocument();
        var resp = await DavClient.put(destUrl, new Blob([doc], { type: 'text/html' }));
        if (resp.status >= 200 && resp.status < 300) {
          Toast.show('Saved as ' + result.filename);
          currentHref = destUrl;
          currentName = result.filename;
          dirty = false;
          WinManager.setTitle(winId, 'Edit — ' + currentName);
        } else {
          Toast.error('Save failed (' + resp.status + ')');
        }
      } catch(e) {
        Toast.error('Save failed: ' + e.message);
      }
      saveAsBtn.disabled = false;
    });

    // Source button to switch to CodeMirror
    var sourceBtn = document.createElement('button');
    sourceBtn.className = 'btn btn-sm';
    sourceBtn.textContent = 'Source';
    sourceBtn.addEventListener('click', function() {
      var editItem = { name: currentName, href: currentHref, isDir: false };
      if (dirty) {
        Dialog.confirm('Switch to source editor? Unsaved changes will be lost.', 'Switch', true).then(function(ok) {
          if (ok) {
            WinManager.close(winId);
            Viewers._openCode(editItem);
          }
        });
      } else {
        WinManager.close(winId);
        Viewers._openCode(editItem);
      }
    });

    var winId = WinManager.open('Edit — ' + item.name, wrap, {
      type: 'html-editor', full: true, noPadding: true,
      headerActions: [saveBtn, saveAsBtn, sourceBtn],
      beforeClose: function() {
        if (!dirty) return true;
        return Dialog.confirm('You have unsaved changes. Close anyway?', 'Close', true);
      },
      onClose: function() {
        if (joditInstance) {
          try { joditInstance.destruct(); } catch(e) {}
        }
      }
    });

    // Compute base URL for the document's directory (for iframe <base> tag)
    var docDir = currentHref.substring(0, currentHref.lastIndexOf('/') + 1);
    // Full origin-relative base: e.g. "https://host/dav/Documents/"
    var baseUrl = location.origin + docDir;

    // Initialize Jodit in iframe mode so <base> makes relative URLs work
    joditInstance = Jodit.make(editorArea, {
      iframe: true,
      iframeBaseUrl: baseUrl,
      height: '100%',
      width: '100%',
      toolbarSticky: false,
      showCharsCounter: false,
      showWordsCounter: false,
      showXPathInStatusbar: false,
      askBeforePasteHTML: false,
      askBeforePasteFromWord: false,
      defaultActionOnPaste: 'insert_only_text',
      beautifyHTMLCDNUrlsJS: [CDN.beautifyJs, CDN.beautifyHtmlJs],
      sourceEditorCDNUrlsJS: [CDN.aceJs]
    });
    joditInstance.value = bodyContent;
    // Reset dirty after initial value set, then track changes
    dirty = false;
    joditInstance.events.on('change', function() { dirty = true; });

    // Handle image drops from the file manager.
    // Convert absolute /dav/ paths to relative when possible, then insert.
    function makeRelative(src) {
      if (src.indexOf(docDir) === 0) {
        return src.substring(docDir.length);
      }
      return src;
    }

    // Handle file drops from the file manager directly on the iframe document,
    // using capture phase to intercept before Jodit's own handlers.
    var iframeDoc = joditInstance.editorDocument;
    iframeDoc.addEventListener('dragover', function(e) {
      if (e.dataTransfer && e.dataTransfer.types && e.dataTransfer.types.indexOf('text/html') !== -1) {
        e.preventDefault();
      }
    }, true);
    iframeDoc.addEventListener('drop', function(e) {
      var dt = e.dataTransfer;
      if (!dt) return;
      var html = dt.getData('text/html');
      if (html && (/<img\s/i.test(html) || /<a\s/i.test(html))) {
        e.preventDefault();
        e.stopImmediatePropagation();
        // Convert absolute paths to relative where possible
        html = html.replace(/<img\s/gi, '<img style="max-width:100%;height:auto" ');
        html = html.replace(/\b(src|href)="([^"]+)"/gi, function(match, attr, url) {
          return attr + '="' + makeRelative(url) + '"';
        });
        // Place caret at drop position
        var range;
        if (iframeDoc.caretRangeFromPoint) {
          range = iframeDoc.caretRangeFromPoint(e.clientX, e.clientY);
        } else if (iframeDoc.caretPositionFromPoint) {
          var pos = iframeDoc.caretPositionFromPoint(e.clientX, e.clientY);
          if (pos) {
            range = iframeDoc.createRange();
            range.setStart(pos.offsetNode, pos.offset);
            range.collapse(true);
          }
        }
        if (range) {
          var sel = iframeDoc.defaultView.getSelection();
          sel.removeAllRanges();
          sel.addRange(range);
        }
        joditInstance.s.insertHTML(html);
      }
    }, true);
  },

  async _openImageEditor(item, onSaveReturn) {
    try {
      await this._loadFilerobot();
    } catch(e) {
      Toast.error('Failed to load image editor');
      return;
    }

    var FIE = window.FilerobotImageEditor;
    var wrap = document.createElement('div');
    wrap.style.cssText = 'width:100%;height:100%';

    var currentHref = item.href;
    var currentName = item.name;
    var dirty = false;
    var filerobotEditor = null;

    function mimeForName(name) {
      var ext = name.split('.').pop().toLowerCase();
      if (ext === 'png') return 'image/png';
      if (ext === 'webp') return 'image/webp';
      return 'image/jpeg';
    }

    function canvasToBlob(canvas, mime) {
      return new Promise(function(resolve) {
        canvas.toBlob(function(blob) { resolve(blob); }, mime, 0.92);
      });
    }

    // Get full-resolution canvas via FIE's public API.
    // getCurrentImgData(imageFileInfo, pixelRatio, keepLoadingSpinnerShown)
    // Internally uses Konva to re-render at original dimensions with all
    // transforms (rotation, crop, filters, annotations) applied.
    function getEditedCanvas() {
      if (!filerobotEditor || typeof filerobotEditor.getCurrentImgData !== 'function') {
        return null;
      }
      var ext = currentName.split('.').pop().toLowerCase();
      var baseName = currentName.replace(/\.[^.]+$/, '');
      // Pass undefined for pixelRatio to use FIE's default savingPixelRatio
      var result = filerobotEditor.getCurrentImgData(
        { name: baseName, extension: ext, quality: 0.92 }
      );
      if (result && result.imageData && result.imageData.imageCanvas) {
        return result.imageData.imageCanvas;
      }
      return null;
    }

    // Header buttons
    var saveBtn = document.createElement('button');
    saveBtn.className = 'btn btn-primary btn-sm';
    saveBtn.textContent = 'Save';

    var saveAsBtn = document.createElement('button');
    saveAsBtn.className = 'btn btn-sm';
    saveAsBtn.textContent = 'Save As';

    saveBtn.addEventListener('click', async function() {
      var canvas = getEditedCanvas();
      if (!canvas) { Toast.error('No edits to save'); return; }
      saveBtn.disabled = true;
      try {
        var blob = await canvasToBlob(canvas, mimeForName(currentName));
        var resp = await DavClient.put(currentHref, blob);
        if (resp.status >= 200 && resp.status < 300) {
          Toast.show('Saved');
          dirty = false;
          // Register cache bust so all future loads get the fresh version
          Viewers._cacheBust[currentHref] = Date.now();
          if (onSaveReturn) {
            var returnItem = { name: currentName, href: currentHref, isDir: false };
            WinManager.close(winId);
            onSaveReturn(returnItem);
          }
        } else {
          Toast.error('Save failed (' + resp.status + ')');
        }
      } catch(e) {
        Toast.error('Save failed: ' + e.message);
      }
      saveBtn.disabled = false;
    });

    saveAsBtn.addEventListener('click', async function() {
      var canvas = getEditedCanvas();
      if (!canvas) { Toast.error('No edits to save'); return; }
      var dir = currentHref.substring(0, currentHref.lastIndexOf('/') + 1);
      var result = await FilePicker.save('Save Image As', dir, currentName);
      if (!result) return;
      var destUrl = result.dir + encodeURIComponent(result.filename);
      saveAsBtn.disabled = true;
      try {
        var blob = await canvasToBlob(canvas, mimeForName(result.filename));
        var resp = await DavClient.put(destUrl, blob);
        if (resp.status >= 200 && resp.status < 300) {
          Toast.show('Saved as ' + result.filename);
          currentHref = destUrl;
          currentName = result.filename;
          dirty = false;
          WinManager.setTitle(winId, 'Edit — ' + currentName);
        } else {
          Toast.error('Save failed (' + resp.status + ')');
        }
      } catch(e) {
        Toast.error('Save failed: ' + e.message);
      }
      saveAsBtn.disabled = false;
    });

    var winId = WinManager.open('Edit — ' + item.name, wrap, {
      type: 'image-editor', full: true, noPadding: true,
      headerActions: [saveBtn, saveAsBtn],
      beforeClose: function() {
        if (!dirty) return true;
        return Dialog.confirm('You have unsaved changes. Close anyway?', 'Close', true);
      },
      onClose: function() {
        if (filerobotEditor) {
          try { filerobotEditor.terminate(); } catch(e) {}
        }
      }
    });

    // Initialize Filerobot — save button hidden via CSS, save dialog suppressed
    filerobotEditor = new FIE(wrap, {
      source: item.href,
      onBeforeSave: function() { return false; },
      onModify: function() { dirty = true; },
      annotationsCommon: { fill: 'transparent', stroke: '#ff0000' },
      tabsIds: [
        FIE.TABS.ADJUST,
        FIE.TABS.FINETUNE,
        FIE.TABS.FILTERS,
        FIE.TABS.RESIZE,
        FIE.TABS.ANNOTATE
      ],
      defaultTabId: FIE.TABS.ADJUST
    });

    filerobotEditor.render();
  },


  async _openEpub(item) {
    const wrap = document.createElement('div');
    wrap.className = 'epub-viewer';

    // Navigation bar
    const nav = document.createElement('div');
    nav.className = 'epub-nav';

    const prevBtn = document.createElement('button');
    prevBtn.className = 'btn btn-sm';
    prevBtn.textContent = '\u2190 Prev';

    const nextBtn = document.createElement('button');
    nextBtn.className = 'btn btn-sm';
    nextBtn.textContent = 'Next \u2192';

    const pageInfo = document.createElement('span');
    pageInfo.className = 'epub-page-info';

    nav.appendChild(prevBtn);
    nav.appendChild(pageInfo);
    nav.appendChild(nextBtn);

    // Reader area
    const reader = document.createElement('div');
    reader.className = 'epub-reader';
    wrap.appendChild(reader);
    wrap.appendChild(nav);

    // TOC sidebar
    const tocBtn = document.createElement('button');
    tocBtn.className = 'btn btn-sm';
    tocBtn.textContent = 'TOC';

    const dlBtn = document.createElement('button');
    dlBtn.className = 'btn btn-sm';
    dlBtn.textContent = 'Download';
    dlBtn.addEventListener('click', () => App.downloadFile(item));

    let book = null;
    let rendition = null;
    let resizeObserver = null;

    var winId = WinManager.open(item.name, wrap, {
      type: 'epub', full: true, noPadding: true,
      headerActions: [tocBtn, dlBtn],
      onClose: () => {
        if (resizeObserver) try { resizeObserver.disconnect(); } catch (e) {}
        if (rendition) try { rendition.destroy(); } catch (e) {}
        if (book) try { book.destroy(); } catch (e) {}
      }
    });

    try {
      await this._loadEpubJs();

      // Fetch the epub as an ArrayBuffer
      const resp = await DavClient.send('GET', item.href);
      const arrayBuf = await resp.arrayBuffer();

      book = window.ePub(arrayBuf);
      rendition = book.renderTo(reader, {
        width: '100%',
        height: '100%',
        spread: 'auto'
      });
      // Resize rendition when modal is resized
      resizeObserver = new ResizeObserver(() => {
        if (rendition) rendition.resize();
      });
      resizeObserver.observe(reader);

      // Restore saved reading position
      const savedCfi = await this._loadEpubPosition(item.href);
      rendition.display(savedCfi || undefined);

      // Navigation
      prevBtn.addEventListener('click', () => rendition.prev());
      nextBtn.addEventListener('click', () => rendition.next());

      // Keyboard nav
      rendition.on('keydown', (e) => {
        if (e.key === 'ArrowLeft') rendition.prev();
        if (e.key === 'ArrowRight') rendition.next();
      });
      document.addEventListener('keydown', function epubKey(e) {
        var w = WinManager.getWindow(winId);
        if (!w) {
          document.removeEventListener('keydown', epubKey);
          return;
        }
        if (w.minimized) return;
        var focused = WinManager.getFocusedWindow();
        if (!focused || focused.id !== winId) return;
        if (e.key === 'ArrowLeft') { rendition.prev(); e.preventDefault(); }
        if (e.key === 'ArrowRight') { rendition.next(); e.preventDefault(); }
      });

      // Page location display + save position on each page turn
      book.ready.then(() => book.locations.generate(1024)).then(() => {
        rendition.on('relocated', (location) => {
          const pct = book.locations.percentageFromCfi(location.start.cfi);
          pageInfo.textContent = Math.round(pct * 100) + '%';
          // Save position to server
          this._saveEpubPosition(item.href, location.start.cfi);
        });
      });

      // TOC button — opens as modal dialog over the epub window
      tocBtn.addEventListener('click', async () => {
        const toc = await book.loaded.navigation;
        const tocWrap = document.createElement('div');
        tocWrap.className = 'epub-toc';
        toc.toc.forEach(ch => {
          const link = document.createElement('a');
          link.className = 'epub-toc-item';
          link.textContent = ch.label.trim();
          link.href = '#';
          link.addEventListener('click', (e) => {
            e.preventDefault();
            rendition.display(ch.href);
            Dialog.close();
          });
          tocWrap.appendChild(link);
        });
        Dialog.open(item.name + ' \u2014 Contents', tocWrap, { wide: true });
      });

    } catch (e) {
      console.warn('Failed to load EPUB:', e);
      Toast.error('Failed to open EPUB');
    }
  },

  async _loadEpubJs() {
    if (this._loaded.epubjs) return;
    // Load JSZip (required by epub.js)
    if (!window.JSZip) {
      await new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = CDN.jszip;
        s.onload = resolve;
        s.onerror = reject;
        document.head.appendChild(s);
      });
    }
    // Load epub.js
    if (!window.ePub) {
      await new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = CDN.epubjs;
        s.onload = resolve;
        s.onerror = reject;
        document.head.appendChild(s);
      });
    }
    this._loaded.epubjs = true;
  },

  // Save epub reading position as a dead property: <R:epub-pos-USERNAME>cfi</R:epub-pos-USERNAME>
  _epubPropName() {
    return 'R:epub-pos-' + Auth.username;
  },

  _saveEpubPositionDebounce: null,
  _saveEpubPosition(href, cfi) {
    // Debounce — save at most once per 2 seconds
    clearTimeout(this._saveEpubPositionDebounce);
    this._saveEpubPositionDebounce = setTimeout(() => {
      const prop = this._epubPropName();
      const body = '<?xml version="1.0" encoding="UTF-8"?>' +
        '<D:propertyupdate xmlns:D="DAV:" xmlns:R="reader:">' +
        '<D:set><D:prop><' + prop + '>' +
        App._escXml(cfi) +
        '</' + prop + '></D:prop></D:set></D:propertyupdate>';
      DavClient.send('PROPPATCH', href, body, {
        'Content-Type': 'text/xml; charset=utf-8'
      }).catch(() => {});
    }, 2000);
  },

  async _loadEpubPosition(href) {
    const prop = this._epubPropName();
    const body = '<?xml version="1.0" encoding="UTF-8"?>' +
      '<D:propfind xmlns:D="DAV:" xmlns:R="reader:"><D:prop>' +
      '<' + prop + '/></D:prop></D:propfind>';
    try {
      const resp = await DavClient.send('PROPFIND', href, body, {
        'Depth': '0', 'Content-Type': 'text/xml; charset=utf-8'
      });
      const text = await resp.text();
      const re = new RegExp('<[^>]*epub-pos-' + Auth.username + '[^>]*>([^<]+)<');
      const match = text.match(re);
      return match ? match[1] : null;
    } catch (e) {
      return null;
    }
  },

  // Check if current user has write permission on an item (client-side check for UI)
  _canWrite(item) {
    if (item.fsWritable === false) return false;
    if (Auth.admin) return true;
    if (!item.permissions && item.permissions !== 0) return true; // no metadata = allow
    const mode = item.permissions;
    let bits;
    if (item.owner === Auth.username) {
      bits = Math.floor(mode / 100) % 10;
    } else if (item.group === 'everyone' ||
               (item.group !== 'nogroup' && Auth.groups && Auth.groups.indexOf(item.group) !== -1)) {
      bits = Math.floor(mode / 10) % 10;
    } else {
      bits = mode % 10;
    }
    return (bits & 2) !== 0;
  },

  async _openCode(item) {
    const wrap = document.createElement('div');
    wrap.className = 'code-editor-wrap';
    let readOnly = !this._canWrite(item);

    let content;
    try {
      content = await DavClient.getText(item.href);
    } catch (e) {
      Toast.error('Failed to load file');
      return;
    }

    let editor = null;
    let savedContent = content;

    // Always create a textarea first so content is visible immediately
    const textarea = document.createElement('textarea');
    textarea.className = 'code-textarea';
    textarea.value = content;
    if (readOnly) {
      textarea.readOnly = true;
    } else {
      textarea.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 's') {
          e.preventDefault();
          if (e.shiftKey) saveFileAs(); else saveFile();
        }
        if (e.key === 'Tab') {
          e.preventDefault();
          const start = textarea.selectionStart;
          textarea.value = textarea.value.substring(0, start) + '\t' + textarea.value.substring(textarea.selectionEnd);
          textarea.selectionStart = textarea.selectionEnd = start + 1;
        }
      });
    }
    wrap.appendChild(textarea);

    var currentHref = item.href;

    const saveToUrl = async (url) => {
      const text = editor ? editor.state.doc.toString() : textarea.value;
      try {
        const resp = await DavClient.put(url, new Blob([text], { type: 'text/plain' }));
        if (!isDavOk(resp)) {
          Toast.error('Save failed (' + resp.status + ')');
          return false;
        }
        // Verify the file was actually written by reading it back
        try {
          var verify = await DavClient.getText(url);
          if (verify !== text) {
            Toast.error('Save failed — file content did not update');
            return false;
          }
        } catch (ve) {
          Toast.error('Save failed — could not verify file');
          return false;
        }
        savedContent = text;
        return true;
      } catch (e) {
        Toast.error('Save failed');
        return false;
      }
    };

    const saveFile = async () => {
      if (readOnly) return;
      if (await saveToUrl(currentHref)) {
        Toast.success('Saved ' + item.name);
      }
    };

    const saveFileAs = async () => {
      var text = editor ? editor.state.doc.toString() : textarea.value;
      var dir = currentHref.substring(0, currentHref.lastIndexOf('/') + 1);
      var name = currentHref.substring(currentHref.lastIndexOf('/') + 1);
      name = decodeURIComponent(name);
      var result = await FilePicker.save('Save As', dir, name);
      if (!result) return;
      var destUrl = result.dir + encodeURIComponent(result.filename);
      if (await saveToUrl(destUrl)) {
        // Update current file reference
        currentHref = destUrl;
        item = Object.assign({}, item, {href: destUrl, name: result.filename});
        WinManager.setTitle(winId, result.filename);
        readOnly = false;
        saveBtn.hidden = false;
        Toast.success('Saved as ' + result.filename);
      }
    };

    // Save button (hidden in read-only mode)
    const saveBtn = document.createElement('button');
    saveBtn.className = 'btn btn-primary btn-sm';
    saveBtn.textContent = 'Save';
    if (readOnly) saveBtn.hidden = true;
    saveBtn.addEventListener('click', saveFile);

    // Save As button
    const saveAsBtn = document.createElement('button');
    saveAsBtn.className = 'btn btn-sm';
    saveAsBtn.textContent = 'Save As';
    saveAsBtn.addEventListener('click', saveFileAs);

    // Download button
    const dlBtn = document.createElement('button');
    dlBtn.className = 'btn btn-sm';
    dlBtn.textContent = 'Download';
    dlBtn.addEventListener('click', () => App.downloadFile(item));

    // Markdown preview toggle for .md files
    let mdPreviewBtn = null;
    let mdPreviewDiv = null;
    let mdShowingPreview = false;
    const isMarkdown = /\.md$/i.test(item.name) || (item.mime && item.mime === 'text/markdown');
    if (isMarkdown) {
      mdPreviewDiv = document.createElement('div');
      mdPreviewDiv.className = 'markdown-preview';
      wrap.appendChild(mdPreviewDiv);

      mdPreviewBtn = document.createElement('button');
      mdPreviewBtn.className = 'btn btn-sm';
      mdPreviewBtn.textContent = 'Preview';
      mdPreviewBtn.addEventListener('click', async () => {
        if (mdShowingPreview) {
          wrap.classList.remove('showing-preview');
          mdPreviewBtn.textContent = 'Preview';
          mdShowingPreview = false;
        } else {
          // Render and show preview
          if (!window.marked) {
            await new Promise(function(resolve, reject) {
              var s = document.createElement('script');
              s.src = CDN.marked;
              s.onload = resolve;
              s.onerror = reject;
              document.head.appendChild(s);
            });
          }
          var text = editor ? editor.state.doc.toString() : textarea.value;
          mdPreviewDiv.innerHTML = window.marked.parse(text);
          wrap.classList.add('showing-preview');
          mdPreviewBtn.textContent = 'Edit';
          mdShowingPreview = true;
        }
      });
    }

    // Theme selector for titlebar (temporary, per-window override)
    const themeSelect = document.createElement('select');
    themeSelect.title = 'Editor theme';
    themeSelect.style.cssText = 'font-size:12px;padding:1px 4px;border-radius:3px;border:1px solid var(--color-border);background:var(--color-bg);color:var(--color-fg);max-width:130px';
    var cmPref = Auth.cmTheme || 'auto';
    themeSelect.innerHTML = '<option value="auto">Auto</option>';
    var cmThemeList = this.cmThemes;
    for (var tk in cmThemeList) {
      if (cmThemeList.hasOwnProperty(tk)) {
        themeSelect.innerHTML += '<option value="' + tk + '"' + (cmPref === tk ? ' selected' : '') + '>' + cmThemeList[tk].label + '</option>';
      }
    }
    if (cmPref !== 'auto') themeSelect.value = cmPref;

    const actions = [saveBtn, saveAsBtn, dlBtn];
    if (mdPreviewBtn) actions.push(mdPreviewBtn);

    // WYSIWYG edit button for HTML files
    const isHtml = /\.html?$/i.test(item.name);
    if (isHtml && !readOnly) {
      var wysiwygBtn = document.createElement('button');
      wysiwygBtn.className = 'btn btn-sm';
      wysiwygBtn.textContent = 'WYSIWYG';
      wysiwygBtn.addEventListener('click', function() {
        var editItem = { name: item.name, href: currentHref, isDir: false };
        // If dirty, confirm before switching
        if (isDirty()) {
          Dialog.confirm('Switch to WYSIWYG editor? Unsaved changes will be lost.', 'Switch', true).then(function(ok) {
            if (ok) {
              WinManager.close(winId);
              Viewers._openHtmlEditor(editItem, function(savedItem) {
                Viewers._openCode(savedItem);
              });
            }
          });
        } else {
          WinManager.close(winId);
          Viewers._openHtmlEditor(editItem, function(savedItem) {
            Viewers._openCode(savedItem);
          });
        }
      });
      actions.push(wysiwygBtn);
    }

    actions.push(themeSelect);

    const isDirty = () => {
      if (readOnly) return false;
      const current = editor ? editor.state.doc.toString() : textarea.value;
      return current !== savedContent;
    };

    if (!readOnly) Editors.register(isDirty);

    // Open window immediately — textarea shows content right away
    const title = readOnly ? item.name + ' (read-only)' : item.name;
    var winId = WinManager.open(title, wrap, {
      type: 'code', full: true,
      headerActions: actions,
      beforeClose: async () => {
        if (isDirty()) {
          return await Dialog.confirm('You have unsaved changes. Discard them?', 'Discard', true);
        }
        return true;
      },
      onClose: () => { Editors.unregister(isDirty); }
    });

    // Try to upgrade to CodeMirror in the background
    try {
      if (!this._loaded.codemirror) {
        await this._loadCodeMirror();
      }

      const CM = window._CM;
      const extensions = [CM.basicSetup];

      // Read-only mode
      if (readOnly) {
        extensions.push(CM.EditorView.editable.of(false));
        extensions.push(CM.EditorState.readOnly.of(true));
      }

      // Try language extension (loaded lazily per language)
      const langExt = await this._getLanguageExtension(item.name);
      if (langExt) extensions.push(langExt);

      // CodeMirror theme via compartment (allows live switching)
      var themeCompartment = new CM.Compartment();
      var self = this;

      var resolveAutoTheme = function() {
        var isDark = false;
        var appTheme = Auth.theme || 'auto';
        if (appTheme === 'dark') isDark = true;
        else if (appTheme === 'auto') isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
        else if (appTheme !== 'light') {
          var bg = getComputedStyle(document.documentElement).getPropertyValue('--color-bg').trim();
          var m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(bg);
          if (m) {
            var lum = (parseInt(m[1],16)*299 + parseInt(m[2],16)*587 + parseInt(m[3],16)*114) / 1000;
            isDark = lum < 128;
          }
        }
        return (isDark && CM.oneDark) ? CM.oneDark : [];
      };

      var initialThemeExt;
      var initPref = Auth.cmTheme || 'auto';
      if (initPref !== 'auto') {
        var loaded = await this._loadCmTheme(initPref);
        initialThemeExt = loaded || [];
      } else {
        initialThemeExt = resolveAutoTheme();
      }
      extensions.push(themeCompartment.of(initialThemeExt));

      // Grab current textarea content (user may have edited while CM loaded)
      const currentText = textarea.value;

      // Create EditorView BEFORE removing textarea — if it throws, textarea stays
      editor = new CM.EditorView({
        doc: currentText,
        extensions: extensions,
        parent: wrap
      });

      // Success — remove textarea now
      textarea.remove();

      // Wire up titlebar theme selector for live switching
      themeSelect.addEventListener('change', async function() {
        var val = themeSelect.value;
        var ext;
        if (val === 'auto') {
          ext = resolveAutoTheme();
        } else {
          var loaded = await self._loadCmTheme(val);
          ext = loaded || [];
        }
        editor.dispatch({ effects: themeCompartment.reconfigure(ext) });
      });

      // Ctrl+S / Ctrl+Shift+S in CodeMirror
      wrap.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 's') {
          e.preventDefault();
          if (e.shiftKey) saveFileAs(); else if (!readOnly) saveFile();
        }
      });
    } catch (e) {
      console.warn('CodeMirror failed to load, keeping textarea fallback:', e);
      // If textarea was somehow removed, re-add it
      if (!wrap.contains(textarea)) {
        wrap.appendChild(textarea);
      }
    }
  },

  // Available CodeMirror themes — key is the setting value, label is the display name,
  // pkg is the esm.sh package, exp is the export name.
  cmThemes: {
    'one-dark':        { label: 'One Dark',        pkg: '@codemirror/theme-one-dark@6', exp: 'oneDark' },
    'dracula':         { label: 'Dracula',          pkg: '@uiw/codemirror-theme-dracula', exp: 'dracula' },
    'github-light':    { label: 'GitHub Light',     pkg: '@uiw/codemirror-theme-github', exp: 'githubLight' },
    'github-dark':     { label: 'GitHub Dark',      pkg: '@uiw/codemirror-theme-github', exp: 'githubDark' },
    'material-dark':   { label: 'Material Dark',    pkg: '@uiw/codemirror-theme-material', exp: 'material' },
    'material-light':  { label: 'Material Light',   pkg: '@uiw/codemirror-theme-material', exp: 'materialLight' },
    'monokai':         { label: 'Monokai',          pkg: '@uiw/codemirror-theme-monokai', exp: 'monokai' },
    'nord':            { label: 'Nord',             pkg: '@uiw/codemirror-theme-nord', exp: 'nord' },
    'solarized-light': { label: 'Solarized Light',  pkg: '@uiw/codemirror-theme-solarized', exp: 'solarizedLight' },
    'solarized-dark':  { label: 'Solarized Dark',   pkg: '@uiw/codemirror-theme-solarized', exp: 'solarizedDark' },
    'tokyo-night':     { label: 'Tokyo Night',      pkg: '@uiw/codemirror-theme-tokyo-night', exp: 'tokyoNight' },
    'vscode-dark':     { label: 'VS Code Dark',     pkg: '@uiw/codemirror-theme-vscode', exp: 'vscodeDark' },
  },

  // Map theme names to bundle export keys
  _cmThemeBundleMap: {
    'one-dark': 'themeOneDark',
    'dracula': 'themeDracula',
    'github-light': 'themeGithub',
    'github-dark': 'themeGithub',
    'material-dark': 'themeMaterial',
    'material-light': 'themeMaterial',
    'monokai': 'themeMonokai',
    'nord': 'themeNord',
    'solarized-light': 'themeSolarized',
    'solarized-dark': 'themeSolarized',
    'tokyo-night': 'themeTokyoNight',
    'vscode-dark': 'themeVscode',
  },

  async _loadCmTheme(name) {
    if (!window._CM) return null;
    if (window._CM._themes && window._CM._themes[name]) return window._CM._themes[name];
    var info = this.cmThemes[name];
    if (!info) return null;
    if (!window._CM._themes) window._CM._themes = {};
    try {
      var ext;
      if (window._CM._bundle && this._cmThemeBundleMap[name]) {
        var bundleMod = window._CM._bundle[this._cmThemeBundleMap[name]];
        ext = bundleMod[info.exp];
      } else if (CDN.esm) {
        var mod = await import(CDN.esm + info.pkg);
        ext = mod[info.exp];
      } else {
        return null;
      }
      window._CM._themes[name] = ext;
      return ext;
    } catch (e) {
      console.warn('Failed to load CM theme ' + name + ':', e);
      return null;
    }
  },

  async _loadCodeMirror() {
    if (CDN.cmBundle) {
      // Local bundle: everything in one file
      const bundle = await import(CDN.cmBundle);
      const cm = bundle.codemirror;
      const stateModule = bundle.state;
      window._CM = {
        EditorView: cm.EditorView,
        EditorState: stateModule.EditorState,
        Compartment: stateModule.Compartment,
        basicSetup: cm.basicSetup,
        oneDark: bundle.themeOneDark.oneDark,
        langs: {},
        _bundle: bundle,
      };
      if (!window._CM._themes) window._CM._themes = {};
      window._CM._themes['one-dark'] = bundle.themeOneDark.oneDark;
    } else {
      // CDN: load from esm.sh
      const cm = await import(CDN.esm + 'codemirror@6.0.1');
      const stateModule = await import(CDN.esm + '@codemirror/state@6');
      window._CM = {
        EditorView: cm.EditorView,
        EditorState: stateModule.EditorState,
        Compartment: stateModule.Compartment,
        basicSetup: cm.basicSetup,
        oneDark: null,
        langs: {},
      };
      // Pre-load one-dark since it's also the auto-dark fallback
      import(CDN.esm + '@codemirror/theme-one-dark@6').then(m => {
        window._CM.oneDark = m.oneDark;
        if (!window._CM._themes) window._CM._themes = {};
        window._CM._themes['one-dark'] = m.oneDark;
      }).catch(() => {});
    }
    this._loaded.codemirror = true;
  },

  async _getLanguageExtension(filename) {
    // Load the language data registry
    if (!window._CM._langDescs) {
      try {
        if (window._CM._bundle) {
          window._CM._langDescs = window._CM._bundle.languageData.languages;
        } else if (CDN.esm) {
          const mod = await import(CDN.esm + '@codemirror/language-data@6');
          window._CM._langDescs = mod.languages;
        } else {
          return null;
        }
      } catch (e) {
        console.warn('Language data failed to load:', e);
        return null;
      }
    }

    // Match by filename regex first, then by extension
    const descs = window._CM._langDescs;
    let desc = null;
    for (const d of descs) {
      if (d.filename && d.filename.test(filename)) { desc = d; break; }
    }
    if (!desc) {
      const m = /\.([^.]+)$/.exec(filename);
      if (m) {
        const ext = m[1].toLowerCase();
        for (const d of descs) {
          if (d.extensions && d.extensions.indexOf(ext) > -1) { desc = d; break; }
        }
      }
    }
    if (!desc) return null;

    try {
      const support = await desc.load();
      return support;
    } catch (e) {
      console.warn('Language extension failed to load for', filename, ':', e);
      return null;
    }
  },

  _openMarkdownPreview() {
    // Removed — markdown preview is now inline in the code editor dialog
  }
};


/* -----------------------------------------------------------------------
 * Section 11: App — Top-level controller
 * ----------------------------------------------------------------------- */

const App = {
  davUrl: '/dav/',
  _themeLink: null,

  // Apply a theme: 'light', 'dark', 'auto' are built-in base themes.
  // Any other name loads css/themes/<name>.css as an overlay that overrides all variables.
  applyTheme(theme) {
    var builtIn = ['light', 'dark', 'auto'];
    if (builtIn.indexOf(theme) !== -1) {
      document.documentElement.setAttribute('data-theme', theme);
      if (this._themeLink) { this._themeLink.remove(); this._themeLink = null; }
    } else {
      // Custom theme CSS targets all data-theme values, so the attribute doesn't matter.
      // Set to 'custom' to avoid triggering built-in dark/light variables.
      document.documentElement.setAttribute('data-theme', 'custom');
      if (!this._themeLink) {
        this._themeLink = document.createElement('link');
        this._themeLink.rel = 'stylesheet';
        document.head.appendChild(this._themeLink);
      }
      this._themeLink.href = 'css/themes/' + encodeURIComponent(theme) + '.css';
    }
  },

  // Minimal viewer mode — open a single file with no file list or sidebar
  _viewerMode: false,
  _viewerFile: null,

  _checkViewerMode() {
    var params = new URLSearchParams(window.location.search);
    var file = params.get('file');
    var vnc = params.get('vnc');
    var term = params.get('term');
    if (file) {
      this._viewerMode = true;
      this._viewerFile = file;
    } else if (vnc) {
      this._viewerMode = true;
      this._viewerVnc = vnc;
      this._viewerVncPass = params.get('vncpass') || '';
      this._viewerVncUser = params.get('vncuser') || '';
    } else if (term) {
      this._viewerMode = true;
      this._viewerTerm = term;
    }
  },

  async _startViewerMode() {
    // Hide everything except the loading screen
    document.getElementById('login-screen').hidden = true;
    document.getElementById('app').hidden = true;
    var ls = document.getElementById('loading-screen');
    if (ls) ls.remove();

    // Set page background
    document.body.style.background = 'var(--color-bg, #fff)';

    // Need WinManager and Dialog for the viewer windows
    Dialog.init();
    WinManager.init();

    // Authenticate
    if (!Auth.restoreSession() || !(await Auth.verifySession())) {
      // Not logged in — redirect to main file manager to log in
      window.location.href = window.location.pathname;
      return;
    }

    // Fetch settings for theme
    try {
      var settingsResp = await fetch(this.davUrl + '_settings', {credentials: 'same-origin'});
      var settingsData = await settingsResp.json();
      if (settingsData.ok) {
        Auth.admin = !!settingsData.admin;
        Auth.theme = settingsData.theme || 'auto';
        Auth.cmTheme = settingsData.cmTheme || 'auto';
        this.applyTheme(Auth.theme);
      }
    } catch(e) {}

    // Tell WinManager to open the next window maximized and in viewer mode
    WinManager._nextViewerMode = true;

    if (this._viewerVnc) {
      // VNC mode
      var parts = this._viewerVnc.split(':');
      var vncHost = parts[0] || 'localhost';
      var vncPort = parts[1] || '5900';
      document.title = 'VNC — ' + vncHost + ':' + vncPort;
      this._openVncSession(vncHost, vncPort, this._viewerVncPass, this._viewerVncUser);
    } else if (this._viewerTerm) {
      // Terminal mode
      document.title = 'Terminal — ' + this._viewerTerm;
      this._openTerminalSession(this._viewerTerm);
    } else {
      // File viewer mode
      var href = this._viewerFile;
      var name = decodeURIComponent(href.substring(href.lastIndexOf('/') + 1));
      var item = {name: name, href: href, isDir: false};
      document.title = name + ' - File Manager';

      var opened = await Viewers.open(item);
      if (!opened) {
        WinManager._nextViewerMode = false;
        var a = document.createElement('a');
        a.href = href;
        a.download = name;
        document.body.appendChild(a);
        a.click();
        a.remove();
        return;
      }
    }

    WinManager._nextViewerMode = false;
  },

  init() {
    this.davUrl = document.documentElement.dataset.webdavUrl || '/dav/';

    this._checkViewerMode();
    if (this._viewerMode) {
      this._startViewerMode();
      return;
    }

    Dialog.init();
    WinManager.init();

    // Login form
    const loginForm = document.getElementById('login-form');
    loginForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const user = document.getElementById('login-user').value.trim();
      const pass = document.getElementById('login-pass').value;
      const errEl = document.getElementById('login-error');

      if (!user || !pass) { errEl.textContent = 'Please enter username and password'; return; }
      errEl.textContent = 'Signing in...';

      const result = await Auth.login(user, pass);
      if (result.ok) {
        errEl.textContent = '';
        if (result.requirePasswordChange) {
          await this._forcePasswordChange(pass);
          return;
        }
        this.showApp();
        await this._startApp();
        if (Auth.demoMode) {
          var secs = Auth.demoClearTime || 600;
          var timeStr = secs >= 120 ? Math.round(secs / 60) + ' minutes' : secs + ' seconds';
          Dialog.alert('Welcome to the demo! Files you upload or create will be automatically deleted after ' + timeStr + ' of inactivity. Copy files from the demo-files folder to get started.');
        }
      } else {
        errEl.textContent = result.error;
      }
    });

    // Try to restore session
    if (Auth.restoreSession()) {
      Auth.verifySession().then(valid => {
        if (valid) {
          this.showApp();
          this._startApp();
        } else {
          this.showLogin();
        }
      });
    } else {
      this.showLogin();
    }

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
      // Priority 1: Modal dialog
      if (Dialog.isOpen()) {
        if (e.key === 'Escape') Dialog.close();
        return;
      }

      // Priority 2: Focused window
      if (e.key === 'Escape') {
        var focusedWin = WinManager.getFocusedWindow();
        if (focusedWin) {
          WinManager.close(focusedWin.id);
          return;
        }
      }

      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT' || e.target.isContentEditable) return;

      // '/' key opens search bar — only when no window is focused
      if (e.key === '/' && !WinManager.getFocusedWindow()) {
        e.preventDefault();
        Search.showInput();
        return;
      }

      // Don't intercept navigation keys when a viewer window is focused
      var hasWin = WinManager.getFocusedWindow();
      if (hasWin && !e.ctrlKey && !e.metaKey && (e.key === 'ArrowDown' || e.key === 'ArrowUp' || e.key === ' ' || e.key === 'Enter' || e.key === 'Backspace')) return;

      if (e.key === 'Delete' && FileList.selected.size > 0) {
        this.deleteSelected();
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'a') {
        e.preventDefault();
        FileList.selectAll(true);
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'c') {
        if (FileList.selected.size > 0) {
          Clipboard.set(FileList.getSelected(), 'copy', FileList.currentPath);
          FileList.clearSelection();
        }
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'x') {
        if (FileList.selected.size > 0) {
          Clipboard.set(FileList.getSelected(), 'cut', FileList.currentPath);
          FileList.clearSelection();
        }
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'v') {
        if (Clipboard.hasItems()) {
          Clipboard.paste(FileList.currentPath);
        }
      }
      // Arrow key navigation (Finder-style)
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        FileList._handleArrowKey(e.key === 'ArrowDown' ? 1 : -1, e.shiftKey);
      }
      // Space — open/preview selected item
      if (e.key === ' ') {
        e.preventDefault();
        var sel = FileList.getSelected();
        if (sel.length === 1) FileList._openItem(sel[0]);
      }
      // Enter — inline rename
      if (e.key === 'Enter') {
        var sel = FileList.getSelected();
        if (sel.length === 1) {
          e.preventDefault();
          FileList._inlineRename(sel[0]);
        }
      }
      // Backspace — go to parent directory
      if (e.key === 'Backspace') {
        e.preventDefault();
        var parentUrl = FileList._getParentUrl();
        if (parentUrl) App.navigateTo(parentUrl);
      }
      if (e.key === 'Escape') {
        FileList.clearSelection();
        Clipboard.clear();
        this.hideContextMenu();
      }
    });

    // Close context menu on click outside
    document.addEventListener('click', () => this.hideContextMenu());

    // Hash navigation
    window.addEventListener('popstate', async () => {
      // If this popstate was triggered by WinManager.close() cleaning up history, skip
      if (WinManager._skipNextPop) {
        WinManager._skipNextPop = false;
        return;
      }

      // If a window is open, close the topmost one instead of navigating
      var topWin = WinManager.getFocusedWindow();
      if (topWin) {
        var closed = await WinManager.close(topWin.id, true);
        if (!closed) {
          // beforeClose prevented it (e.g. unsaved changes), re-push state
          history.pushState({ win: true }, '', location.hash);
        }
        return;
      }

      var raw = location.hash.substring(1);
      var path = raw ? (/^\/dav\//.test(raw) ? raw : '/dav' + raw) : '';
      if (path && path !== FileList.currentPath) {
        FileList.navigate(path, false);
        Tree.revealPath(path);
      }
    });
  },

  async _ensureDefaults() {
    const tryMkcol = async (url) => {
      try {
        const resp = await DavClient.send('PROPFIND', url, null, { 'Depth': '0' });
        if (resp.status !== 207 && resp.status !== 200) {
          await DavClient.mkcol(url);
        }
      } catch (e) {
        try { await DavClient.mkcol(url); } catch (e2) {}
      }
    };

    // Ensure /dav/shared/ exists
    await tryMkcol(this.davUrl + 'shared/');

    // If user home is empty (new account), create standard subdirectories
    try {
      const homeUrl = Auth.getUserHomeUrl();
      const items = await DavClient.list(homeUrl, 1);
      const children = items.filter(i => !i.isSelf);
      if (children.length === 0) {
        const dirs = ['Documents', 'Music', 'Pictures', 'Videos'];
        for (const dir of dirs) {
          try { await DavClient.mkcol(homeUrl + dir + '/'); } catch (e) {}
        }
      }
    } catch (e) {}
  },

  async _startApp() {
    FileList.init();
    Tree.init();
    Toolbar.init();
    Upload.init();
    Search.init();
    this._applyGridSettings();

    // Fetch user settings to populate Auth.groups
    try {
      const settingsResp = await fetch(this.davUrl + '_settings', { credentials: 'same-origin' });
      const settingsData = await settingsResp.json();
      if (settingsData.ok) {
        Auth.groups = settingsData.userGroups || [];
        Auth.admin = !!settingsData.admin;
        Auth.terminal = !!settingsData.terminal;
        Auth.vnc = !!settingsData.vnc;
        Auth.termTheme = settingsData.termTheme || 'auto';
        Auth.cmTheme = settingsData.cmTheme || 'auto';
        Auth.ooAutosave = typeof settingsData.ooAutosave === 'boolean' ? settingsData.ooAutosave : true;
        Auth.sshHosts = settingsData.sshHosts || [];
        Auth.demoMode = !!settingsData.demoMode;
        Auth.demoClearTime = settingsData.demoClearTime || 0;
        Auth.searchDirs = settingsData.searchDirs || [];
        Auth.mountNames = [];
        Auth.readOnlyMounts = [];
        Auth.theme = settingsData.theme || 'auto';
        this.applyTheme(Auth.theme);
        document.getElementById('terminal-btn').hidden = !settingsData.terminal;
        document.getElementById('vnc-btn').hidden = !settingsData.vnc;
      }
    } catch (e) {}

    // Fetch rclone mount names for zone detection
    try {
      var mountResp = await fetch(this.davUrl + '_rclone/mounts', { credentials: 'same-origin' });
      var mountData = await mountResp.json();
      if (mountData.ok && mountData.mounts) {
        Auth.mountNames = mountData.mounts
          .filter(function(m) { return m.mounted; })
          .map(function(m) { return m.name; });
        Auth.readOnlyMounts = mountData.mounts
          .filter(function(m) { return m.mounted && m.readOnly; })
          .map(function(m) { return m.name; });
      }
    } catch (e) {}

    // Fetch plugins
    try {
      var pluginResp = await fetch(this.davUrl + '_plugins', { credentials: 'same-origin' });
      var pluginData = await pluginResp.json();
      if (pluginData.ok) {
        Viewers._pluginExtMap = pluginData.extMap || {};
        Viewers._pluginMimeMap = pluginData.mimeMap || {};
        Viewers._plugins = {};
        (pluginData.plugins || []).forEach(function(p) { Viewers._plugins[p.name] = p; });
        // Store drop plugins for URL drop handling
        Upload._dropPlugins = (pluginData.dropPlugins || []).map(function(dp) {
          return {
            name: dp.name,
            patterns: (dp.patterns || []).map(function(p) { return new RegExp(p.source, p.flags); })
          };
        });
      }
    } catch (e) {}

    await this._ensureDefaults();
    await Tree.loadRoot();

    // Navigate to hash path or user home
    var rawHash = location.hash.substring(1);
    var hashPath = rawHash ? (/^\/dav\//.test(rawHash) ? rawHash : '/dav' + rawHash) : '';
    const startPath = hashPath || Auth.getUserHomeUrl();
    NavHistory.push(startPath);
    await FileList.navigate(startPath, false);
    location.hash = '#' + FileList.currentPath.replace(/^\/dav\//, '/');

    if (hashPath) {
      Tree.revealPath(hashPath);
    }
  },

  showLogin() {
    var ls = document.getElementById('loading-screen');
    if (ls) ls.remove();
    document.getElementById('login-screen').hidden = false;
    document.getElementById('app').hidden = true;
    document.getElementById('login-user').value = (typeof demoCredentials !== 'undefined' && demoCredentials.user) || '';
    document.getElementById('login-pass').value = (typeof demoCredentials !== 'undefined' && demoCredentials.pass) || '';
    document.getElementById('login-error').textContent = '';
    document.title = 'File Manager';

    // Check if any users/admins exist
    fetch(App.davUrl + '_status').then(function(r) { return r.json(); }).then(function(data) {
      if (!data.ok) return;
      var errEl = document.getElementById('login-error');
      if (!data.hasUsers) {
        errEl.innerHTML = '<strong>No user accounts exist.</strong><br>' +
          'Create an admin account from the command line:<br><br>' +
          '<code style="white-space:pre-wrap">./admin.sh add &lt;username&gt; &lt;password&gt;\n' +
          './admin.sh admin &lt;username&gt; true</code>';
      } else if (!data.hasAdmin) {
        errEl.innerHTML = '<strong>No administrator account exists.</strong><br>' +
          'Promote a user to admin from the command line:<br><br>' +
          '<code>./admin.sh admin &lt;username&gt; true</code>';
      }
    }).catch(function() {});
  },

  showApp() {
    var ls = document.getElementById('loading-screen');
    if (ls) ls.remove();
    document.getElementById('login-screen').hidden = true;
    document.getElementById('app').hidden = false;
    document.title = Auth.username + '@' + location.hostname + ' - File Manager';
  },

  async _forcePasswordChange(currentPassword) {
    var self = this;
    var wrap = document.createElement('div');
    wrap.innerHTML =
      '<p style="margin:0 0 12px;color:var(--color-fg-secondary)">Your administrator requires you to change your password before continuing.</p>' +
      '<div class="settings-field"><label>New Password</label><input type="password" id="force-new-pass" autocomplete="new-password"></div>' +
      '<div class="settings-field"><label>Confirm Password</label><input type="password" id="force-confirm-pass" autocomplete="new-password"></div>' +
      '<div class="settings-msg" id="force-pass-msg"></div>';

    await new Promise(function(resolve) {
      var okBtn = document.createElement('button');
      okBtn.className = 'btn btn-primary';
      okBtn.textContent = 'Change Password';

      function submit() {
        var msg = document.getElementById('force-pass-msg');
        var newPass = document.getElementById('force-new-pass').value;
        var confirmPass = document.getElementById('force-confirm-pass').value;
        if (!newPass) { msg.textContent = 'Please enter a new password'; msg.className = 'settings-msg error'; return; }
        if (newPass.length < 4) { msg.textContent = 'Password must be at least 4 characters'; msg.className = 'settings-msg error'; return; }
        if (newPass !== confirmPass) { msg.textContent = 'Passwords do not match'; msg.className = 'settings-msg error'; return; }
        if (newPass === currentPassword) { msg.textContent = 'New password must be different'; msg.className = 'settings-msg error'; return; }
        msg.textContent = 'Changing...';
        msg.className = 'settings-msg';
        fetch(self.davUrl + '_settings/password', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ currentPassword: currentPassword, newPassword: newPass })
        }).then(function(r) { return r.json(); }).then(function(data) {
          if (data.ok) {
            Dialog._beforeClose = null;
            Dialog.close();
            Toast.show('Password changed. Please log in with your new password.');
            Auth.logout();
            resolve();
          } else {
            msg.textContent = data.error || 'Failed';
            msg.className = 'settings-msg error';
          }
        }).catch(function() {
          msg.textContent = 'Connection error';
          msg.className = 'settings-msg error';
        });
      }

      okBtn.addEventListener('click', submit);
      wrap.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') submit();
      });

      Dialog.open('Password Change Required', wrap, { footer: [okBtn] });
      // No onClose/beforeClose — the only way out is to change the password
      // Override close to prevent dismissal
      Dialog._beforeClose = function() { return false; };
      document.getElementById('force-new-pass').focus();
    });
  },

  async navigateTo(path, pushHistory) {
    path = path.replace(/\/?$/, '/');
    await FileList.navigate(path, pushHistory);
    Tree.revealPath(path);
  },

  // VNC Remote Desktop
  async openVnc() {
    var wrap = document.createElement('div');
    wrap.style.cssText = 'display:flex;flex-direction:column;gap:8px';

    var hostLabel = document.createElement('label');
    hostLabel.textContent = 'VNC Host';
    var hostInput = document.createElement('input');
    hostInput.type = 'text';
    hostInput.className = 'dialog-input';
    hostInput.value = 'localhost';
    hostInput.placeholder = 'hostname or IP';

    var portLabel = document.createElement('label');
    portLabel.textContent = 'VNC Port';
    var portInput = document.createElement('input');
    portInput.type = 'text';
    portInput.className = 'dialog-input';
    portInput.value = '5900';
    portInput.placeholder = '5900';

    var userLabel = document.createElement('label');
    userLabel.textContent = 'Username (for macOS, leave blank for standard VNC)';
    var userInput = document.createElement('input');
    userInput.type = 'text';
    userInput.className = 'dialog-input';
    userInput.placeholder = 'username';
    userInput.autocomplete = 'off';

    var passLabel = document.createElement('label');
    passLabel.textContent = 'Password';
    var passInput = document.createElement('input');
    passInput.type = 'password';
    passInput.className = 'dialog-input';
    passInput.placeholder = 'VNC password';

    wrap.appendChild(hostLabel);
    wrap.appendChild(hostInput);
    wrap.appendChild(portLabel);
    wrap.appendChild(portInput);
    wrap.appendChild(userLabel);
    wrap.appendChild(userInput);
    wrap.appendChild(passLabel);
    wrap.appendChild(passInput);

    var self = this;
    var confirmed = await new Promise(function(resolve) {
      var connectBtn = document.createElement('button');
      connectBtn.className = 'btn btn-primary btn-sm';
      connectBtn.textContent = 'Connect';
      connectBtn.style.marginTop = '8px';
      connectBtn.addEventListener('click', function() {
        resolve('connect');
        Dialog.close();
      });
      var newTabBtn = document.createElement('button');
      newTabBtn.className = 'btn btn-primary btn-sm';
      newTabBtn.textContent = 'Connect and Open in New Tab';
      newTabBtn.style.marginTop = '8px';
      newTabBtn.addEventListener('click', function() {
        resolve('newtab');
        Dialog.close();
      });
      wrap.appendChild(connectBtn);
      wrap.appendChild(newTabBtn);
      Dialog.open('VNC Remote Desktop', wrap);
    });

    if (!confirmed) return;

    var host = hostInput.value.trim() || 'localhost';
    var port = portInput.value.trim() || '5900';
    var username = userInput.value.trim();
    var password = passInput.value;

    if (confirmed === 'newtab') {
      var vncParams = '?vnc=' + encodeURIComponent(host) + ':' + encodeURIComponent(port) +
        (password ? '&vncpass=' + encodeURIComponent(password) : '') +
        (username ? '&vncuser=' + encodeURIComponent(username) : '');
      window.open(window.location.pathname + vncParams, '_blank');
      return;
    }

    this._openVncSession(host, port, password, username);
  },

  _openVncSession(host, port, password, username) {
    var containerId = 'vnc-container-' + Date.now();
    var container = document.createElement('div');
    container.id = containerId;
    container.style.cssText = 'width:100%;height:100%;background:#000;overflow:hidden';

    var winTitle = 'VNC — ' + host + ':' + port;

    // Store RFB reference on window for cleanup
    var vncKey = 'vnc_rfb_' + containerId;

    // Visual flash feedback for header buttons
    function flashBtn(btn) {
      btn.style.opacity = '0.4';
      setTimeout(function() { btn.style.opacity = ''; }, 150);
    }

    // Type text into remote via sendKey (avoids clipboard issues)
    function vncTypeText(rfb, text) {
      for (var i = 0; i < text.length; i++) {
        var ch = text.charCodeAt(i);
        // X11 keysym: ASCII/Latin-1 chars map directly, Unicode uses 0x01000000 + codepoint
        var keysym = ch > 0xFF ? 0x01000000 | ch : ch;
        // newline → Return key
        if (ch === 10) keysym = 0xFF0D;
        // tab → Tab key
        else if (ch === 9) keysym = 0xFF09;
        rfb.sendKey(keysym);
      }
    }

    // Ctrl+Alt+Del button
    var cadBtn = document.createElement('button');
    cadBtn.className = 'modal-header-btn';
    cadBtn.title = 'Send Ctrl+Alt+Del';
    cadBtn.style.cssText = 'font-size:11px;min-width:auto;padding:2px 6px';
    cadBtn.innerHTML = 'C+A+D';
    cadBtn.addEventListener('click', function() {
      var rfb = window[vncKey];
      if (rfb) {
        flashBtn(cadBtn);
        rfb.sendCtrlAltDel();
      }
    });

    // Clipboard paste button (local → remote) — types text via sendKey
    var pasteBtn = document.createElement('button');
    pasteBtn.className = 'modal-header-btn';
    pasteBtn.title = 'Type local clipboard text into remote';
    pasteBtn.style.cssText = 'font-size:11px;min-width:auto;padding:2px 6px';
    pasteBtn.textContent = 'Paste';
    pasteBtn.addEventListener('click', async function() {
      var rfb = window[vncKey];
      if (!rfb) return;
      flashBtn(pasteBtn);
      var text;
      try {
        text = await navigator.clipboard.readText();
      } catch(e) {
        text = prompt('Text to type into remote:');
      }
      if (text) {
        vncTypeText(rfb, text);
        Toast.show('Typed ' + text.length + ' characters into remote');
      } else {
        Toast.show('Clipboard is empty');
      }
    });

    // Clipboard copy button (remote → local)
    var clipTextKey = 'vnc_clip_' + containerId;
    window[clipTextKey] = '';
    var copyBtn = document.createElement('button');
    copyBtn.className = 'modal-header-btn';
    copyBtn.title = 'Copy remote clipboard to local';
    copyBtn.style.cssText = 'font-size:11px;min-width:auto;padding:2px 6px';
    copyBtn.textContent = 'Copy';
    copyBtn.addEventListener('click', async function() {
      flashBtn(copyBtn);
      var text = window[clipTextKey];
      if (!text) {
        Toast.show('No remote clipboard received. Note: macOS Screen Sharing does not share its clipboard over VNC. This works with x11vnc and TigerVNC on Linux.');
        return;
      }
      try {
        await navigator.clipboard.writeText(text);
        Toast.show('Copied to local clipboard (' + text.length + ' chars)');
      } catch(e) {
        prompt('Remote clipboard text (select and copy):', text);
      }
    });

    // Zoom state
    var vncZoom = { level: 0, fit: true }; // level=0 means fit-to-window
    var zoomKey = 'vnc_zoom_' + containerId;
    window[zoomKey] = vncZoom;

    function applyVncZoom() {
      var rfb = window[vncKey];
      if (!rfb) return;
      var canvas = container.querySelector('canvas');
      if (!canvas) return;
      if (vncZoom.fit) {
        rfb.scaleViewport = true;
        canvas.style.transform = '';
        canvas.style.transformOrigin = '';
        container.style.overflow = 'hidden';
        zoomLabel.textContent = 'Fit';
      } else {
        rfb.scaleViewport = false;
        var scale = Math.pow(1.25, vncZoom.level);
        canvas.style.transform = 'scale(' + scale + ')';
        canvas.style.transformOrigin = '0 0';
        container.style.overflow = 'auto';
        zoomLabel.textContent = Math.round(scale * 100) + '%';
      }
    }

    // Zoom label
    var zoomLabel = document.createElement('span');
    zoomLabel.style.cssText = 'font-size:11px;opacity:0.7;margin:0 2px;min-width:32px;text-align:center';
    zoomLabel.textContent = 'Fit';

    // Zoom out button
    var zoomOutBtn = document.createElement('button');
    zoomOutBtn.className = 'modal-header-btn';
    zoomOutBtn.title = 'Zoom out';
    zoomOutBtn.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><path d="M7 1a6 6 0 104.45 10.04l3.26 3.25 1.06-1.06-3.25-3.26A6 6 0 007 1zm0 1.5a4.5 4.5 0 110 9 4.5 4.5 0 010-9zM4.5 6.25v1.5h5v-1.5h-5z"/></svg>';
    zoomOutBtn.addEventListener('click', function() {
      vncZoom.fit = false;
      vncZoom.level = Math.max(vncZoom.level - 1, -4);
      applyVncZoom();
    });

    // Zoom in button
    var zoomInBtn = document.createElement('button');
    zoomInBtn.className = 'modal-header-btn';
    zoomInBtn.title = 'Zoom in';
    zoomInBtn.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><path d="M7 1a6 6 0 104.45 10.04l3.26 3.25 1.06-1.06-3.25-3.26A6 6 0 007 1zm0 1.5a4.5 4.5 0 110 9 4.5 4.5 0 010-9zM6.25 4.5v2h-2v1h2v2h1.5v-2h2v-1h-2v-2h-1.5z"/></svg>';
    zoomInBtn.addEventListener('click', function() {
      vncZoom.fit = false;
      vncZoom.level = Math.min(vncZoom.level + 1, 6);
      applyVncZoom();
    });

    // Fit-to-window button
    var fitBtn = document.createElement('button');
    fitBtn.className = 'modal-header-btn';
    fitBtn.title = 'Fit to window';
    fitBtn.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><path d="M2 2v5l1.8-1.8L6.5 8 8 6.5 5.2 3.8 7 2H2zm12 12v-5l-1.8 1.8L9.5 8 8 9.5l2.8 2.7L9 14h5z"/></svg>';
    fitBtn.addEventListener('click', function() {
      vncZoom.fit = true;
      vncZoom.level = 0;
      applyVncZoom();
    });

    var winId = WinManager.open(winTitle, container, {
      type: 'vnc', full: true, noPadding: true,
      headerActions: [cadBtn, pasteBtn, copyBtn, zoomOutBtn, zoomLabel, zoomInBtn, fitBtn],
      onClose: function() {
        if (window[vncKey]) {
          try { window[vncKey].disconnect(); } catch(e) {}
          window[vncKey] = null;
        }
      }
    });

    // Load noVNC and connect directly
    var scheme = location.protocol === 'https:' ? 'wss' : 'ws';
    var wsUrl = scheme + '://' + location.host +
      '/wsapps/vnc/vnc.js?host=' + encodeURIComponent(host) +
      '&port=' + encodeURIComponent(port);
    var escapedPass = password.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    var escapedUser = (username || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");

    var script = document.createElement('script');
    script.type = 'module';
    script.textContent =
      "import RFB from './js/noVNC/core/rfb.js';\n" +
      "var container = document.getElementById('" + containerId + "');\n" +
      "try {\n" +
      "  var creds = { password: '" + escapedPass + "' };\n" +
      "  if ('" + escapedUser + "') creds.username = '" + escapedUser + "';\n" +
      "  var rfb = new RFB(container, '" + wsUrl + "',\n" +
      "    { credentials: creds });\n" +
      "  rfb.scaleViewport = true;\n" +
      "  rfb.resizeSession = true;\n" +
      "  rfb.showDotCursor = true;\n" +
      "  rfb.addEventListener('connect', function() {\n" +
      "    container.style.background = 'none';\n" +
      "  });\n" +
      "  rfb.addEventListener('disconnect', function(e) {\n" +
      "    var msg = (e.detail && e.detail.reason) || 'VNC connection lost';\n" +
      "    if (!e.detail.clean) {\n" +
      "      container.innerHTML = '<div style=\"color:#f88;padding:20px;font-family:sans-serif\">' + msg + '</div>';\n" +
      "    }\n" +
      "  });\n" +
      "  rfb.addEventListener('credentialsrequired', function() {\n" +
      "    var pw = prompt('VNC password required:');\n" +
      "    if (pw) rfb.sendCredentials({ password: pw });\n" +
      "    else rfb.disconnect();\n" +
      "  });\n" +
      "  rfb.addEventListener('clipboard', function(e) {\n" +
      "    if (e.detail && e.detail.text) {\n" +
      "      window['" + clipTextKey + "'] = e.detail.text;\n" +
      "      try { navigator.clipboard.writeText(e.detail.text); } catch(ex) {}\n" +
      "    }\n" +
      "  });\n" +
      "  window['" + vncKey + "'] = rfb;\n" +
      "} catch(e) {\n" +
      "  container.innerHTML = '<div style=\"color:#f88;padding:20px;font-family:sans-serif\">Failed to connect: ' + e.message + '</div>';\n" +
      "}\n";
    document.body.appendChild(script);
  },

  // Context menu
  showContextMenu(e, item) {
    const menu = document.getElementById('context-menu');
    menu.hidden = false;

    // Position menu — measure actual size after showing
    const menuW = menu.offsetWidth || 180;
    const menuH = menu.offsetHeight || 250;
    var statusbarH = 28;
    var maxH = window.innerHeight - 20;
    menu.style.maxHeight = maxH + 'px';
    const x = Math.min(e.clientX, window.innerWidth - menuW);
    var y = Math.min(e.clientY, window.innerHeight - menuH - statusbarH);
    if (menuH >= maxH) y = 10;
    menu.style.left = x + 'px';
    menu.style.top = y + 'px';

    // Open in New Tab — show for single non-directory items
    var newTabItem = menu.querySelector('[data-action="open-new-tab"]');
    newTabItem.hidden = item.isDir || FileList.selected.size > 1;

    // Enable/disable paste
    const pasteItem = menu.querySelector('[data-action="paste"]');
    pasteItem.disabled = !Clipboard.hasItems();

    // Toggle trash/restore depending on current location
    const inTrash = this.isInTrash();
    var onMount = DragDrop._getZone(FileList.currentPath).indexOf('mount:') === 0;
    var trashItem = menu.querySelector('[data-action="trash"]');
    trashItem.hidden = inTrash;
    trashItem.disabled = onMount;
    menu.querySelector('[data-action="restore"]').hidden = !inTrash;

    // Share — only for single items, not in trash
    var shareItem = menu.querySelector('[data-action="share"]');
    shareItem.hidden = inTrash || FileList.selected.size > 1;
    if (!shareItem.hidden) {
      shareItem.textContent = item.shared ? 'Edit Share' : 'Share to Web';
    }

    // Edit Image — show only for editable image formats (single item)
    var editImgItem = menu.querySelector('[data-action="edit-image"]');
    var isEditableImage = !item.isDir && FileList.selected.size <= 1 &&
        /\.(png|jpe?g|webp)$/i.test(item.name);
    editImgItem.hidden = !isEditableImage;

    // Edit HTML — show only for HTML files (single item)
    var editHtmlItem = menu.querySelector('[data-action="edit-html"]');
    var isEditableHtml = !item.isDir && FileList.selected.size <= 1 &&
        /\.html?$/i.test(item.name);
    editHtmlItem.hidden = !isEditableHtml;

    // Archive submenu — hide in trash
    var arcSep = menu.querySelector('.ctx-archive-sep');
    var arcParent = menu.querySelector('.ctx-archive-parent');
    arcSep.hidden = inTrash;
    arcParent.hidden = inTrash;

    // Playlist submenu — show only for audio files
    var plSep = menu.querySelector('.ctx-playlist-sep');
    var plParent = menu.querySelector('.ctx-playlist-parent');
    var actionItems = this._getActionItems(item);
    var hasAudio = actionItems.some(function(it) { return Viewers.getType(it) === 'audio'; });
    plSep.hidden = !hasAudio;
    plParent.hidden = !hasAudio;
    if (hasAudio) {
      this._populatePlaylistSubmenu(actionItems);
    }

    // Search indexing toggle — only for directories, single selection
    var searchSep = menu.querySelector('.ctx-search-sep');
    var searchToggle = menu.querySelector('[data-action="toggle-search"]');
    searchSep.hidden = true;
    searchToggle.hidden = true;
    if (item.isDir && FileList.selected.size <= 1) {
      // Get the dav-relative path for this directory
      var itemDavPath = item.href.replace(App.davUrl.replace(/\/$/, ''), '').replace(/\/$/, '');
      // Check search status asynchronously, update menu item
      (function(davPath, sep, btn) {
        fetch(App.davUrl + '_search/status?path=' + encodeURIComponent(davPath), {credentials: 'same-origin'})
          .then(function(r) { return r.json(); })
          .then(function(data) {
            if (!data.ok) return;
            if (data.mountBlocked) return; // mounted dirs: don't show toggle
            sep.hidden = false;
            btn.hidden = false;
            if (data.parentIndexed) {
              btn.textContent = '\u2713 Search Indexing (inherited)';
              btn.disabled = true;
              btn.classList.add('ctx-always-on');
            } else if (data.indexed) {
              btn.textContent = '\u2713 Search Indexing';
              btn.disabled = false;
              btn.classList.remove('ctx-always-on');
            } else {
              btn.textContent = '    Search Indexing';
              btn.disabled = false;
              btn.classList.remove('ctx-always-on');
            }
            btn._searchPath = davPath;
            btn._searchEnabled = data.indexed;
          }).catch(function() {});
      })(itemDavPath, searchSep, searchToggle);
    }

    this._wireSubmenus(menu);

    // Wire actions
    menu.onclick = (ev) => {
      // Don't close on submenu parent hover
      var subItem = ev.target.closest('.ctx-sub-item');
      if (subItem) {
        ev.stopPropagation();
        menu.hidden = true;
        // Archive submenu
        if (subItem.dataset.arcFormat) {
          this._createArchive(actionItems, subItem.dataset.arcFormat);
          return;
        }
        this._handlePlaylistAction(subItem.dataset.plAction, subItem.dataset.plPath, actionItems);
        return;
      }
      const btn = ev.target.closest('[data-action]');
      if (!btn || btn.classList.contains('ctx-playlist-parent') || btn.classList.contains('ctx-archive-parent')) return;
      menu.hidden = true;
      this._handleAction(btn.dataset.action, item);
    };
  },

  _showSubmenu(parent) {
    var sub = parent.querySelector('.ctx-submenu');
    if (!sub) return;
    sub.style.display = 'block';
    // Position relative to parent item, using fixed coords
    var pr = parent.getBoundingClientRect();
    // Default: to the right of the parent
    var left = pr.right;
    var top = pr.top;
    // Measure submenu
    var sr = sub.getBoundingClientRect();
    // Flip left if overflows right edge
    if (left + sr.width > window.innerWidth) {
      left = pr.left - sr.width;
    }
    // Shift up if overflows bottom edge
    if (top + sr.height > window.innerHeight) {
      top = window.innerHeight - sr.height - 4;
    }
    if (top < 0) top = 0;
    sub.style.left = left + 'px';
    sub.style.top = top + 'px';
  },

  _hideSubmenu(parent) {
    var sub = parent.querySelector('.ctx-submenu');
    if (sub) sub.style.display = 'none';
  },

  _wireSubmenus(menu) {
    var self = this;
    menu.querySelectorAll('.ctx-archive-parent, .ctx-playlist-parent').forEach(function(parent) {
      parent.addEventListener('mouseenter', function() { self._showSubmenu(parent); });
      parent.addEventListener('mouseleave', function() { self._hideSubmenu(parent); });
    });
  },

  hideContextMenu() {
    document.getElementById('context-menu').hidden = true;
    var bg = document.getElementById('bg-ctx-menu');
    if (bg) bg.remove();
  },

  showBgContextMenu(e) {
    this.hideContextMenu();
    var old = document.getElementById('bg-ctx-menu');
    if (old) old.remove();

    var menu = document.createElement('div');
    menu.id = 'bg-ctx-menu';
    menu.className = 'context-menu';
    menu.setAttribute('role', 'menu');

    var self = this;

    function addItem(label, action, opts) {
      opts = opts || {};
      var btn = document.createElement('button');
      btn.className = 'ctx-item';
      if (opts.prefix) btn.textContent = opts.prefix + label;
      else btn.textContent = label;
      if (opts.disabled) btn.disabled = true;
      btn.addEventListener('click', function() {
        menu.remove();
        action();
      });
      menu.appendChild(btn);
      return btn;
    }

    function addSep() {
      var hr = document.createElement('hr');
      hr.className = 'ctx-separator';
      menu.appendChild(hr);
    }

    // Show/Hide Hidden Files
    addItem((FileList.showHidden ? 'Hide' : 'Show') + ' Hidden Files', function() {
      FileList.toggleHidden();
    });

    // Switch view
    addItem('Switch to ' + (FileList.viewMode === 'detail' ? 'Grid' : 'Detail') + ' View', function() {
      FileList.toggleView();
    });

    // Sort by submenu
    var sortParent = document.createElement('div');
    sortParent.className = 'ctx-item ctx-archive-parent';
    sortParent.textContent = 'Sort by \u25B8';
    var sortSub = document.createElement('div');
    sortSub.className = 'ctx-submenu';
    var sortOpts = [
      { key: 'name', label: 'Name' },
      { key: 'size', label: 'Size' },
      { key: 'type', label: 'Type' },
      { key: 'date', label: 'Modified' }
    ];
    sortOpts.forEach(function(s) {
      var btn = document.createElement('button');
      btn.className = 'ctx-sub-item';
      btn.textContent = (FileList.sortKey === s.key ? '\u2713 ' : '    ') + s.label;
      btn.addEventListener('click', function() {
        menu.remove();
        FileList.toggleSort(s.key);
      });
      sortSub.appendChild(btn);
    });
    sortParent.appendChild(sortSub);
    menu.appendChild(sortParent);

    addSep();

    // Upload
    addItem('Upload Files', function() { Upload.pickFiles(); });

    // New Folder
    addItem('New Folder', function() { self.newFolder(); });

    // New File submenu
    var fileParent = document.createElement('div');
    fileParent.className = 'ctx-item ctx-archive-parent';
    fileParent.textContent = 'New File \u25B8';
    var fileSub = document.createElement('div');
    fileSub.className = 'ctx-submenu';
    var fileTypes = [
      { label: 'Text (.txt)', ext: '.txt' },
      { label: 'HTML (.html)', ext: '.html' },
      { label: 'Playlist (.m3u)', ext: '.m3u' },
      null,
      { label: 'Word (.docx)', ext: '.docx' },
      { label: 'Excel (.xlsx)', ext: '.xlsx' },
      { label: 'PowerPoint (.pptx)', ext: '.pptx' },
      null,
      { label: 'Writer (.odt)', ext: '.odt' },
      { label: 'Calc (.ods)', ext: '.ods' },
      { label: 'Impress (.odp)', ext: '.odp' },
      null,
      { label: 'Other...', ext: null }
    ];
    fileTypes.forEach(function(ft) {
      if (!ft) {
        var hr = document.createElement('hr');
        hr.className = 'ctx-separator';
        fileSub.appendChild(hr);
        return;
      }
      var btn = document.createElement('button');
      btn.className = 'ctx-sub-item';
      btn.textContent = ft.label;
      btn.addEventListener('click', function() {
        menu.remove();
        if (ft.ext) {
          self._newFileWithExt(ft.ext);
        } else {
          self.newFile();
        }
      });
      fileSub.appendChild(btn);
    });
    fileParent.appendChild(fileSub);
    menu.appendChild(fileParent);

    addSep();

    // Terminal (if enabled)
    if (Auth.terminal) {
      addItem('Terminal', function() { self.openTerminal(); });
    }

    // Settings
    addItem('Settings', function() { self.openSettings(); });

    document.body.appendChild(menu);

    // Position
    var mw = menu.offsetWidth;
    var mh = menu.offsetHeight;
    var statusbarH = 28;
    var x = Math.min(e.clientX, window.innerWidth - mw);
    var y = Math.min(e.clientY, window.innerHeight - mh - statusbarH);
    menu.style.left = x + 'px';
    menu.style.top = y + 'px';
    menu.hidden = false;

    self._wireSubmenus(menu);

    // Close on click outside or Escape
    var closeMenu = function(ev) {
      if (!menu.contains(ev.target)) {
        menu.remove();
        document.removeEventListener('mousedown', closeMenu);
        document.removeEventListener('keydown', closeKey);
      }
    };
    var closeKey = function(ev) {
      if (ev.key === 'Escape') {
        menu.remove();
        document.removeEventListener('mousedown', closeMenu);
        document.removeEventListener('keydown', closeKey);
      }
    };
    setTimeout(function() {
      document.addEventListener('mousedown', closeMenu);
      document.addEventListener('keydown', closeKey);
    }, 0);
  },

  async _newFileWithExt(ext) {
    var name = await Dialog.prompt('New File', 'untitled' + ext, 'File name');
    if (!name) return;
    if (!isValidFileName(name)) { Toast.error('Invalid file name'); return; }
    var url = FileList.currentPath + encodeURIComponent(name);
    try {
      var resp = await DavClient.send('PUT', url, '');
      if (resp.status === 403) { Toast.error('Permission denied'); return; }
      if (!isDavOk(resp)) { Toast.error('Failed to create file'); return; }
      Toast.success('Created file "' + name + '"');
      FileList.reload();
    } catch (e) {
      Toast.error('Failed to create file');
    }
  },

  _populatePlaylistSubmenu: function(audioItems) {
    var sub = document.getElementById('ctx-playlist-sub');
    sub.innerHTML = '<div class="ctx-sub-item ctx-sub-loading">Loading...</div>';

    // Fetch recent playlists from Music dir
    fetch(this.davUrl + '_playlist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'list' })
    }).then(function(r) { return r.json(); }).then(function(data) {
      var html = '';
      if (audioItems.length > 1) {
        html += '<div class="ctx-sub-item" data-pl-action="new-from-selection">New Playlist from Selection</div>';
        html += '<div class="ctx-sub-sep"></div>';
      }
      if (data.ok && data.playlists && data.playlists.length > 0) {
        for (var i = 0; i < data.playlists.length; i++) {
          var pl = data.playlists[i];
          html += '<div class="ctx-sub-item" data-pl-action="add-to" data-pl-path="' +
            pl.path.replace(/"/g, '&quot;') + '">' + pl.name.replace(/</g, '&lt;') + '</div>';
        }
        html += '<div class="ctx-sub-sep"></div>';
      }
      html += '<div class="ctx-sub-item" data-pl-action="browse">Browse...</div>';
      html += '<div class="ctx-sub-item" data-pl-action="new">New Playlist...</div>';
      if (PlaylistBuilder.isOpen()) {
        html += '<div class="ctx-sub-sep"></div>';
        html += '<div class="ctx-sub-item" data-pl-action="add-to-builder">Add to Current Builder</div>';
      }
      sub.innerHTML = html;
    }).catch(function() {
      sub.innerHTML =
        '<div class="ctx-sub-item" data-pl-action="new">New Playlist...</div>';
    });
  },

  _handlePlaylistAction: function(action, plPath, items) {
    var audioOnly = items.filter(function(it) { return Viewers.getType(it) === 'audio'; });
    var tracks = audioOnly.map(function(it) {
      return { path: it.href, name: it.name, title: it.name, duration: 0 };
    });

    if (action === 'new' || action === 'new-from-selection') {
      PlaylistBuilder.open('New Playlist', tracks);
    } else if (action === 'add-to') {
      // Append to existing playlist file via API
      fetch(this.davUrl + '_playlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'append', path: plPath, tracks: tracks })
      }).then(function(r) { return r.json(); }).then(function(data) {
        if (!data.ok) Dialog.alert('Failed: ' + (data.error || 'Unknown error'));
      }).catch(function(e) { Dialog.alert('Failed: ' + e.message); });
    } else if (action === 'browse') {
      // Open a simple file picker for .m3u files
      this._browseForPlaylist(tracks);
    } else if (action === 'add-to-builder') {
      for (var i = 0; i < tracks.length; i++) {
        PlaylistBuilder.addTrack(tracks[i]);
      }
    }
  },

  _browseForPlaylist: async function(tracks) {
    // Simple prompt for now — user enters path to .m3u file
    var path = await Dialog.prompt('Path to .m3u file', App.davUrl + Auth.username + '/Music/', 'e.g. /dav/user/Music/playlist.m3u');
    if (!path) return;
    if (!/\.m3u$/i.test(path)) path += '.m3u';
    // Check if file exists, if not create it
    try {
      var resp = await fetch(path, { method: 'HEAD' });
      if (!resp.ok) {
        // Create new file
        await fetch(path, { method: 'PUT', body: '#EXTM3U\n', headers: { 'Content-Type': 'audio/x-mpegurl' } });
      }
      // Append tracks
      var appendResp = await fetch(App.davUrl + '_playlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'append', path: path, tracks: tracks })
      });
      var data = await appendResp.json();
      if (!data.ok) Dialog.alert('Failed: ' + (data.error || 'Unknown error'));
    } catch(e) {
      Dialog.alert('Failed: ' + e.message);
    }
  },

  // Get the effective items for a context menu action:
  // If the right-clicked item is in the selection, operate on all selected items.
  // Otherwise operate on just the right-clicked item.
  _getActionItems(item) {
    if (FileList.selected.has(item.href) && FileList.selected.size > 1) {
      return FileList.getSelected();
    }
    return [item];
  },

  async _handleAction(action, item) {
    const items = this._getActionItems(item);
    switch (action) {
      case 'open':
        // Open only applies to the single right-clicked item
        if (item.isDir) {
          this.navigateTo(item.href);
        } else {
          Viewers.open(item) || this.downloadFile(item);
        }
        break;
      case 'open-new-tab':
        window.open(window.location.pathname + '?file=' + encodeURIComponent(item.href), '_blank');
        break;
      case 'download':
        for (const i of items.filter(i => !i.isDir)) {
          this.downloadFile(i);
          await new Promise(r => setTimeout(r, 200));
        }
        break;
      case 'rename':
        // Rename only makes sense for a single item
        await this._rename(item);
        break;
      case 'copy':
        Clipboard.set(items, 'copy', FileList.currentPath);
        break;
      case 'cut':
        Clipboard.set(items, 'cut', FileList.currentPath);
        break;
      case 'paste':
        Clipboard.paste(FileList.currentPath);
        break;
      case 'trash':
        await this._trashItems(items);
        break;
      case 'restore':
        await this._restoreItems(items);
        break;
      case 'delete':
        await this._deleteItems(items);
        break;
      case 'create-link':
        await this._createLink(item);
        break;
      case 'share':
        this._showShareDialog(item);
        break;
      case 'info':
        this._showInfoDialog(item);
        break;
      case 'edit-image':
        Viewers._openImageEditor(item);
        break;
      case 'edit-html':
        Viewers._openHtmlEditor(item);
        break;
      case 'toggle-search': {
        var toggleBtn = document.querySelector('[data-action="toggle-search"]');
        var searchPath = toggleBtn._searchPath;
        var isEnabled = toggleBtn._searchEnabled;
        if (!searchPath) break;
        try {
          var resp = await fetch(App.davUrl + '_search/toggle', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ path: searchPath, enable: !isEnabled })
          });
          var data = await resp.json();
          if (data.ok) {
            Toast.success(data.message);
            // Refresh searchDirs
            var sResp = await fetch(App.davUrl + '_settings', { credentials: 'same-origin' });
            var sData = await sResp.json();
            if (sData.ok) Auth.searchDirs = sData.searchDirs || [];
            Search.updateButton();
          } else {
            Toast.error(data.error || 'Failed');
          }
        } catch(e) {
          Toast.error('Failed: ' + e.message);
        }
        break;
      }
    }
    FileList.clearSelection();
  },

  downloadFile(item) {
    const a = document.createElement('a');
    a.href = item.href;
    a.download = item.name;
    document.body.appendChild(a);
    a.click();
    a.remove();
  },

  async downloadSelected() {
    const items = FileList.getSelected().filter(i => !i.isDir);
    for (const item of items) {
      this.downloadFile(item);
      await new Promise(r => setTimeout(r, 200)); // Small delay between downloads
    }
    FileList.clearSelection();
  },

  async deleteSelected() {
    const items = FileList.getSelected();
    await this._deleteItems(items);
    FileList.clearSelection();
  },

  async _deleteItems(items) {
    if (items.length === 0) return;
    var msg;
    if (items.length === 1) {
      msg = 'Delete "' + items[0].name + '"?';
      if (items[0].isDir) {
        try {
          var dirList = await DavClient.list(items[0].href, 1);
          var children = dirList.filter(function(e) { return !e.isSelf; });
          if (children.length > 0) {
            var files = children.filter(function(e) { return !e.isDir; }).length;
            var dirs = children.filter(function(e) { return e.isDir; }).length;
            var parts = [];
            if (files) parts.push(files + ' file' + (files !== 1 ? 's' : ''));
            if (dirs) parts.push(dirs + ' folder' + (dirs !== 1 ? 's' : ''));
            msg = 'Delete "' + items[0].name + '" containing ' + parts.join(' and ') + '?';
          }
        } catch(e) {}
      }
    } else {
      msg = 'Delete ' + items.length + ' items?';
    }
    const ok = await Dialog.confirm(msg, 'Delete', true);
    if (!ok) return;

    var errorMsgs = [];
    for (const item of items) {
      try {
        const resp = await DavClient.del(item.href);
        if (!isDavOk(resp)) {
          var txt = await resp.text().catch(function() { return ''; });
          errorMsgs.push(item.name + ': ' + (txt || 'Error ' + resp.status));
        }
      } catch (e) { errorMsgs.push(item.name + ': ' + (e.message || 'Failed')); }
    }

    if (errorMsgs.length) Toast.error(errorMsgs.join('\n'));
    else Toast.success('Deleted ' + items.length + ' item(s)');
    FileList.reload();
    Tree.refresh(FileList.currentPath);
  },

  getTrashUrl() {
    return this.davUrl + Auth.username + '/trash/';
  },

  isInTrash() {
    return FileList.currentPath.indexOf(this.getTrashUrl()) === 0;
  },

  async _ensureTrashDir() {
    const trashUrl = this.getTrashUrl();
    try {
      const resp = await DavClient.send('PROPFIND', trashUrl, null, { 'Depth': '0' });
      if (resp.status === 404) {
        await DavClient.mkcol(trashUrl);
      }
    } catch (e) {
      await DavClient.mkcol(trashUrl);
    }
  },

  async _trashItems(items) {
    if (items.length === 0) return;
    await this._ensureTrashDir();
    const trashUrl = this.getTrashUrl();
    var errorMsgs = [];

    for (const item of items) {
      const destUrl = trashUrl + encodeURIComponent(item.name) + (item.isDir ? '/' : '');
      try {
        // Store original location in a custom header the server can use for metadata
        const resp = await DavClient.copyMove('MOVE', item.href, destUrl, false);
        if (resp.status === 412) {
          // Name collision in trash — add timestamp suffix
          const ts = Date.now();
          const dot = item.name.lastIndexOf('.');
          const trashName = dot > 0
            ? item.name.substring(0, dot) + '.' + ts + item.name.substring(dot)
            : item.name + '.' + ts;
          const altUrl = trashUrl + encodeURIComponent(trashName) + (item.isDir ? '/' : '');
          const resp2 = await DavClient.copyMove('MOVE', item.href, altUrl, false);
          if (!isDavOk(resp2)) {
            var txt2 = await resp2.text().catch(function() { return ''; });
            errorMsgs.push(item.name + ': ' + (txt2 || 'Error ' + resp2.status));
          }
        } else if (!isDavOk(resp)) {
          var txt = await resp.text().catch(function() { return ''; });
          errorMsgs.push(item.name + ': ' + (txt || 'Error ' + resp.status));
        }
      } catch (e) {
        console.warn('Trash move failed:', item.name, e);
        errorMsgs.push(item.name + ': ' + (e.message || 'Failed'));
      }
    }

    // Store original paths via PROPPATCH so restore knows where to put them back
    // We use a simple custom dead property for this
    for (const item of items) {
      const originalDir = FileList.currentPath;
      // Find the item in trash (may have been renamed with timestamp)
      const trashItems = await DavClient.list(trashUrl, 1);
      const trashed = trashItems.find(t => !t.isSelf &&
        (t.name === item.name || t.name.indexOf(item.name.replace(/\.[^.]+$/, '')) === 0));
      if (trashed) {
        // Set the original path as a dead property via PROPPATCH
        const propBody = '<?xml version="1.0" encoding="UTF-8"?>' +
          '<D:propertyupdate xmlns:D="DAV:" xmlns:T="trash:">' +
          '<D:set><D:prop><T:original-location>' +
          this._escXml(originalDir) +
          '</T:original-location></D:prop></D:set></D:propertyupdate>';
        await DavClient.send('PROPPATCH', trashed.href, propBody, {
          'Content-Type': 'text/xml; charset=utf-8'
        });
      }
    }

    if (errorMsgs.length) Toast.error(errorMsgs.join('\n'));
    else Toast.success('Moved ' + items.length + ' item(s) to trash');
    FileList.reload();
    Tree.refresh(FileList.currentPath);
    Tree.refresh(trashUrl);
  },

  async _restoreItems(items) {
    if (items.length === 0) return;
    let errors = 0;
    let restored = 0;

    for (const item of items) {
      // Get original location from dead property
      const propBody = '<?xml version="1.0" encoding="UTF-8"?>' +
        '<D:propfind xmlns:D="DAV:" xmlns:T="trash:"><D:prop>' +
        '<T:original-location/></D:prop></D:propfind>';
      let originalDir = null;
      try {
        const resp = await DavClient.send('PROPFIND', item.href, propBody, {
          'Depth': '0', 'Content-Type': 'text/xml; charset=utf-8'
        });
        const text = await resp.text();
        const match = text.match(/<[^>]*original-location[^>]*>([^<]+)</);
        if (match) originalDir = match[1];
      } catch (e) {}

      if (!originalDir) {
        // Fall back to user home if we can't find original location
        originalDir = Auth.getUserHomeUrl();
      }

      // Strip timestamp suffix if added during trash (e.g. file.1234567890.txt → file.txt)
      let restoreName = item.name;
      const tsMatch = restoreName.match(/^(.+)\.\d{13}(\.[^.]+)$/);
      if (tsMatch) restoreName = tsMatch[1] + tsMatch[2];

      const destUrl = originalDir + encodeURIComponent(restoreName) + (item.isDir ? '/' : '');
      try {
        const resp = await DavClient.copyMove('MOVE', item.href, destUrl, false);
        if (resp.status === 412) {
          Toast.error('"' + restoreName + '" already exists in original location');
          errors++;
        } else if (!isDavOk(resp)) {
          errors++;
        } else {
          restored++;
          // Clean up the dead property
          const removeBody = '<?xml version="1.0" encoding="UTF-8"?>' +
            '<D:propertyupdate xmlns:D="DAV:" xmlns:T="trash:">' +
            '<D:remove><D:prop><T:original-location/></D:prop></D:remove></D:propertyupdate>';
          await DavClient.send('PROPPATCH', destUrl, removeBody, {
            'Content-Type': 'text/xml; charset=utf-8'
          }).catch(() => {});
        }
      } catch (e) {
        console.warn('Restore failed for', item.name, ':', e);
        errors++;
      }
    }

    if (restored && !errors) Toast.success('Restored ' + restored + ' item(s)');
    else if (restored && errors) Toast.info('Restored ' + restored + ', failed ' + errors);
    else if (errors) Toast.error(errors + ' item(s) failed to restore');
    FileList.reload();
    Tree.refresh(FileList.currentPath);
  },

  async _emptyTrash() {
    const trashUrl = this.getTrashUrl();
    const ok = await Dialog.confirm('Permanently delete all items in trash?', 'Empty Trash', true);
    if (!ok) return;

    try {
      const items = await DavClient.list(trashUrl, 1);
      const toDelete = items.filter(i => !i.isSelf);
      let errors = 0;
      for (const item of toDelete) {
        try {
          const resp = await DavClient.del(item.href);
          if (!isDavOk(resp)) errors++;
        } catch (e) { errors++; }
      }
      if (errors) Toast.error(errors + ' item(s) failed to delete');
      else Toast.success('Trash emptied');
    } catch (e) {
      Toast.error('Failed to empty trash');
    }
    FileList.reload();
    Tree.refresh(trashUrl);
  },

  _escXml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  },

  async trashSelected() {
    const items = FileList.getSelected();
    await this._trashItems(items);
    FileList.clearSelection();
  },

  async restoreSelected() {
    const items = FileList.getSelected();
    await this._restoreItems(items);
    FileList.clearSelection();
  },

  async _rename(item) {
    const newName = await Dialog.prompt('Rename', item.name);
    if (!newName || newName === item.name) return;
    if (!isValidFileName(newName)) { Toast.error('Invalid file name'); return; }

    const destUrl = FileList.currentPath + encodeURIComponent(newName) + (item.isDir ? '/' : '');
    try {
      const resp = await DavClient.copyMove('MOVE', item.href, destUrl, false);
      if (resp.status === 412) {
        Toast.error('A file with that name already exists');
        return;
      }
      if (!isDavOk(resp)) {
        Toast.error('Rename failed');
        return;
      }
      Toast.success('Renamed to ' + newName);
      FileList.reload();
      Tree.refresh(FileList.currentPath);
    } catch (e) {
      Toast.error('Rename failed');
    }
  },

  async newFolder() {
    const name = await Dialog.prompt('New Folder', '', 'Folder name');
    if (!name) return;
    if (!isValidFileName(name)) { Toast.error('Invalid folder name'); return; }

    const url = FileList.currentPath + encodeURIComponent(name) + '/';
    try {
      const resp = await DavClient.mkcol(url);
      if (resp.status === 405) {
        Toast.error('Folder already exists');
        return;
      }
      if (!isDavOk(resp)) {
        Toast.error('Failed to create folder');
        return;
      }
      Toast.success('Created folder "' + name + '"');
      FileList.reload();
      Tree.refresh(FileList.currentPath);
    } catch (e) {
      Toast.error('Failed to create folder');
    }
  },

  async newFile() {
    const name = await Dialog.prompt('New File', '', 'File name');
    if (!name) return;
    if (!isValidFileName(name)) { Toast.error('Invalid file name'); return; }

    const url = FileList.currentPath + encodeURIComponent(name);
    try {
      const resp = await DavClient.send('PUT', url, '');
      if (resp.status === 403) {
        Toast.error('Permission denied');
        return;
      }
      if (!isDavOk(resp)) {
        Toast.error('Failed to create file');
        return;
      }
      Toast.success('Created file "' + name + '"');
      FileList.reload();
    } catch (e) {
      Toast.error('Failed to create file');
    }
  },

  async _createLink(item) {
    const linkName = prompt('Create link — enter name for the symlink:', item.name);
    if (!linkName) return;
    if (!isValidFileName(linkName)) { Toast.error('Invalid link name'); return; }
    const targetPath = item.href;
    const linkPath = FileList.currentPath + encodeURIComponent(linkName);
    try {
      const data = await DavClient.createSymlink(targetPath, linkPath);
      if (data.ok) {
        Toast.success('Link created');
        FileList.reload();
        Tree.refresh(FileList.currentPath);
      } else {
        Toast.error(data.error || 'Failed to create link');
      }
    } catch (e) {
      Toast.error('Failed to create link');
    }
  },

  async _showShareDialog(item) {
    var self = this;
    var wrap = document.createElement('div');
    wrap.className = 'share-dialog';

    // Duration options
    var presets = [
      {label: '1 Hour', seconds: 3600},
      {label: '1 Day', seconds: 86400},
      {label: '1 Week', seconds: 604800},
      {label: '1 Month', seconds: 2592000},
      {label: 'Forever', seconds: 0},
      {label: 'Custom...', seconds: -1}
    ];

    var html = '<div class="share-section">' +
      '<label class="share-label">Duration</label>' +
      '<select id="share-duration" class="share-select">';
    for (var i = 0; i < presets.length; i++) {
      html += '<option value="' + presets[i].seconds + '">' + _escHtml(presets[i].label) + '</option>';
    }
    html += '</select>' +
      '<div id="share-custom-row" class="share-custom-row" hidden>' +
      '<input type="text" id="share-custom-input" class="share-input" placeholder="e.g. 2 hours, 3 days, 1 week">' +
      '<div id="share-custom-preview" class="share-custom-preview"></div>' +
      '</div>' +
      '<button id="share-create-btn" class="btn btn-primary share-create-btn">Create Share Link</button>' +
      '</div>' +
      '<div id="share-existing" class="share-existing"></div>';

    wrap.innerHTML = html;
    Dialog.open('Share — ' + item.name, wrap);

    var durationSel = document.getElementById('share-duration');
    var customRow = document.getElementById('share-custom-row');
    var customInput = document.getElementById('share-custom-input');
    var customPreview = document.getElementById('share-custom-preview');
    var createBtn = document.getElementById('share-create-btn');
    var existingDiv = document.getElementById('share-existing');

    durationSel.addEventListener('change', function() {
      customRow.hidden = durationSel.value !== '-1';
      if (!customRow.hidden) customInput.focus();
      customPreview.textContent = '';
    });

    // Parse custom input and show expiration preview
    var parseTimer = null;
    customInput.addEventListener('input', function() {
      clearTimeout(parseTimer);
      customPreview.textContent = '';
      var text = customInput.value.trim();
      if (!text) return;
      parseTimer = setTimeout(function() {
        fetch(self.davUrl + '_share', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({action: 'parse', text: text})
        }).then(function(r) { return r.json(); }).then(function(data) {
          if (data.ok) {
            if (data.expires) {
              customPreview.textContent = 'Expires: ' + new Date(data.expires).toLocaleString();
              customPreview.className = 'share-custom-preview';
            } else {
              customPreview.textContent = 'Never expires';
              customPreview.className = 'share-custom-preview';
            }
          } else {
            customPreview.textContent = data.error || 'Invalid duration';
            customPreview.className = 'share-custom-preview share-custom-error';
          }
        }).catch(function() {});
      }, 400);
    });

    // Create share link
    createBtn.addEventListener('click', function() {
      var duration;
      if (durationSel.value === '-1') {
        var text = customInput.value.trim();
        if (!text) { customPreview.textContent = 'Enter a duration'; customPreview.className = 'share-custom-preview share-custom-error'; return; }
        // Synchronously fetch parsed value
        createBtn.disabled = true;
        fetch(self.davUrl + '_share', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({action: 'parse', text: text})
        }).then(function(r) { return r.json(); }).then(function(data) {
          createBtn.disabled = false;
          if (!data.ok) { customPreview.textContent = data.error || 'Invalid duration'; customPreview.className = 'share-custom-preview share-custom-error'; return; }
          doCreate(data.seconds, data.expires);
        }).catch(function() { createBtn.disabled = false; });
        return;
      }
      duration = parseInt(durationSel.value);
      doCreate(duration);
    });

    function doCreate(duration, expires) {
      createBtn.disabled = true;
      var createBody = {action: 'create', path: item.href, duration: duration};
      if (expires) createBody.expires = expires;
      fetch(self.davUrl + '_share', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(createBody)
      }).then(function(r) { return r.json(); }).then(function(data) {
        createBtn.disabled = false;
        if (!data.ok) { Toast.show(data.error || 'Failed to create share', 'error'); return; }
        loadExisting();
        FileList.reload();
      }).catch(function() { createBtn.disabled = false; Toast.show('Failed to create share', 'error'); });
    }

    function makeShareUrl(token) {
      var url = location.origin + self.davUrl + '_s/' + token + '/';
      if (!item.collection && Viewers.getType(item) === 'video') url += '?player=1';
      return url;
    }

    function loadExisting() {
      fetch(self.davUrl + '_share', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({action: 'list', path: item.href})
      }).then(function(r) { return r.json(); }).then(function(data) {
        if (!data.ok || !data.shares.length) { existingDiv.innerHTML = ''; return; }
        var h = '<div class="share-label">Active Share Links</div>';
        for (var i = 0; i < data.shares.length; i++) {
          var sh = data.shares[i];
          var url = makeShareUrl(sh.token);
          var expires = sh.share.expires ? 'Expires ' + new Date(sh.share.expires).toLocaleString() : 'Never expires';
          var isExpired = sh.share.expires && new Date(sh.share.expires) < new Date();
          h += '<div class="share-link-row' + (isExpired ? ' share-expired' : '') + '">' +
            '<input type="text" class="share-url-input" value="' + _escHtml(url) + '" readonly>' +
            '<button class="btn btn-sm share-copy-btn" data-url="' + _escHtml(url) + '">Copy</button>' +
            '<button class="btn btn-sm btn-danger share-revoke-btn" data-token="' + _escHtml(sh.token) + '">Revoke</button>' +
            '<div class="share-link-meta">' + (isExpired ? 'Expired' : expires) +
            ' &middot; Created ' + new Date(sh.share.created).toLocaleString() + '</div>' +
            '</div>';
        }
        existingDiv.innerHTML = h;

        // Wire copy buttons
        existingDiv.querySelectorAll('.share-copy-btn').forEach(function(btn) {
          btn.addEventListener('click', function() {
            navigator.clipboard.writeText(btn.dataset.url).then(function() {
              btn.textContent = 'Copied!';
              setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
            });
          });
        });

        // Wire revoke buttons
        existingDiv.querySelectorAll('.share-revoke-btn').forEach(function(btn) {
          btn.addEventListener('click', function() {
            fetch(self.davUrl + '_share', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({action: 'delete', token: btn.dataset.token})
            }).then(function(r) { return r.json(); }).then(function(data) {
              if (data.ok) { loadExisting(); FileList.reload(); }
              else Toast.show(data.error || 'Failed to revoke', 'error');
            });
          });
        });
      });
    }

    loadExisting();
  },

  async _showInfoDialog(item) {
    const canEdit = Auth.admin || item.owner === Auth.username;
    const wrap = document.createElement('div');
    wrap.className = 'settings-panel';

    // Derive parent location
    const loc = decodeURIComponent(item.href.replace(/[^/]*\/?$/, ''));

    // Detect user-path files: href relative to davUrl has a first segment matching owner
    const relPath = item.href.replace(App.davUrl, '');
    const firstSeg = relPath.split('/').filter(Boolean)[0] || '';
    const isUserPath = item.owner && firstSeg === item.owner;

    // Info section
    let html = '<div class="settings-section"><h3>File Information</h3>' +
      '<table class="info-table">' +
      '<tr><td class="info-label">Name</td><td>' + _escHtml(item.name) + '</td></tr>' +
      '<tr><td class="info-label">Type</td><td>' + _escHtml(item.isDir ? 'Folder' : (item.mime || 'application/octet-stream')) + '</td></tr>';
    if (!item.isDir) {
      html += '<tr><td class="info-label">Size</td><td>' + _escHtml(FileList._formatSize(item.size)) + '</td></tr>';
    }
    if (item.modified) {
      html += '<tr><td class="info-label">Modified</td><td>' + _escHtml(item.modified.toLocaleString()) + '</td></tr>';
    }
    html += '<tr><td class="info-label">Location</td><td>' + _escHtml(loc) + '</td></tr>';
    if (item.fsReadable === false) {
      html += '<tr><td class="info-label">Access</td><td style="color:var(--color-danger)">No access (restricted by mounted filesystem)</td></tr>';
    } else if (item.fsWritable === false) {
      html += '<tr><td class="info-label">Access</td><td style="color:var(--color-warning, #b58900)">Read only (restricted by mounted filesystem)</td></tr>';
    }
    html += '<tr id="info-share-row" style="display:none"><td class="info-label">Share URL</td><td id="info-share-cell"></td></tr>';
    if (Auth.admin && !isUserPath) {
      html += '<tr><td class="info-label">Owner</td><td>' +
        '<select id="info-owner"><option value="">Loading...</option></select>' +
        '</td></tr>';
    } else {
      html += '<tr><td class="info-label">Owner</td><td>' + _escHtml(item.owner || 'unknown') + '</td></tr>';
    }
    html += '</table></div>';

    const curGroup = item.group || 'nogroup';

    if (isUserPath) {
      // User-path files: private to owner, no editable permissions
      var isOwnDir = item.owner === Auth.username;
      html += '<div class="settings-section"><h3>Sharing</h3>' +
        '<p style="font-size:13px;color:var(--color-fg-secondary);margin:0">' +
        (isOwnDir
          ? 'Files in your home directory are private. Only you and administrators can access them.'
          : 'Files in a user\u2019s home directory are private. Only the owner and administrators can access them.') +
        '</p></div>';
    } else {
    // Sharing / Group section
    const mode = item.permissions != null ? item.permissions : 644;
    const groupBits = Math.floor(mode / 10) % 10;
    // Map current group bits to access level
    const curAccess = (groupBits & 2) ? 'rw' : (groupBits & 4) ? 'r' : 'none';
    const otherBitsVal = mode % 10;
    const curOtherAccess = (otherBitsVal & 2) ? 'rw' : (otherBitsVal & 4) ? 'r' : 'none';
    const accessLabels = { none: 'Not accessible', r: 'Read only', rw: 'Read and modify' };

    html += '<div class="settings-section"><h3>Sharing</h3>';
    if (canEdit) {
      html +=
        '<div class="settings-field">' +
          '<label>Group</label>' +
          '<select id="info-group"><option value="">Loading...</option></select>' +
          '<button class="btn btn-sm" id="info-members-btn" style="margin-left:8px">Membership Details</button>' +
        '</div>' +
        '<p style="font-size:13px;color:var(--color-fg-secondary);margin:8px 0 4px">Members of the above group can:</p>' +
        '<div class="info-access-radios">' +
          '<label class="info-radio-label"><input type="radio" name="info-access" value="none"' + (curAccess === 'none' ? ' checked' : '') + '> Not access</label>' +
          '<label class="info-radio-label"><input type="radio" name="info-access" value="r"' + (curAccess === 'r' ? ' checked' : '') + '> Read (including copy)</label>' +
          '<label class="info-radio-label"><input type="radio" name="info-access" value="rw"' + (curAccess === 'rw' ? ' checked' : '') + '> Read and modify</label>' +
        '</div>' +
        '<p style="font-size:13px;color:var(--color-fg-secondary);margin:8px 0 4px">Others can:</p>' +
        '<div class="info-access-radios">' +
          '<label class="info-radio-label"><input type="radio" name="info-other" value="none"' + (curOtherAccess === 'none' ? ' checked' : '') + '> Not access</label>' +
          '<label class="info-radio-label"><input type="radio" name="info-other" value="r"' + (curOtherAccess === 'r' ? ' checked' : '') + '> Read (including copy)</label>' +
          '<label class="info-radio-label"><input type="radio" name="info-other" value="rw"' + (curOtherAccess === 'rw' ? ' checked' : '') + '> Read and modify</label>' +
        '</div>' +
        '<button class="btn btn-primary btn-sm" id="info-save" style="margin-top:12px">Save</button>' +
        '<span class="settings-msg" id="info-msg"></span>';
    } else {
      html +=
        '<table class="info-table">' +
        '<tr><td class="info-label">Group</td><td>' + _escHtml(curGroup) + '</td></tr>' +
        '<tr><td class="info-label">Group access</td><td>' + _escHtml(accessLabels[curAccess]) + '</td></tr>' +
        '<tr><td class="info-label">Other access</td><td>' + _escHtml(accessLabels[curOtherAccess]) + '</td></tr>' +
        '</table>';
    }
    html += '</div>';
    } // end !isUserPath

    wrap.innerHTML = html;
    Dialog.open('Info — ' + item.name, wrap);

    // Load share links for this item
    var davUrl = this.davUrl;
    fetch(this.davUrl + '_share', {
      method: 'POST', credentials: 'same-origin',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({action: 'list', path: item.href})
    }).then(function(r) { return r.json(); }).then(function(data) {
      if (!data.ok || !data.shares || !data.shares.length) return;
      var row = document.getElementById('info-share-row');
      var cell = document.getElementById('info-share-cell');
      if (!row || !cell) return;
      row.style.display = '';
      var urls = data.shares.map(function(sh) {
        var url = location.origin + davUrl + '_s/' + sh.token + '/';
        if (!item.isDir && Viewers.getType(item) === 'video') url += '?player=1';
        var expired = sh.share.expires && new Date(sh.share.expires) < new Date();
        return '<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">' +
          '<input type="text" class="share-url-input" value="' + _escHtml(url) + '" readonly style="flex:1;font-size:12px">' +
          '<button class="btn btn-sm info-share-copy" data-url="' + _escHtml(url) + '">Copy</button>' +
          (expired ? '<span style="color:var(--color-danger);font-size:11px">Expired</span>' : '') +
          '</div>';
      });
      cell.innerHTML = urls.join('');
      cell.querySelectorAll('.info-share-copy').forEach(function(btn) {
        btn.addEventListener('click', function() {
          navigator.clipboard.writeText(btn.dataset.url).then(function() {
            btn.textContent = 'Copied!';
            setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
          });
        });
      });
    }).catch(function() {});

    if (isUserPath || !canEdit) return;

    // Populate group dropdown and collect membership data
    const groupSelect = document.getElementById('info-group');
    let groupOptions = ['nogroup', 'everyone'];
    const groupMembers = {};  // { groupName: [user1, user2, ...] }
    groupMembers['nogroup'] = [];
    groupMembers['everyone'] = [];  // conceptually all users
    if (Auth.admin) {
      try {
        const [grpResp, usrResp] = await Promise.all([
          fetch(this.davUrl + '_admin/groups', { credentials: 'same-origin' }),
          fetch(this.davUrl + '_admin/users', { credentials: 'same-origin' })
        ]);
        const grpData = await grpResp.json();
        const usrData = await usrResp.json();
        if (grpData.ok && grpData.groups) {
          grpData.groups.forEach(function(g) {
            if (groupOptions.indexOf(g.name) === -1) groupOptions.push(g.name);
            groupMembers[g.name] = g.members || [];
          });
        }
        if (usrData.ok && usrData.users) {
          groupMembers['everyone'] = usrData.users.map(function(u) { return u.username; });
          // Populate owner dropdown (admin only, non-user paths)
          var ownerSelect = document.getElementById('info-owner');
          if (ownerSelect) {
            var curOwner = item.owner || '';
            ownerSelect.innerHTML = usrData.users.map(function(u) {
              return '<option value="' + _escAttr(u.username) + '"' +
                (u.username === curOwner ? ' selected' : '') + '>' +
                _escHtml(u.username) + '</option>';
            }).join('');
          }
        }
      } catch (e) {}
    } else {
      Auth.groups.forEach(function(g) {
        if (groupOptions.indexOf(g) === -1) groupOptions.push(g);
      });
    }
    groupSelect.innerHTML = groupOptions.map(function(g) {
      return '<option value="' + _escAttr(g) + '"' + (g === curGroup ? ' selected' : '') + '>' + _escHtml(g) + '</option>';
    }).join('');
    if (groupOptions.indexOf(curGroup) === -1) {
      groupSelect.insertAdjacentHTML('afterbegin',
        '<option value="' + _escAttr(curGroup) + '" selected>' + _escHtml(curGroup) + '</option>');
    }

    // Membership details floating modal
    const membersBtn = document.getElementById('info-members-btn');
    membersBtn.addEventListener('click', function() {
      const selGroup = groupSelect.value;
      let body;
      if (selGroup === 'nogroup') {
        body = '<em>nogroup has no members</em>';
      } else if (selGroup === 'everyone') {
        const list = groupMembers['everyone'];
        body = list.length > 0
          ? list.map(function(u) { return _escHtml(u); }).join(', ')
          : '<em>All authenticated users</em>';
      } else if (groupMembers[selGroup]) {
        const list = groupMembers[selGroup];
        body = list.length > 0 ? list.map(function(u) { return _escHtml(u); }).join(', ') : '<em>No members</em>';
      } else {
        body = '<em>Membership info not available</em>';
      }

      // Create modal overlay
      const overlay = document.createElement('div');
      overlay.className = 'members-overlay';
      overlay.innerHTML =
        '<div class="members-modal">' +
          '<div class="members-modal-header">' +
            '<span class="members-modal-title">' + _escHtml(selGroup) + ' — Members</span>' +
            '<button class="members-modal-close" title="Close">&times;</button>' +
          '</div>' +
          '<div class="members-modal-body">' + body + '</div>' +
        '</div>';
      document.body.appendChild(overlay);

      function dismiss() { overlay.remove(); }
      overlay.querySelector('.members-modal-close').addEventListener('click', dismiss);
      overlay.addEventListener('click', function(e) {
        if (e.target === overlay) dismiss();
      });
    });

    // Constrain "others" radios: other access cannot exceed group access
    const accessOrder = ['none', 'r', 'rw'];
    function updateOtherRadios() {
      const grpEl = document.querySelector('input[name="info-access"]:checked');
      const grpLevel = grpEl ? accessOrder.indexOf(grpEl.value) : 0;
      document.querySelectorAll('input[name="info-other"]').forEach(function(radio) {
        const lvl = accessOrder.indexOf(radio.value);
        const label = radio.closest('.info-radio-label');
        radio.disabled = lvl > grpLevel;
        if (label) label.style.opacity = lvl > grpLevel ? '0.4' : '1';
        if (radio.checked && radio.disabled) {
          document.querySelectorAll('input[name="info-other"]')[grpLevel].checked = true;
        }
      });
    }
    document.querySelectorAll('input[name="info-access"]').forEach(function(radio) {
      radio.addEventListener('change', updateOtherRadios);
    });
    updateOtherRadios();

    // Save handler — build permissions from access level radios
    const self = this;
    document.getElementById('info-save').addEventListener('click', async function() {
      const msg = document.getElementById('info-msg');
      const newGroup = groupSelect.value;
      const groupAccessEl = document.querySelector('input[name="info-access"]:checked');
      const otherAccessEl = document.querySelector('input[name="info-other"]:checked');
      const groupAccess = groupAccessEl ? groupAccessEl.value : 'none';
      const otherAccess = otherAccessEl ? otherAccessEl.value : 'none';

      const ownerBits = item.isDir ? 7 : 6;
      const bitsMap = { none: 0, r: item.isDir ? 5 : 4, rw: item.isDir ? 7 : 6 };
      const newPerms = ownerBits * 100 + bitsMap[groupAccess] * 10 + bitsMap[otherAccess];

      var postBody = { path: item.href, permissions: newPerms, group: newGroup };
      var ownerSel = document.getElementById('info-owner');
      if (ownerSel) postBody.owner = ownerSel.value;
      try {
        const resp = await fetch(self.davUrl + '_filemeta', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(postBody)
        });
        const data = await resp.json();
        if (data.ok) {
          msg.textContent = 'Saved';
          msg.className = 'settings-msg success';
          item.permissions = data.permissions;
          item.group = data.group;
          if (data.owner) item.owner = data.owner;
          FileList.render();
        } else {
          msg.textContent = data.error || 'Failed';
          msg.className = 'settings-msg error';
        }
      } catch (e) {
        msg.textContent = 'Connection error';
        msg.className = 'settings-msg error';
      }
    });
  },

  async _createArchive(items, format) {
    if (!items || !items.length) return;

    // Default filename: first item name + extension
    var ext = format === 'zip' ? '.zip' : '.tar.gz';
    var firstName = items[0].name.replace(/\/$/, '');
    // Strip common extensions (including multi-part like .tar.gz)
    var baseName = firstName.replace(/\.(tar\.(gz|bz2|xz|zst)|zip|rar|7z|gz|bz2|xz|[^.]+)$/, '');
    if (!baseName) baseName = firstName;  // fallback if name was entirely extension-like
    if (items.length > 1) baseName = baseName + '-etc';
    var defaultName = baseName + ext;

    // Show file picker for save location
    var result = await FilePicker.save('Save Archive As', FileList.currentPath, defaultName);
    if (!result) return;

    // Ensure correct extension
    var filename = result.filename;
    if (format === 'zip' && !filename.endsWith('.zip')) filename += '.zip';
    if (format === 'tar.gz' && !filename.endsWith('.tar.gz')) filename += '.tar.gz';
    var destPath = result.dir + encodeURIComponent(filename);

    // Collect source DAV paths
    var paths = items.map(function(it) { return it.href; });

    // Show progress spinner while archive is created
    var progressWrap = document.createElement('div');
    progressWrap.style.cssText = 'text-align:center;padding:16px 0';
    var statusText = document.createElement('div');
    statusText.style.cssText = 'margin-bottom:12px;font-size:14px';
    statusText.textContent = 'Creating ' + (format === 'zip' ? 'zip' : 'tar.gz') + ' archive\u2026';
    progressWrap.appendChild(statusText);

    var spinner = document.createElement('div');
    spinner.className = 'spinner';
    spinner.style.cssText = 'width:32px;height:32px;margin:0 auto 16px';
    progressWrap.appendChild(spinner);

    Dialog.open('Creating Archive', progressWrap);
    Dialog._beforeClose = function() { return false; }; // prevent Escape closing

    // The server runs the archive synchronously and returns when done
    var resp;
    try {
      resp = await fetch(App.davUrl + '_archive', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'start', paths: paths, format: format, dest: destPath })
      });
    } catch(e) {
      Dialog._beforeClose = null;
      Dialog.close();
      Dialog.alert('Failed to create archive: ' + e.message);
      return;
    }

    Dialog._beforeClose = null;
    Dialog.close();

    var data = await resp.json();
    if (!data.ok) {
      Dialog.alert('Archive failed: ' + (data.error || 'Unknown error'));
      return;
    }

    Toast.success('Archive created: ' + filename + ' (' + FileList._formatSize(data.size) + ')');
    FileList.refresh();
  },

  async openTerminal() {
    var hosts = Auth.sshHosts || [];
    var defaultVal = hosts.length ? hosts[0] : '';

    // Build prompt content: input + optional dropdown
    var wrap = document.createElement('div');
    var input = document.createElement('input');
    input.type = 'text';
    input.className = 'dialog-input';
    input.value = defaultVal;
    input.placeholder = 'user@hostname';
    wrap.appendChild(input);

    if (hosts.length > 0) {
      var select = document.createElement('select');
      select.className = 'dialog-input';
      select.style.marginTop = '8px';
      var emptyOpt = document.createElement('option');
      emptyOpt.value = '';
      emptyOpt.textContent = 'Recent hosts\u2026';
      emptyOpt.disabled = true;
      select.appendChild(emptyOpt);
      hosts.forEach(function(h) {
        var opt = document.createElement('option');
        opt.value = h;
        opt.textContent = h;
        select.appendChild(opt);
      });
      if (defaultVal) select.value = defaultVal;
      select.addEventListener('change', function() {
        input.value = select.value;
        input.focus();
      });
      wrap.appendChild(select);
    }

    var host = await new Promise(function(resolve) {
      var cancelBtn = document.createElement('button');
      cancelBtn.className = 'btn';
      cancelBtn.textContent = 'Cancel';
      var okBtn = document.createElement('button');
      okBtn.className = 'btn btn-primary';
      okBtn.textContent = 'Connect';

      input.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') { Dialog._onClose = null; Dialog.close(); resolve(input.value.trim()); }
      });
      var newTabBtn = document.createElement('button');
      newTabBtn.className = 'btn btn-primary';
      newTabBtn.textContent = 'Connect and Open in New Tab';
      cancelBtn.addEventListener('click', function() { Dialog._onClose = null; Dialog.close(); resolve(null); });
      okBtn.addEventListener('click', function() { Dialog._onClose = null; Dialog.close(); resolve(input.value.trim()); });
      newTabBtn.addEventListener('click', function() { Dialog._onClose = null; Dialog.close(); resolve('newtab:' + input.value.trim()); });

      Dialog.open('SSH Host', wrap, {
        footer: [cancelBtn, newTabBtn, okBtn],
        onClose: function() { resolve(null); }
      });
      input.focus();
      if (defaultVal) input.select();
    });

    if (!host) return;
    if (host.indexOf('newtab:') === 0) {
      var termHost = host.substring(7);
      if (!termHost || termHost.indexOf('@') === -1) {
        Toast.error('Please include a username (e.g. user@hostname)');
        return this.openTerminal();
      }
      this._saveSSHHost(termHost);
      window.open(window.location.pathname + '?term=' + encodeURIComponent(termHost), '_blank');
      return;
    }
    if (host.indexOf('@') === -1) {
      Toast.error('Please include a username (e.g. user@hostname)');
      return this.openTerminal();
    }
    this._saveSSHHost(host);
    this._openTerminalSession(host);
  },

  _saveSSHHost(host) {
    var hosts = Auth.sshHosts || [];
    // Move to front, deduplicate, cap at 10
    hosts = hosts.filter(function(h) { return h !== host; });
    hosts.unshift(host);
    hosts = hosts.slice(0, 10);
    Auth.sshHosts = hosts;
    // Persist to server
    fetch(this.davUrl + '_settings/sshHosts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ hosts: hosts }),
      credentials: 'same-origin'
    });
  },

  _getEffectiveTheme() {
    var t = document.documentElement.getAttribute('data-theme') || 'auto';
    if (t === 'auto') {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    if (t === 'dark' || t === 'light') return t;
    // Named theme — check background luminance
    var bg = getComputedStyle(document.documentElement).getPropertyValue('--color-bg').trim();
    var m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(bg);
    if (m) {
      var lum = (parseInt(m[1],16)*299 + parseInt(m[2],16)*587 + parseInt(m[3],16)*114) / 1000;
      return lum < 128 ? 'dark' : 'light';
    }
    return 'light';
  },

  _termThemes: {
    dark: {
      background: '#0d1117',
      foreground: '#e6edf3',
      cursor: '#58a6ff',
      cursorAccent: '#0d1117',
      selectionBackground: 'rgba(88,166,255,0.3)',
      black: '#484f58',
      red: '#ff7b72',
      green: '#3fb950',
      yellow: '#d29922',
      blue: '#58a6ff',
      magenta: '#bc8cff',
      cyan: '#39d353',
      white: '#e6edf3',
      brightBlack: '#6e7681',
      brightRed: '#ffa198',
      brightGreen: '#56d364',
      brightYellow: '#e3b341',
      brightBlue: '#79c0ff',
      brightMagenta: '#d2a8ff',
      brightCyan: '#56d364',
      brightWhite: '#f0f6fc'
    },
    light: {
      background: '#ffffff',
      foreground: '#1f2328',
      cursor: '#0969da',
      cursorAccent: '#ffffff',
      selectionBackground: 'rgba(9,105,218,0.2)',
      black: '#24292f',
      red: '#cf222e',
      green: '#1a7f37',
      yellow: '#9a6700',
      blue: '#0969da',
      magenta: '#8250df',
      cyan: '#1b7c83',
      white: '#6e7781',
      brightBlack: '#57606a',
      brightRed: '#a40e26',
      brightGreen: '#2da44e',
      brightYellow: '#bf8700',
      brightBlue: '#218bff',
      brightMagenta: '#a475f9',
      brightCyan: '#3192aa',
      brightWhite: '#8c959f'
    }
  },

  _getTermTheme() {
    var pref = Auth.termTheme || 'auto';
    if (pref === 'auto') {
      return this._termThemes[this._getEffectiveTheme()] || this._termThemes.dark;
    }
    if (pref === 'dark' || pref === 'light') {
      return this._termThemes[pref];
    }
    // Custom JSON theme
    try { return JSON.parse(pref); } catch(e) {
      return this._termThemes[this._getEffectiveTheme()] || this._termThemes.dark;
    }
  },

  _openTerminalSession(host) {
    var wrap = document.createElement('div');
    wrap.className = 'term-container';

    var winId = WinManager.open('Terminal — ' + host, wrap, {
      type: 'terminal', wide: true, noPadding: true,
      beforeClose: function() {
        return Dialog.confirm('Close this terminal session? Any running processes will be terminated.', 'Close', true);
      }
    });

    // Load xterm CSS + JS, then connect
    var self = this;
    this._loadTermLibs().then(function() {
      var term = new window.Terminal({
        fontFamily: 'monospace, monospace',
        cursorBlink: true,
        lineHeight: 1,
        theme: self._getTermTheme()
      });

      var fitAddon = new FitAddon.FitAddon();
      term.loadAddon(fitAddon);
      term.open(wrap);

      // Workaround for scrollbar sizing
      var screen = wrap.querySelector('.xterm-screen');
      var viewport = wrap.querySelector('.xterm-viewport');
      var scrollArea = wrap.querySelector('.xterm-scroll-area');

      function onSize() {
        if (!wrap.parentNode) return;
        screen.style.height = wrap.clientHeight + 'px';
        viewport.style.height = wrap.clientHeight + 'px';
        scrollArea.style.height = screen.style.height;
        screen.style.width = wrap.clientWidth + 'px';
        viewport.style.width = wrap.clientWidth + 'px';
        scrollArea.style.width = screen.style.width;
        fitAddon.fit();
        viewport.style.height = (screen.clientHeight + 10) + 'px';
        viewport.style.width = (screen.clientWidth - 2) + 'px';
        sendcomm('resize', { cols: term.cols, rows: term.rows });
      }

      // Websocket
      var ws = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      var wsurl = ws + window.location.host + '/wsapps/terminal/terminal.js?host=' + encodeURIComponent(host);
      var socket;

      function openSocket() {
        try {
          var s = new WebSocket(wsurl);
          return { socket: s };
        } catch(e) {
          return { error: e };
        }
      }

      function sendcomm(cmd, opt) {
        if (!socket || socket.readyState !== WebSocket.OPEN) return;
        var o = {};
        if (!opt) opt = true;
        o[cmd] = opt;
        var buff = new TextEncoder().encode(JSON.stringify(o));
        setTimeout(function() { socket.send(buff.buffer); }, 50);
      }

      function checksocket() {
        if (!socket || socket.readyState !== WebSocket.OPEN) {
          var res = openSocket();
          if (res.error) {
            term.write('Could not connect: ' + res.error);
            return false;
          }
          socket = res.socket;
          socket.onopen = wsOnOpen;
          return false;
        }
        return true;
      }

      var termCallbackSet = false;
      var lostMessage = false;
      function wsOnOpen() {
        socket.onmessage = function(event) {
          event.data.text().then(function(d) { term.write(d); });
        };
        if (!termCallbackSet) {
          var resizeObs = new ResizeObserver(onSize);
          resizeObs.observe(wrap);
          onSize();
          term.onData(function(e) {
            if (checksocket()) {
              if (lostMessage) {
                socket.send(lostMessage);
                lostMessage = false;
              }
              socket.send(e);
            } else {
              term.write('-----connection broken-----\n');
              lostMessage = e;
            }
          });
          termCallbackSet = true;

          // Clean up on window close
          var winState = WinManager.getWindow(winId);
          if (winState) winState.onClose = function() {
            resizeObs.disconnect();
            if (socket && socket.readyState === WebSocket.OPEN) socket.close();
            term.dispose();
          };
        }
      }

      var res = openSocket();
      if (res.error) {
        term.write('Could not connect: ' + res.error);
      } else {
        socket = res.socket;
        socket.onopen = wsOnOpen;
      }

      term.focus();
    });
  },

  _loadTermLibs() {
    if (Viewers._loaded.xterm) return Promise.resolve();
    return new Promise(function(resolve, reject) {
      // Load CSS
      var link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = CDN.xtermCss;
      document.head.appendChild(link);
      // Load xterm.js
      var s1 = document.createElement('script');
      s1.src = CDN.xtermJs;
      s1.onload = function() {
        // Load fit addon
        var s2 = document.createElement('script');
        s2.src = CDN.xtermFit;
        s2.onload = function() {
          Viewers._loaded.xterm = true;
          resolve();
        };
        s2.onerror = reject;
        document.head.appendChild(s2);
      };
      s1.onerror = reject;
      document.head.appendChild(s1);
    });
  },

  _applyGridSettings() {
    var saved = localStorage.getItem('fm_grid_settings');
    var root = document.documentElement.style;
    if (saved) {
      try {
        var s = JSON.parse(saved);
        var pad = Math.round(s.gap / 2);
        root.setProperty('--grid-thumb-size', s.thumbSize + 'px');
        root.setProperty('--grid-icon-size', Math.round(s.thumbSize * 0.6) + 'px');
        root.setProperty('--grid-gap', s.gap + 'px');
        root.setProperty('--grid-item-size', (s.thumbSize + pad * 2) + 'px');
        root.setProperty('--grid-item-pad', pad + 'px');
      } catch(e) {}
    } else {
      root.removeProperty('--grid-thumb-size');
      root.removeProperty('--grid-icon-size');
      root.removeProperty('--grid-gap');
      root.removeProperty('--grid-item-size');
      root.removeProperty('--grid-item-pad');
    }
  },

  _openGridAdjust() {
    var self = this;
    var defaults = { thumbSize: 80, gap: 24 };
    var saved = localStorage.getItem('fm_grid_settings');
    var current;
    try { current = saved ? JSON.parse(saved) : Object.assign({}, defaults); }
    catch(e) { current = Object.assign({}, defaults); }

    // Hide the settings dialog
    var overlay = document.getElementById('modal-overlay');
    overlay.style.visibility = 'hidden';

    // Switch to grid view temporarily so user can see changes
    var wasDetail = FileList.viewMode === 'detail';
    if (wasDetail) {
      FileList.viewMode = 'grid';
      FileList.render();
    }

    // Build the popup
    var popup = document.createElement('div');
    popup.style.cssText = 'position:fixed;bottom:48px;right:16px;z-index:200001;background:var(--color-modal-bg,var(--color-bg));' +
      'border:1px solid var(--color-modal-border,var(--color-border));border-radius:var(--border-radius);' +
      'box-shadow:var(--shadow-lg);padding:16px;width:260px;font-size:var(--font-size-sm)';

    var title = document.createElement('div');
    title.style.cssText = 'font-weight:600;margin-bottom:12px';
    title.textContent = 'Grid View';
    popup.appendChild(title);

    // Thumbnail size slider
    var sizeLabel = document.createElement('label');
    sizeLabel.style.cssText = 'display:block;margin-bottom:4px';
    sizeLabel.textContent = 'Thumbnail size: ' + current.thumbSize + 'px';
    popup.appendChild(sizeLabel);

    var sizeSlider = document.createElement('input');
    sizeSlider.type = 'range';
    sizeSlider.min = '48';
    sizeSlider.max = '256';
    sizeSlider.value = current.thumbSize;
    sizeSlider.style.cssText = 'width:100%;margin-bottom:12px';
    popup.appendChild(sizeSlider);

    // Spacing slider
    var gapLabel = document.createElement('label');
    gapLabel.style.cssText = 'display:block;margin-bottom:4px';
    gapLabel.textContent = 'Spacing: ' + current.gap + 'px';
    popup.appendChild(gapLabel);

    var gapSlider = document.createElement('input');
    gapSlider.type = 'range';
    gapSlider.min = '0';
    gapSlider.max = '48';
    gapSlider.value = current.gap;
    gapSlider.style.cssText = 'width:100%;margin-bottom:16px';
    popup.appendChild(gapSlider);

    // Buttons
    var btnRow = document.createElement('div');
    btnRow.style.cssText = 'display:flex;gap:8px;justify-content:flex-end';

    var defaultBtn = document.createElement('button');
    defaultBtn.className = 'btn btn-sm';
    defaultBtn.textContent = 'Defaults';
    defaultBtn.style.marginRight = 'auto';

    var cancelBtn = document.createElement('button');
    cancelBtn.className = 'btn btn-sm';
    cancelBtn.textContent = 'Cancel';

    var okBtn = document.createElement('button');
    okBtn.className = 'btn btn-primary btn-sm';
    okBtn.textContent = 'OK';

    btnRow.appendChild(defaultBtn);
    btnRow.appendChild(cancelBtn);
    btnRow.appendChild(okBtn);
    popup.appendChild(btnRow);

    document.body.appendChild(popup);

    // Apply preview live
    function applyPreview(thumbSize, gap) {
      var root = document.documentElement.style;
      var pad = Math.round(gap / 2);
      root.setProperty('--grid-thumb-size', thumbSize + 'px');
      root.setProperty('--grid-icon-size', Math.round(thumbSize * 0.6) + 'px');
      root.setProperty('--grid-gap', gap + 'px');
      root.setProperty('--grid-item-size', (thumbSize + pad * 2) + 'px');
      root.setProperty('--grid-item-pad', pad + 'px');
    }

    applyPreview(current.thumbSize, current.gap);

    sizeSlider.addEventListener('input', function() {
      sizeLabel.textContent = 'Thumbnail size: ' + this.value + 'px';
      applyPreview(parseInt(this.value), parseInt(gapSlider.value));
    });

    gapSlider.addEventListener('input', function() {
      gapLabel.textContent = 'Spacing: ' + this.value + 'px';
      applyPreview(parseInt(sizeSlider.value), parseInt(this.value));
    });

    function close(save) {
      if (save) {
        var thumbSize = parseInt(sizeSlider.value);
        var gap = parseInt(gapSlider.value);
        if (thumbSize === defaults.thumbSize && gap === defaults.gap) {
          localStorage.removeItem('fm_grid_settings');
        } else {
          localStorage.setItem('fm_grid_settings', JSON.stringify({ thumbSize: thumbSize, gap: gap }));
        }
        self._applyGridSettings();
      } else {
        // Revert to saved
        self._applyGridSettings();
      }
      if (wasDetail) {
        FileList.viewMode = 'detail';
        FileList.render();
      }
      popup.remove();
      overlay.style.visibility = '';
    }

    defaultBtn.addEventListener('click', function() {
      sizeSlider.value = defaults.thumbSize;
      gapSlider.value = defaults.gap;
      sizeLabel.textContent = 'Thumbnail size: ' + defaults.thumbSize + 'px';
      gapLabel.textContent = 'Spacing: ' + defaults.gap + 'px';
      applyPreview(defaults.thumbSize, defaults.gap);
    });

    cancelBtn.addEventListener('click', function() { close(false); });
    okBtn.addEventListener('click', function() { close(true); });
  },

  async openSettings() {
    // Fetch current settings
    let settings;
    try {
      const resp = await fetch(this.davUrl + '_settings', { credentials: 'same-origin' });
      settings = await resp.json();
    } catch (e) {
      Toast.error('Failed to load settings');
      return;
    }

    const wrap = document.createElement('div');
    wrap.className = 'settings-panel';

    const timeoutSecs = typeof settings.sessionTimeout === 'number' ? settings.sessionTimeout : 7200;
    const timeoutOptions = [
      { label: '15 minutes', value: 900 },
      { label: '30 minutes', value: 1800 },
      { label: '1 hour', value: 3600 },
      { label: '2 hours', value: 7200 },
      { label: '8 hours', value: 28800 },
      { label: '24 hours', value: 86400 },
      { label: '7 days', value: 604800 },
      { label: 'Never', value: 0 }
    ];

    var currentTheme = Auth.theme || 'auto';
    var cmTheme = Auth.cmTheme || 'auto';
    var cmThemeOptions = '<option value="auto"' + (cmTheme === 'auto' ? ' selected' : '') + '>Auto (match app theme)</option>';
    var cmThemes = Viewers.cmThemes;
    for (var k in cmThemes) {
      if (cmThemes.hasOwnProperty(k)) {
        cmThemeOptions += '<option value="' + k + '"' + (cmTheme === k ? ' selected' : '') + '>' + cmThemes[k].label + '</option>';
      }
    }
    var cmThemeSection =
      '<div class="settings-section">' +
        '<h3>Code Editor Theme</h3>' +
        '<div class="settings-field">' +
          '<label>CodeMirror theme</label>' +
          '<select id="set-cm-theme">' + cmThemeOptions + '</select>' +
        '</div>' +
      '</div>';
    var termTheme = Auth.termTheme || 'auto';
    var isCustomTermTheme = termTheme !== 'auto' && termTheme !== 'dark' && termTheme !== 'light';
    var termThemeSection = '';
    if (Auth.terminal) {
      termThemeSection =
        '<div class="settings-section">' +
          '<h3>Terminal Theme</h3>' +
          '<div class="settings-field">' +
            '<label>Color scheme</label>' +
            '<select id="set-term-theme">' +
              '<option value="auto"' + (termTheme === 'auto' ? ' selected' : '') + '>Match app theme</option>' +
              '<option value="dark"' + (termTheme === 'dark' ? ' selected' : '') + '>Dark</option>' +
              '<option value="light"' + (termTheme === 'light' ? ' selected' : '') + '>Light</option>' +
              '<option value="custom"' + (isCustomTermTheme ? ' selected' : '') + '>Custom</option>' +
            '</select>' +
          '</div>' +
          '<div class="settings-field" id="set-term-custom-wrap"' + (isCustomTermTheme ? '' : ' hidden') + '>' +
            '<label>Custom theme JSON</label>' +
            '<textarea id="set-term-custom" rows="8" spellcheck="false" ' +
              'style="font-family:var(--font-mono);font-size:var(--font-size-sm);width:100%;resize:vertical"' +
              '>' + (isCustomTermTheme ? termTheme.replace(/</g, '&lt;') : '') + '</textarea>' +
            '<p style="font-size:12px;color:var(--color-fg-secondary);margin:4px 0 0">xterm.js theme object — keys: background, foreground, cursor, black, red, green, yellow, blue, magenta, cyan, white, bright*</p>' +
          '</div>' +
          '<button class="btn btn-sm" id="set-save-term-theme">Save</button>' +
          '<span class="settings-msg" id="set-term-theme-msg"></span>' +
        '</div>';
    }
    wrap.innerHTML =
      '<div class="settings-section">' +
        '<h3>Appearance</h3>' +
        '<div class="settings-field">' +
          '<label>Theme</label>' +
          '<select id="set-theme">' +
            '<option value="auto">Auto (system)</option>' +
            '<option value="light">Light</option>' +
            '<option value="dark">Dark</option>' +
            (settings.themes || []).map(function(t) {
              return '<option value="' + t.value + '">' + t.label + '</option>';
            }).join('') +
          '</select>' +
        '</div>' +
        '<div class="settings-field" style="margin-top:8px">' +
          '<button class="btn btn-sm" id="set-grid-adjust">Adjust grid view</button>' +
        '</div>' +
      '</div>' +
      termThemeSection +
      cmThemeSection +
      '<div class="settings-section">' +
        '<h3>Document Editor</h3>' +
        '<div class="settings-field">' +
          '<label><input type="checkbox" id="set-oo-autosave"' + (Auth.ooAutosave ? ' checked' : '') + '> Autosave documents in ONLYOFFICE</label>' +
        '</div>' +
      '</div>' +
      (Auth.demoMode ? '' :
      '<div class="settings-section">' +
        '<h3>Change Password</h3>' +
        '<div class="settings-field"><label>Current password</label><input type="password" id="set-cur-pass"></div>' +
        '<div class="settings-field"><label>New password</label><input type="password" id="set-new-pass"></div>' +
        '<div class="settings-field"><label>Confirm password</label><input type="password" id="set-confirm-pass"></div>' +
        '<button class="btn btn-primary btn-sm" id="set-change-pass">Change Password</button>' +
        '<span class="settings-msg" id="set-pass-msg"></span>' +
      '</div>' +
      '<div class="settings-section">' +
        '<h3>Session Timeout</h3>' +
        '<div class="settings-field">' +
          '<label>Auto-logout after</label>' +
          '<select id="set-timeout">' +
            timeoutOptions.map(function(o) {
              return '<option value="' + o.value + '"' +
                (o.value === timeoutSecs ? ' selected' : '') + '>' + o.label + '</option>';
            }).join('') +
          '</select>' +
        '</div>' +
        '<button class="btn btn-sm" id="set-save-timeout">Save</button>' +
        '<span class="settings-msg" id="set-timeout-msg"></span>' +
      '</div>' +
      '<div class="settings-section">' +
        '<h3>Security</h3>' +
        '<p style="font-size:13px;color:var(--color-fg-secondary);margin:0 0 8px">Log out from all devices and browsers. You will need to log in again.</p>' +
        '<button class="btn btn-danger btn-sm" id="set-revoke">Log Out All Devices</button>' +
      '</div>');

    // Cloud Storage section
    var cloudHtml = '<div class="settings-section"><h3>Cloud Storage</h3>';
    if (Auth.demoMode) {
      cloudHtml +=
        '<div class="cloud-unavailable">' +
          '<p>Cloud storage is disabled in demo mode.</p>' +
        '</div>';
    } else if (!settings.rcloneAvailable) {
      cloudHtml +=
        '<div class="cloud-unavailable">' +
          '<p>Cloud storage mounting is not available.</p>' +
          '<p>The server administrator needs to install ' +
            '<a href="https://rclone.org/install/" target="_blank" rel="noopener">rclone</a> ' +
            'on this server to enable cloud storage integration.</p>' +
        '</div>';
    } else {
      cloudHtml +=
        '<div id="cloud-mount-list">' +
          '<p style="font-size:13px;color:var(--color-fg-secondary)">Loading mounts...</p>' +
        '</div>' +
        '<button class="btn btn-primary btn-sm" id="cloud-add-mount" style="margin-top:8px">' +
          'Add Cloud Storage</button>';
    }
    cloudHtml += '</div>';
    wrap.innerHTML += cloudHtml;

    // Admin section (only for admins)
    if (settings.admin) {
      wrap.innerHTML +=
        '<div class="settings-section">' +
          '<h3>User Administration</h3>' +
          '<div class="admin-user-list" id="admin-user-list">' +
            '<p style="font-size:13px;color:var(--color-fg-secondary)">Loading users...</p>' +
          '</div>' +
          '<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--color-border-light)">' +
            '<h4 style="margin:0 0 8px;font-size:13px;font-weight:600">Add New User</h4>' +
            '<div class="settings-field"><label>Username</label><input type="text" id="admin-new-user" autocomplete="off"></div>' +
            '<div class="settings-field"><label>Password</label><input type="password" id="admin-new-pass" autocomplete="new-password"></div>' +
            '<div class="settings-field"><label style="display:flex;align-items:center;gap:6px;cursor:pointer"><input type="checkbox" id="admin-new-force-pw" checked> Require password change on login</label></div>' +
            '<button class="btn btn-primary btn-sm" id="admin-add-user">Add User</button>' +
            '<span class="settings-msg" id="admin-add-msg"></span>' +
          '</div>' +
        '</div>' +
        '<div class="settings-section">' +
          '<h3>Group Management</h3>' +
          '<p style="font-size:13px;color:var(--color-fg-secondary);margin:0 0 8px">' +
            'Create groups and assign users to control file access via group permissions.</p>' +
          '<div id="admin-group-list">' +
            '<p style="font-size:13px;color:var(--color-fg-secondary)">Loading groups...</p>' +
          '</div>' +
          '<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--color-border-light)">' +
            '<h4 style="margin:0 0 8px;font-size:13px;font-weight:600">Add New Group</h4>' +
            '<div class="settings-field"><label>Group name</label><input type="text" id="admin-new-group" autocomplete="off" placeholder="e.g. editors"></div>' +
            '<button class="btn btn-primary btn-sm" id="admin-add-group">Add Group</button>' +
            '<span class="settings-msg" id="admin-group-msg"></span>' +
          '</div>' +
          '<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--color-border-light)">' +
            '<h4 style="margin:0 0 8px;font-size:13px;font-weight:600">Manage Members</h4>' +
            '<div class="settings-field"><label>Group</label><select id="admin-gm-group"><option value="">Select group...</option></select></div>' +
            '<div class="settings-field"><label>User</label><select id="admin-gm-user"><option value="">Select user...</option></select></div>' +
            '<div style="display:flex;gap:8px">' +
              '<button class="btn btn-primary btn-sm" id="admin-gm-add">Add to Group</button>' +
              '<button class="btn btn-sm" id="admin-gm-remove">Remove from Group</button>' +
            '</div>' +
            '<span class="settings-msg" id="admin-gm-msg"></span>' +
          '</div>' +
        '</div>' +
        '<div class="settings-section">' +
          '<h3>External Paths</h3>' +
          '<p style="font-size:13px;color:var(--color-fg-secondary);margin:0 0 8px">' +
            'Allow symlinks to directories outside the WebDAV tree.</p>' +
          '<div id="admin-extpath-list">' +
            '<p style="font-size:13px;color:var(--color-fg-secondary)">Loading...</p>' +
          '</div>' +
          '<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--color-border-light)">' +
            '<h4 style="margin:0 0 8px;font-size:13px;font-weight:600">Add Allowed Path</h4>' +
            '<div class="settings-field">' +
              '<label>Filesystem path</label>' +
              '<input type="text" id="admin-extpath-input" placeholder="/path/to/directory">' +
            '</div>' +
            '<button class="btn btn-primary btn-sm" id="admin-add-extpath">Add Path</button>' +
            '<span class="settings-msg" id="admin-extpath-msg"></span>' +
          '</div>' +
          '<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--color-border-light)">' +
            '<h4 style="margin:0 0 8px;font-size:13px;font-weight:600">Create External Symlink</h4>' +
            '<div class="settings-field">' +
              '<label>Target filesystem path</label>' +
              '<input type="text" id="admin-extsym-target" placeholder="/path/to/target">' +
            '</div>' +
            '<div class="settings-field">' +
              '<label>Link location (DAV path)</label>' +
              '<input type="text" id="admin-extsym-link" placeholder="/dav/shared/mylink">' +
            '</div>' +
            '<button class="btn btn-primary btn-sm" id="admin-create-extsym">Create Symlink</button>' +
            '<span class="settings-msg" id="admin-extsym-msg"></span>' +
          '</div>' +
        '</div>';

    }

    Dialog.open('Settings — ' + settings.username, wrap, { wide: true });

    // Cloud storage: load mounts and wire add button
    if (settings.rcloneAvailable) {
      this._loadCloudMounts();
      const addMountBtn = document.getElementById('cloud-add-mount');
      if (addMountBtn) addMountBtn.addEventListener('click', () => this._showAddMountDialog());
    }

    // Admin: load user list and wire up actions
    if (settings.admin) {
      this._loadAdminUserList();
      this._loadAdminGroups();
      document.getElementById('admin-add-group').addEventListener('click', async () => {
        const msg = document.getElementById('admin-group-msg');
        const name = document.getElementById('admin-new-group').value.trim();
        if (!name) { msg.textContent = 'Name required'; msg.className = 'settings-msg error'; return; }
        try {
          const resp = await fetch(this.davUrl + '_admin/addgroup', {
            method: 'POST', credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name })
          });
          const data = await resp.json();
          if (data.ok) {
            msg.textContent = 'Group created'; msg.className = 'settings-msg success';
            document.getElementById('admin-new-group').value = '';
            this._loadAdminGroups();
          } else { msg.textContent = data.error || 'Failed'; msg.className = 'settings-msg error'; }
        } catch (e) { msg.textContent = 'Connection error'; msg.className = 'settings-msg error'; }
      });
      document.getElementById('admin-gm-add').addEventListener('click', async () => {
        const msg = document.getElementById('admin-gm-msg');
        const group = document.getElementById('admin-gm-group').value;
        const username = document.getElementById('admin-gm-user').value;
        if (!group || !username) { msg.textContent = 'Select both group and user'; msg.className = 'settings-msg error'; return; }
        try {
          const resp = await fetch(this.davUrl + '_admin/groupmember', {
            method: 'POST', credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ group, username, action: 'add' })
          });
          const data = await resp.json();
          if (data.ok) { msg.textContent = 'Added'; msg.className = 'settings-msg success'; this._loadAdminGroups(); }
          else { msg.textContent = data.error || 'Failed'; msg.className = 'settings-msg error'; }
        } catch (e) { msg.textContent = 'Connection error'; msg.className = 'settings-msg error'; }
      });
      document.getElementById('admin-gm-remove').addEventListener('click', async () => {
        const msg = document.getElementById('admin-gm-msg');
        const group = document.getElementById('admin-gm-group').value;
        const username = document.getElementById('admin-gm-user').value;
        if (!group || !username) { msg.textContent = 'Select both group and user'; msg.className = 'settings-msg error'; return; }
        try {
          const resp = await fetch(this.davUrl + '_admin/groupmember', {
            method: 'POST', credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ group, username, action: 'remove' })
          });
          const data = await resp.json();
          if (data.ok) { msg.textContent = 'Removed'; msg.className = 'settings-msg success'; this._loadAdminGroups(); }
          else { msg.textContent = data.error || 'Failed'; msg.className = 'settings-msg error'; }
        } catch (e) { msg.textContent = 'Connection error'; msg.className = 'settings-msg error'; }
      });
      document.getElementById('admin-add-user').addEventListener('click', async () => {
        const msg = document.getElementById('admin-add-msg');
        const username = document.getElementById('admin-new-user').value.trim();
        const password = document.getElementById('admin-new-pass').value;
        if (!username || !password) { msg.textContent = 'Both fields required'; msg.className = 'settings-msg error'; return; }
        try {
          const resp = await fetch(this.davUrl + '_admin/adduser', {
            method: 'POST', credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password, requirePasswordChange: document.getElementById('admin-new-force-pw').checked })
          });
          const data = await resp.json();
          if (data.ok) {
            msg.textContent = 'User created';
            msg.className = 'settings-msg success';
            document.getElementById('admin-new-user').value = '';
            document.getElementById('admin-new-pass').value = '';
            this._loadAdminUserList();
          } else {
            msg.textContent = data.error || 'Failed';
            msg.className = 'settings-msg error';
          }
        } catch (e) {
          msg.textContent = 'Connection error';
          msg.className = 'settings-msg error';
        }
      });

      // External paths
      this._loadExtPaths();
      document.getElementById('admin-add-extpath').addEventListener('click', async () => {
        const msg = document.getElementById('admin-extpath-msg');
        const path = document.getElementById('admin-extpath-input').value.trim();
        if (!path) { msg.textContent = 'Path required'; msg.className = 'settings-msg error'; return; }
        try {
          const resp = await fetch(this.davUrl + '_admin/addextpath', {
            method: 'POST', credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path })
          });
          const data = await resp.json();
          if (data.ok) {
            msg.textContent = 'Path added'; msg.className = 'settings-msg success';
            document.getElementById('admin-extpath-input').value = '';
            this._loadExtPaths();
          } else { msg.textContent = data.error || 'Failed'; msg.className = 'settings-msg error'; }
        } catch (e) { msg.textContent = 'Connection error'; msg.className = 'settings-msg error'; }
      });
      document.getElementById('admin-create-extsym').addEventListener('click', async () => {
        const msg = document.getElementById('admin-extsym-msg');
        const target = document.getElementById('admin-extsym-target').value.trim();
        const link = document.getElementById('admin-extsym-link').value.trim();
        if (!target || !link) { msg.textContent = 'Both fields required'; msg.className = 'settings-msg error'; return; }
        try {
          const resp = await fetch(this.davUrl + '_admin/symlink', {
            method: 'POST', credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ target, link })
          });
          const data = await resp.json();
          if (data.ok) {
            msg.textContent = 'Symlink created'; msg.className = 'settings-msg success';
            document.getElementById('admin-extsym-target').value = '';
            document.getElementById('admin-extsym-link').value = '';
            FileList.reload();
          } else { msg.textContent = data.error || 'Failed'; msg.className = 'settings-msg error'; }
        } catch (e) { msg.textContent = 'Connection error'; msg.className = 'settings-msg error'; }
      });
    }

    // Populate theme dropdown with custom themes from server
    var themeSelect = document.getElementById('set-theme');
    fetch(this.davUrl + '_themes', { credentials: 'same-origin' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.ok && data.themes && data.themes.length) {
          var sep = document.createElement('option');
          sep.disabled = true;
          sep.textContent = '───────────';
          themeSelect.appendChild(sep);
          data.themes.forEach(function(t) {
            var opt = document.createElement('option');
            opt.value = t.name;
            opt.textContent = t.label;
            themeSelect.appendChild(opt);
          });
        }
        themeSelect.value = currentTheme;
      }).catch(function() {});

    // Theme switcher — apply immediately and save to server
    themeSelect.addEventListener('change', async (e) => {
      var theme = e.target.value;
      Auth.theme = theme;
      App.applyTheme(theme);
      try {
        await fetch(this.davUrl + '_settings/theme', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ theme: theme })
        });
      } catch (err) {}
    });

    // Grid view adjustment
    document.getElementById('set-grid-adjust').addEventListener('click', () => {
      this._openGridAdjust();
    });

    // ONLYOFFICE autosave — save immediately
    document.getElementById('set-oo-autosave').addEventListener('change', async (e) => {
      Auth.ooAutosave = e.target.checked;
      try {
        await fetch(this.davUrl + '_settings/ooAutosave', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ooAutosave: e.target.checked })
        });
      } catch (err) {}
    });

    // Code editor theme — save immediately
    document.getElementById('set-cm-theme').addEventListener('change', async (e) => {
      var val = e.target.value;
      Auth.cmTheme = val;
      try {
        await fetch(this.davUrl + '_settings/cmTheme', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ cmTheme: val })
        });
      } catch (err) {}
    });

    // Terminal theme
    if (Auth.terminal) {
      document.getElementById('set-term-theme').addEventListener('change', function(e) {
        var customWrap = document.getElementById('set-term-custom-wrap');
        customWrap.hidden = e.target.value !== 'custom';
      });
      document.getElementById('set-save-term-theme').addEventListener('click', async () => {
        var msg = document.getElementById('set-term-theme-msg');
        var sel = document.getElementById('set-term-theme').value;
        var value;
        if (sel === 'custom') {
          var raw = document.getElementById('set-term-custom').value.trim();
          if (!raw) { msg.textContent = 'Enter a JSON theme'; msg.className = 'settings-msg error'; return; }
          try { JSON.parse(raw); } catch(e) {
            msg.textContent = 'Invalid JSON'; msg.className = 'settings-msg error'; return;
          }
          value = raw;
        } else {
          value = sel;
        }
        try {
          var resp = await fetch(this.davUrl + '_settings/termTheme', {
            method: 'POST', credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ termTheme: value })
          });
          var data = await resp.json();
          if (data.ok) {
            Auth.termTheme = value;
            msg.textContent = 'Saved'; msg.className = 'settings-msg success';
          } else { msg.textContent = data.error || 'Failed'; msg.className = 'settings-msg error'; }
        } catch(e) { msg.textContent = 'Connection error'; msg.className = 'settings-msg error'; }
      });
    }

    // Change password (hidden in demo mode)
    if (!Auth.demoMode)
    document.getElementById('set-change-pass').addEventListener('click', async () => {
      const msg = document.getElementById('set-pass-msg');
      const curPass = document.getElementById('set-cur-pass').value;
      const newPass = document.getElementById('set-new-pass').value;
      const confirmPass = document.getElementById('set-confirm-pass').value;

      if (!curPass || !newPass) { msg.textContent = 'Please fill in all fields'; msg.className = 'settings-msg error'; return; }
      if (newPass !== confirmPass) { msg.textContent = 'Passwords do not match'; msg.className = 'settings-msg error'; return; }
      if (newPass.length < 4) { msg.textContent = 'Password must be at least 4 characters'; msg.className = 'settings-msg error'; return; }

      try {
        const resp = await fetch(this.davUrl + '_settings/password', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ currentPassword: curPass, newPassword: newPass })
        });
        const data = await resp.json();
        if (data.ok) {
          msg.textContent = 'Password changed';
          msg.className = 'settings-msg success';
          document.getElementById('set-cur-pass').value = '';
          document.getElementById('set-new-pass').value = '';
          document.getElementById('set-confirm-pass').value = '';
        } else {
          msg.textContent = data.error || 'Failed';
          msg.className = 'settings-msg error';
        }
      } catch (e) {
        msg.textContent = 'Connection error';
        msg.className = 'settings-msg error';
      }
    });

    // Save timeout (hidden in demo mode)
    if (!Auth.demoMode)
    document.getElementById('set-save-timeout').addEventListener('click', async () => {
      const msg = document.getElementById('set-timeout-msg');
      const timeout = parseInt(document.getElementById('set-timeout').value);

      try {
        const resp = await fetch(this.davUrl + '_settings/timeout', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ timeout: timeout })
        });
        const data = await resp.json();
        if (data.ok) {
          msg.textContent = 'Saved';
          msg.className = 'settings-msg success';
        } else {
          msg.textContent = data.error || 'Failed';
          msg.className = 'settings-msg error';
        }
      } catch (e) {
        msg.textContent = 'Connection error';
        msg.className = 'settings-msg error';
      }
    });

    // Revoke all sessions (hidden in demo mode)
    if (!Auth.demoMode)
    document.getElementById('set-revoke').addEventListener('click', async () => {
      if (!await Dialog.confirm('This will log you out of all devices including this one. Continue?', 'Log Out All', true)) return;
      try {
        await fetch(this.davUrl + '_settings/revoke', {
          method: 'POST', credentials: 'same-origin'
        });
        Auth.logout();
      } catch (e) {
        Toast.error('Failed to revoke sessions');
      }
    });
  },

  async _loadAdminUserList() {
    const container = document.getElementById('admin-user-list');
    if (!container) return;
    try {
      const resp = await fetch(this.davUrl + '_admin/users', { credentials: 'same-origin' });
      const data = await resp.json();
      if (!data.ok || !data.users) { container.innerHTML = '<p style="color:var(--color-danger)">Failed to load users</p>'; return; }

      const currentUser = Auth.username;
      let html = '<table class="admin-table"><thead><tr>' +
        '<th>Username</th><th>Role</th><th>Created</th><th>Actions</th>' +
        '</tr></thead><tbody>';

      data.users.forEach(function(u) {
        const isSelf = u.username === currentUser;
        const roleLabel = u.admin ? 'Admin' : 'User';
        const created = u.created ? new Date(u.created).toLocaleDateString() : '—';
        const roleBtnTitle = isSelf ? 'Cannot change own role' : (u.admin ? 'Demote to user' : 'Promote to admin');
        const roleBtnClass = u.admin ? 'admin-demote' : 'admin-promote';
        html += '<tr>' +
          '<td>' + _escHtml(u.username) + (isSelf ? ' <em>(you)</em>' : '') + '</td>' +
          '<td><span class="admin-role-badge' + (u.admin ? ' admin' : '') + '">' + roleLabel + '</span></td>' +
          '<td>' + created + '</td>' +
          '<td class="admin-actions">' +
            '<button class="btn btn-sm admin-btn-reset" data-user="' + _escAttr(u.username) + '"' +
              ' title="Reset password">Reset Pw</button>' +
            '<button class="btn btn-sm ' + roleBtnClass + '" data-user="' + _escAttr(u.username) + '"' +
              ' data-admin="' + (u.admin ? 'true' : 'false') + '"' +
              (isSelf ? ' disabled' : '') +
              ' title="' + roleBtnTitle + '">' + (u.admin ? 'Demote' : 'Promote') + '</button>' +
            '<button class="btn btn-sm admin-btn-del btn-danger" data-user="' + _escAttr(u.username) + '"' +
              ((isSelf || u.admin) ? ' disabled' : '') +
              ' title="' + (isSelf ? 'Cannot delete yourself' : u.admin ? 'Demote from admin first' : 'Delete user (files preserved)') + '">Delete</button>' +
            '<button class="btn btn-sm admin-btn-term' + (u.terminal ? ' active' : '') + '" data-user="' + _escAttr(u.username) + '"' +
              ' title="' + (u.terminal ? 'Disable terminal' : 'Enable terminal') + '">' + (u.terminal ? 'Term \u2713' : 'Term') + '</button>' +
            '<button class="btn btn-sm admin-btn-vnc' + (u.vnc ? ' active' : '') + '" data-user="' + _escAttr(u.username) + '"' +
              ' title="' + (u.vnc ? 'Disable VNC' : 'Enable VNC') + '">' + (u.vnc ? 'VNC* \u2713' : 'VNC*') + '</button>' +
          '</td></tr>';
      });
      html += '</tbody></table>';
      html += '<p style="font-size:11px;color:var(--color-fg-secondary);margin-top:6px">* VNC is experimental. Connection to some VNC servers may fail due to encoding incompatibilities.</p>';
      container.innerHTML = html;

      // Wire up action buttons
      const self = this;
      container.querySelectorAll('.admin-btn-reset').forEach(function(btn) {
        btn.addEventListener('click', function() { self._adminResetPassword(btn.dataset.user); });
      });
      container.querySelectorAll('.admin-promote, .admin-demote').forEach(function(btn) {
        btn.addEventListener('click', function() { self._adminToggleRole(btn.dataset.user, btn.dataset.admin === 'true'); });
      });
      container.querySelectorAll('.admin-btn-del').forEach(function(btn) {
        btn.addEventListener('click', function() { self._adminDeleteUser(btn.dataset.user); });
      });
      container.querySelectorAll('.admin-btn-term').forEach(function(btn) {
        btn.addEventListener('click', function() { self._adminToggleTerminal(btn.dataset.user); });
      });
      container.querySelectorAll('.admin-btn-vnc').forEach(function(btn) {
        btn.addEventListener('click', function() { self._adminToggleVnc(btn.dataset.user); });
      });
    } catch (e) {
      container.innerHTML = '<p style="color:var(--color-danger)">Failed to load users</p>';
    }
  },

  async _loadAdminGroups() {
    const container = document.getElementById('admin-group-list');
    if (!container) return;
    try {
      const [grpResp, usrResp] = await Promise.all([
        fetch(this.davUrl + '_admin/groups', { credentials: 'same-origin' }),
        fetch(this.davUrl + '_admin/users', { credentials: 'same-origin' })
      ]);
      const grpData = await grpResp.json();
      const usrData = await usrResp.json();
      if (!grpData.ok) { container.innerHTML = '<p style="color:var(--color-danger)">Failed to load groups</p>'; return; }

      // Populate group dropdown
      const grpSelect = document.getElementById('admin-gm-group');
      if (grpSelect) {
        grpSelect.innerHTML = '<option value="">Select group...</option>';
        grpData.groups.forEach(function(g) {
          grpSelect.innerHTML += '<option value="' + _escAttr(g.name) + '">' + _escHtml(g.name) + '</option>';
        });
      }
      // Populate user dropdown
      const usrSelect = document.getElementById('admin-gm-user');
      if (usrSelect && usrData.ok && usrData.users) {
        usrSelect.innerHTML = '<option value="">Select user...</option>';
        usrData.users.forEach(function(u) {
          usrSelect.innerHTML += '<option value="' + _escAttr(u.username) + '">' + _escHtml(u.username) + '</option>';
        });
      }

      if (grpData.groups.length === 0) {
        container.innerHTML = '<p style="font-size:13px;color:var(--color-fg-secondary)">No groups defined yet.</p>';
        return;
      }

      const self = this;
      let html = '<table class="admin-table"><thead><tr><th>Group</th><th>Members</th><th>Actions</th></tr></thead><tbody>';
      grpData.groups.forEach(function(g) {
        const members = g.members.length > 0 ? g.members.map(function(m) {
          return '<span class="group-member-badge">' + _escHtml(m) + '</span>';
        }).join(' ') : '<em style="color:var(--color-fg-muted)">none</em>';
        var delBtn = g.name === 'everyone' ? ''
          : '<button class="btn btn-sm btn-danger admin-btn-delgroup" data-group="' + _escAttr(g.name) + '" title="Delete group">Delete</button>';
        html += '<tr><td>' + _escHtml(g.name) + (g.name === 'everyone' ? ' <em style="font-size:11px;color:var(--color-fg-muted)">(all users)</em>' : '') +
          '</td><td>' + members + '</td>' +
          '<td class="admin-actions">' + delBtn + '</td></tr>';
      });
      html += '</tbody></table>';
      container.innerHTML = html;

      container.querySelectorAll('.admin-btn-delgroup').forEach(function(btn) {
        btn.addEventListener('click', async function() {
          const name = btn.dataset.group;
          if (!await Dialog.confirm('Delete group "' + name + '"? Files with this group will revert to nogroup.', 'Delete', true)) return;
          try {
            const resp = await fetch(self.davUrl + '_admin/delgroup', {
              method: 'POST', credentials: 'same-origin',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ name: name })
            });
            const data = await resp.json();
            if (data.ok) { Toast.show('Group "' + name + '" deleted'); self._loadAdminGroups(); }
            else { Toast.error(data.error || 'Failed'); }
          } catch (e) { Toast.error('Connection error'); }
        });
      });
    } catch (e) {
      container.innerHTML = '<p style="color:var(--color-danger)">Failed to load groups</p>';
    }
  },

  async _adminResetPassword(username) {
    var result = await new Promise(function(resolve) {
      var overlay = document.createElement('div');
      overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.35);z-index:300000;display:flex;align-items:center;justify-content:center';

      var box = document.createElement('div');
      box.style.cssText = 'background:var(--color-modal-bg,var(--color-bg));border:1px solid var(--color-modal-border,var(--color-border));border-radius:8px;padding:20px 24px 16px;max-width:400px;width:90vw;box-shadow:0 8px 24px rgba(0,0,0,0.3)';

      var title = document.createElement('div');
      title.style.cssText = 'font-weight:600;margin-bottom:12px;font-size:14px';
      title.textContent = 'Reset Password for "' + username + '"';

      var passField = document.createElement('div');
      passField.className = 'settings-field';
      passField.innerHTML = '<label>New Password</label><input type="password" autocomplete="new-password" style="width:100%">';
      var passInput = passField.querySelector('input');

      var checkField = document.createElement('div');
      checkField.style.cssText = 'margin:8px 0 16px';
      checkField.innerHTML = '<label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:13px"><input type="checkbox" checked> Require password change on login</label>';
      var checkbox = checkField.querySelector('input');

      var btnRow = document.createElement('div');
      btnRow.style.cssText = 'display:flex;justify-content:flex-end;gap:8px';
      var cancelBtn = document.createElement('button');
      cancelBtn.className = 'btn';
      cancelBtn.textContent = 'Cancel';
      var okBtn = document.createElement('button');
      okBtn.className = 'btn btn-primary';
      okBtn.textContent = 'Reset Password';
      btnRow.appendChild(cancelBtn);
      btnRow.appendChild(okBtn);

      box.appendChild(title);
      box.appendChild(passField);
      box.appendChild(checkField);
      box.appendChild(btnRow);
      overlay.appendChild(box);
      document.body.appendChild(overlay);

      var done = function(val) { overlay.remove(); resolve(val); };
      cancelBtn.addEventListener('click', function() { done(null); });
      okBtn.addEventListener('click', function() {
        done({ password: passInput.value, requirePasswordChange: checkbox.checked });
      });
      passInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') okBtn.click();
        if (e.key === 'Escape') done(null);
      });
      passInput.focus();
    });

    if (!result) return;
    if (!result.password) { Toast.error('Password is required'); return; }
    if (result.password.length < 4) { Toast.error('Password must be at least 4 characters'); return; }
    try {
      const resp = await fetch(this.davUrl + '_admin/resetpass', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username, password: result.password, requirePasswordChange: result.requirePasswordChange })
      });
      const data = await resp.json();
      if (data.ok) { Toast.show('Password reset for ' + username); }
      else { Toast.error(data.error || 'Failed'); }
    } catch (e) { Toast.error('Connection error'); }
  },

  async _adminToggleRole(username, currentlyAdmin) {
    const action = currentlyAdmin ? 'demote to regular user' : 'promote to admin';
    if (!await Dialog.confirm('Are you sure you want to ' + action + ' "' + username + '"?')) return;
    try {
      const resp = await fetch(this.davUrl + '_admin/toggleadmin', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username, admin: !currentlyAdmin })
      });
      const data = await resp.json();
      if (data.ok) {
        Toast.show(username + ' is now ' + (data.admin ? 'an admin' : 'a regular user'));
        this._loadAdminUserList();
      } else { Toast.error(data.error || 'Failed'); }
    } catch (e) { Toast.error('Connection error'); }
  },

  async _adminDeleteUser(username) {
    if (!await Dialog.confirm('Delete user "' + username + '"? Their files will be preserved on disk.', 'Delete', true)) return;
    try {
      const resp = await fetch(this.davUrl + '_admin/deluser', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username })
      });
      const data = await resp.json();
      if (data.ok) {
        Toast.show('User ' + username + ' deleted');
        this._loadAdminUserList();
      } else { Toast.error(data.error || 'Failed'); }
    } catch (e) { Toast.error('Connection error'); }
  },

  async _adminToggleTerminal(username) {
    try {
      const resp = await fetch(this.davUrl + '_admin/terminal', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username })
      });
      const data = await resp.json();
      if (data.ok) {
        Toast.show('Terminal ' + (data.terminal ? 'enabled' : 'disabled') + ' for ' + username);
        this._loadAdminUserList();
      } else { Toast.error(data.error || 'Failed'); }
    } catch (e) { Toast.error('Connection error'); }
  },

  async _adminToggleVnc(username) {
    try {
      const resp = await fetch(this.davUrl + '_admin/vnc', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username })
      });
      const data = await resp.json();
      if (data.ok) {
        Toast.show('VNC ' + (data.vnc ? 'enabled' : 'disabled') + ' for ' + username);
        this._loadAdminUserList();
      } else { Toast.error(data.error || 'Failed'); }
    } catch (e) { Toast.error('Connection error'); }
  },

  async _loadExtPaths() {
    const container = document.getElementById('admin-extpath-list');
    if (!container) return;
    try {
      const resp = await fetch(this.davUrl + '_admin/extpaths', { credentials: 'same-origin' });
      const data = await resp.json();
      if (!data.ok) { container.innerHTML = '<p style="color:var(--color-danger)">Failed to load</p>'; return; }
      if (!data.paths || data.paths.length === 0) {
        container.innerHTML = '<p style="font-size:13px;color:var(--color-fg-secondary)">No external paths configured.</p>';
        return;
      }
      let html = '<table class="admin-table"><thead><tr><th>Path</th><th>Added</th><th></th></tr></thead><tbody>';
      data.paths.forEach(p => {
        const added = p.added ? new Date(p.added).toLocaleDateString() : '';
        html += '<tr><td><code>' + _escHtml(p.path) + '</code></td><td>' + added + '</td>' +
          '<td><button class="btn btn-sm btn-danger extpath-remove" data-path="' + _escAttr(p.path) + '">Remove</button></td></tr>';
      });
      html += '</tbody></table>';
      container.innerHTML = html;
      const self = this;
      container.querySelectorAll('.extpath-remove').forEach(btn => {
        btn.addEventListener('click', async () => {
          if (!await Dialog.confirm('Remove external path "' + btn.dataset.path + '"? Existing symlinks to this path will become broken.', 'Remove', true)) return;
          try {
            const resp = await fetch(self.davUrl + '_admin/delextpath', {
              method: 'POST', credentials: 'same-origin',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ path: btn.dataset.path })
            });
            const d = await resp.json();
            if (d.ok) self._loadExtPaths();
            else Toast.error(d.error || 'Failed');
          } catch (e) { Toast.error('Connection error'); }
        });
      });
    } catch (e) {
      container.innerHTML = '<p style="color:var(--color-danger)">Failed to load</p>';
    }
  },

  // ---- Cloud Storage Methods ----

  async _loadCloudMounts() {
    const container = document.getElementById('cloud-mount-list');
    if (!container) return;
    try {
      const resp = await fetch(this.davUrl + '_rclone/mounts', { credentials: 'same-origin' });
      const data = await resp.json();

      if (!data.ok || !data.mounts || data.mounts.length === 0) {
        container.innerHTML = '<p style="font-size:13px;color:var(--color-fg-secondary)">No cloud storage mounts configured.</p>';
        return;
      }

      let html = '<table class="admin-table"><thead><tr>' +
        '<th>Name</th><th>Type</th><th>Status</th><th>Actions</th>' +
        '</tr></thead><tbody>';

      for (const m of data.mounts) {
        const statusClass = m.mounted ? 'cloud-status-ok' : 'cloud-status-err';
        var statusFlags = [];
        if (m.readOnly) statusFlags.push('read-only');
        if (m.rootMount) statusFlags.push('root');
        const statusText = m.stale
          ? 'Reconnect failed'
          : m.mounted
            ? ('Mounted' + (statusFlags.length ? ' (' + statusFlags.join(', ') + ')' : ''))
            : 'Disconnected';
        html += '<tr>' +
          '<td>' + _escHtml(m.name) + '</td>' +
          '<td>' + _escHtml(m.type) + '</td>' +
          '<td><span class="cloud-status ' + statusClass + '">' + statusText + '</span></td>' +
          '<td class="admin-actions">' +
            (!m.mounted ? '<button class="btn btn-sm cloud-remount-btn" data-name="' +
              _escAttr(m.name) + '" data-provider="' + _escAttr(m.provider || '') + '">Mount</button>' :
              '<button class="btn btn-sm cloud-unmount-btn" data-name="' +
              _escAttr(m.name) + '">Unmount</button>') +
            '<button class="btn btn-sm btn-danger cloud-remove-btn" data-name="' +
              _escAttr(m.name) + '">Remove</button>' +
          '</td></tr>';
      }
      html += '</tbody></table>';

      // Add "Remount All" button if there are multiple unmounted SFTP mounts
      var unmountedSftp = data.mounts.filter(function(m) { return !m.mounted && m.provider === 'sftp'; });
      if (unmountedSftp.length > 1) {
        html += '<div style="margin-top:8px;text-align:right">' +
          '<button class="btn btn-sm" id="cloud-remount-all-btn">Remount All SSH (' + unmountedSftp.length + ')</button></div>';
      }

      container.innerHTML = html;

      const self = this;

      // Remount All button
      var remountAllBtn = document.getElementById('cloud-remount-all-btn');
      if (remountAllBtn) {
        remountAllBtn.addEventListener('click', async () => {
          var pw = await Dialog.miniPrompt('Enter your login password to remount all SSH mounts');
          if (pw === null) return;
          remountAllBtn.disabled = true;
          remountAllBtn.textContent = 'Mounting...';
          try {
            const resp = await fetch(self.davUrl + '_rclone/remountAll', {
              method: 'POST', credentials: 'same-origin',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({password: pw})
            });
            const d = await resp.json();
            if (d.ok) {
              var mounted = (d.results || []).filter(function(r) { return r.ok; });
              var failed = (d.results || []).filter(function(r) { return !r.ok; });
              if (mounted.length) {
                Toast.success('Mounted ' + mounted.map(function(r) { return r.name; }).join(', '));
                Tree.init(); Tree.loadRoot();
              }
              if (failed.length) Toast.error('Failed: ' + failed.map(function(r) { return r.name; }).join(', '));
            } else Toast.error(d.error || 'Remount failed');
          } catch(e) { Toast.error('Connection error'); }
          self._loadCloudMounts();
        });
      }

      container.querySelectorAll('.cloud-remount-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
          var body = {name: btn.dataset.name};
          if (btn.dataset.provider === 'sftp') {
            var pw = await Dialog.miniPrompt('Enter your login password to mount "' + btn.dataset.name + '"');
            if (pw === null) return;
            body.password = pw;
          }
          btn.disabled = true;
          btn.textContent = 'Mounting...';
          try {
            const resp = await fetch(self.davUrl + '_rclone/remount', {
              method: 'POST', credentials: 'same-origin',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify(body)
            });
            const d = await resp.json();
            if (d.ok) {
              Toast.success('Mounted ' + btn.dataset.name);
              Tree.init(); Tree.loadRoot();
            }
            else Toast.error(d.error || 'Mount failed');
          } catch(e) { Toast.error('Connection error'); }
          self._loadCloudMounts();
        });
      });

      container.querySelectorAll('.cloud-unmount-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
          btn.disabled = true;
          btn.textContent = 'Unmounting...';
          try {
            const resp = await fetch(self.davUrl + '_rclone/unmount', {
              method: 'POST', credentials: 'same-origin',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({name: btn.dataset.name})
            });
            const d = await resp.json();
            if (d.ok) {
              Toast.success('Unmounted ' + btn.dataset.name);
              var un = btn.dataset.name;
              if (Auth.mountNames) Auth.mountNames = Auth.mountNames.filter(function(n) { return n !== un; });
              FileList.reload();
              Tree.init(); Tree.loadRoot();
            } else Toast.error(d.error || 'Unmount failed');
          } catch(e) { Toast.error('Connection error'); }
          self._loadCloudMounts();
        });
      });

      container.querySelectorAll('.cloud-remove-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
          if (!await Dialog.confirm('Remove cloud storage mount "' + btn.dataset.name + '"? This will disconnect the storage.', 'Remove', true)) return;
          try {
            const resp = await fetch(self.davUrl + '_rclone/remove', {
              method: 'POST', credentials: 'same-origin',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({name: btn.dataset.name})
            });
            const d = await resp.json();
            if (d.ok) {
              Toast.success('Removed ' + btn.dataset.name);
              var rn = btn.dataset.name;
              if (Auth.mountNames) Auth.mountNames = Auth.mountNames.filter(function(n) { return n !== rn; });
              if (Auth.readOnlyMounts) Auth.readOnlyMounts = Auth.readOnlyMounts.filter(function(n) { return n !== rn; });
              FileList.reload();
              Tree.init(); Tree.loadRoot();
            } else Toast.error(d.error || 'Remove failed');
          } catch(e) { Toast.error('Connection error'); }
          self._loadCloudMounts();
        });
      });
    } catch(e) {
      container.innerHTML = '<p style="font-size:13px;color:var(--color-danger)">Failed to load mounts.</p>';
    }
  },

  async _showAddMountDialog() {
    let rcStatus;
    try {
      const resp = await fetch(this.davUrl + '_rclone/status', { credentials: 'same-origin' });
      rcStatus = await resp.json();
    } catch(e) {
      Toast.error('Failed to load provider info');
      return;
    }

    const providers = rcStatus.providers || {};

    const wrap = document.createElement('div');
    wrap.className = 'settings-panel';

    let html = '<form autocomplete="off" onsubmit="return false"><div class="settings-section">' +
      '<h3>Choose Provider</h3>' +
      '<div class="settings-field"><label>Mount name</label>' +
        '<input type="text" id="cloud-mount-name" name="cloud-mount-name-' + Date.now() + '" placeholder="mydrive" maxlength="32" autocomplete="new-password"></div>' +
      '<div class="settings-field"><label style="cursor:pointer">' +
        '<input type="checkbox" id="cloud-read-only" style="margin-right:6px;vertical-align:middle">' +
        'Mount read-only</label></div>' +
      (Auth.admin ? '<div class="settings-field"><label style="cursor:pointer">' +
        '<input type="checkbox" id="cloud-root-mount" style="margin-right:6px;vertical-align:middle">' +
        'Mount in root directory (visible to all users)</label></div>' : '') +
      '<div class="settings-field"><label>Provider</label>' +
        '<select id="cloud-provider">';

    // OAuth providers
    ['drive', 'dropbox', 'onedrive'].forEach(p => {
      if (providers[p]) {
        html += '<option value="' + p + '" data-tier="oauth">' + _escHtml(providers[p].label) + '</option>';
      }
    });
    // S3-compatible
    ['s3', 'b2', 'wasabi', 'minio'].forEach(p => {
      if (providers[p]) {
        html += '<option value="' + p + '" data-tier="s3">' + _escHtml(providers[p].label) + '</option>';
      }
    });
    // SFTP
    html += '<option value="sftp" data-tier="sftp">SFTP (SSH)</option>';
    html += '<option value="_manual" data-tier="manual">Other (Manual)</option>';
    html += '</select></div></div>';

    // S3 credential fields
    html += '<div class="settings-section" id="cloud-s3-fields" hidden>' +
      '<h3>S3 Credentials</h3>' +
      '<div class="settings-field"><label>Access Key ID</label>' +
        '<input type="text" id="cloud-s3-key"></div>' +
      '<div class="settings-field"><label>Secret Access Key</label>' +
        '<input type="password" id="cloud-s3-secret"></div>' +
      '<div class="settings-field"><label>Endpoint</label>' +
        '<input type="text" id="cloud-s3-endpoint" placeholder="s3.amazonaws.com"></div>' +
      '<div class="settings-field"><label>Region</label>' +
        '<input type="text" id="cloud-s3-region" placeholder="us-east-1"></div>' +
      '</div>';

    // SFTP fields
    html += '<div class="settings-section" id="cloud-sftp-fields" hidden>' +
      '<h3>SFTP Connection</h3>' +
      '<div class="settings-field"><label>Host</label>' +
        '<input type="text" id="cloud-sftp-host" placeholder="example.com"></div>' +
      '<div class="settings-field"><label>Port</label>' +
        '<input type="text" id="cloud-sftp-port" placeholder="22" value="22"></div>' +
      '<div class="settings-field"><label>Username</label>' +
        '<input type="text" id="cloud-sftp-user"></div>' +
      '<div class="settings-field"><label>Auth method</label>' +
        '<select id="cloud-sftp-auth">' +
          '<option value="password">Password</option>' +
          '<option value="key">Private Key</option>' +
        '</select></div>' +
      '<div id="cloud-sftp-pass-row" class="settings-field"><label>Password</label>' +
        '<input type="password" id="cloud-sftp-pass"></div>' +
      '<div class="settings-field"><label>Remote path <span style="font-weight:normal;color:var(--color-fg-secondary)">(optional, defaults to home)</span></label>' +
        '<input type="text" id="cloud-sftp-path" placeholder="/path/on/server"></div>' +
      '<div id="cloud-sftp-key-rows" hidden>' +
        '<div class="settings-field"><label>Private Key (PEM)</label>' +
          '<textarea id="cloud-sftp-key" rows="6" ' +
          'style="flex:1;font-family:monospace;font-size:12px" ' +
          'placeholder="-----BEGIN OPENSSH PRIVATE KEY-----&#10;...&#10;-----END OPENSSH PRIVATE KEY-----"></textarea></div>' +
        '<div class="settings-field"><label>Key passphrase <span style="font-weight:normal;color:var(--color-fg-secondary)">(optional)</span></label>' +
          '<input type="password" id="cloud-sftp-keypass"></div>' +
      '</div>' +
      '</div>';

    // OAuth section — uses rclone authorize (no admin setup needed)
    html += '<div class="settings-section" id="cloud-oauth-fields" hidden>' +
      '<h3>Authorization</h3>' +
      '<p style="font-size:13px;color:var(--color-fg-secondary);margin:0 0 8px">' +
        'Click below to authorize. A popup will open for you to sign in with the provider.</p>' +
      '<button class="btn btn-primary btn-sm" id="cloud-oauth-start">Authorize</button>' +
      '<span class="settings-msg" id="cloud-oauth-msg"></span>' +
      '<div id="cloud-oauth-relay" hidden style="margin-top:12px;padding:8px;' +
        'background:var(--color-bg-hover);border-radius:var(--border-radius)">' +
        '<p style="font-size:13px;color:var(--color-fg-secondary);margin:0 0 4px">' +
          'After authorizing, your browser may show an error page ' +
          '(it tries to redirect to a local address). ' +
          'Copy the <strong>entire URL</strong> from the address bar of that error page and paste it here:</p>' +
        '<div style="display:flex;gap:4px">' +
          '<input type="text" id="cloud-oauth-relay-url" style="flex:1" ' +
            'placeholder="http://127.0.0.1:53682/?code=...">' +
          '<button class="btn btn-sm btn-primary" id="cloud-oauth-relay-btn">Submit</button>' +
        '</div>' +
      '</div>' +
      '</div>';

    // Manual fields
    html += '<div class="settings-section" id="cloud-manual-fields" hidden>' +
      '<h3>Manual Configuration</h3>' +
      '<div class="settings-field"><label>rclone type</label>' +
        '<input type="text" id="cloud-manual-type" placeholder="e.g. sftp, ftp, webdav"></div>' +
      '<div class="settings-field"><label>Parameters</label>' +
        '<textarea id="cloud-manual-params" rows="4" placeholder="key=value (one per line)" ' +
        'style="flex:1;font-family:monospace;font-size:12px"></textarea></div>' +
      '</div>';

    // Buttons
    html += '<div style="margin-top:12px;display:flex;gap:8px">' +
      '<button class="btn btn-primary" id="cloud-create-btn">Create Mount</button>' +
      '<button class="btn" id="cloud-cancel-btn">Cancel</button>' +
      '</div>' +
      '<span class="settings-msg" id="cloud-create-msg"></span>' +
      '</form>';

    wrap.innerHTML = html;
    Dialog.open('Add Cloud Storage', wrap, { wide: true });

    let oauthToken = null;
    let oauthPollTimer = null;
    let oauthStarted = false;
    let mountCreated = false;
    const self = this;

    // Provider selection toggles fields
    const providerSelect = document.getElementById('cloud-provider');
    const updateFields = () => {
      const opt = providerSelect.selectedOptions[0];
      const tier = opt ? opt.dataset.tier : '';
      document.getElementById('cloud-s3-fields').hidden = tier !== 's3';
      document.getElementById('cloud-sftp-fields').hidden = tier !== 'sftp';
      document.getElementById('cloud-oauth-fields').hidden = tier !== 'oauth';
      document.getElementById('cloud-manual-fields').hidden = tier !== 'manual';
      // Reset OAuth state on provider change
      oauthToken = null;
      if (oauthPollTimer) { clearInterval(oauthPollTimer); oauthPollTimer = null; }
      document.getElementById('cloud-oauth-relay').hidden = true;
      document.getElementById('cloud-oauth-start').disabled = false;
      var createBtn = document.getElementById('cloud-create-btn');
      createBtn.disabled = (tier === 'oauth');
      const oaMsg = document.getElementById('cloud-oauth-msg');
      if (oaMsg) { oaMsg.textContent = ''; oaMsg.className = 'settings-msg'; }
    };
    providerSelect.addEventListener('change', updateFields);
    updateFields();

    // SFTP auth method toggle
    var sftpAuthSelect = document.getElementById('cloud-sftp-auth');
    sftpAuthSelect.addEventListener('change', function() {
      var isKey = sftpAuthSelect.value === 'key';
      document.getElementById('cloud-sftp-pass-row').hidden = isKey;
      document.getElementById('cloud-sftp-key-rows').hidden = !isKey;
    });

    // Helper: relay a 127.0.0.1:53682 callback URL through the server to rclone
    async function relayCallbackUrl(url, oaMsg) {
      try {
        const resp = await fetch(self.davUrl + '_oauth/relay', {
          method: 'POST', credentials: 'same-origin',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({url: url})
        });
        const data = await resp.json();
        if (data.ok) {
          if (data.token) {
            oauthToken = data.token;
            if (oauthPollTimer) { clearInterval(oauthPollTimer); oauthPollTimer = null; }
            if (popup && !popup.closed) popup.close();
            oaMsg.textContent = 'Authorized!';
            oaMsg.className = 'settings-msg success';
            document.getElementById('cloud-oauth-relay').hidden = true;
            document.getElementById('cloud-create-btn').disabled = false;
          } else {
            oaMsg.textContent = 'Callback relayed — waiting for token...';
            oaMsg.className = 'settings-msg';
          }
        } else {
          oaMsg.textContent = data.error || 'Relay failed';
          oaMsg.className = 'settings-msg error';
        }
      } catch(e) {
        oaMsg.textContent = 'Connection error';
        oaMsg.className = 'settings-msg error';
      }
    }

    // OAuth authorize via rclone authorize
    document.getElementById('cloud-oauth-start').addEventListener('click', async () => {
      const provider = providerSelect.value;
      const oaMsg = document.getElementById('cloud-oauth-msg');
      const startBtn = document.getElementById('cloud-oauth-start');
      startBtn.disabled = true;
      oauthStarted = true;
      oaMsg.textContent = 'Starting authorization...';
      oaMsg.className = 'settings-msg';

      try {
        const resp = await fetch(self.davUrl + '_oauth/start', {
          method: 'POST', credentials: 'same-origin',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({provider: provider})
        });
        const data = await resp.json();
        if (!data.ok) {
          oaMsg.textContent = data.error || 'Failed to start authorization';
          oaMsg.className = 'settings-msg error';
          startBtn.disabled = false;
          oauthStarted = false;
          return;
        }

        // Open consent URL in a popup
        const popup = window.open(data.authUrl, 'oauth_' + provider, 'width=600,height=700');

        oaMsg.textContent = 'Authorize in the popup window...';
        oaMsg.className = 'settings-msg';

        // Monitor popup — try to auto-detect redirect to 127.0.0.1:53682
        // After the user authorizes, the provider redirects to rclone's localhost
        // listener, which fails in the user's browser. In many browsers, we can
        // read the popup URL when the page fails to load (no cross-origin doc).
        let relayed = false;
        const popupCheck = setInterval(() => {
          if (oauthToken || relayed) { clearInterval(popupCheck); return; }
          try {
            if (popup && !popup.closed) {
              const loc = popup.location.href;
              if (loc && /127\.0\.0\.1.*[?&]code=/.test(loc)) {
                relayed = true;
                clearInterval(popupCheck);
                popup.close();
                relayCallbackUrl(loc, oaMsg);
              }
            } else if (popup && popup.closed) {
              clearInterval(popupCheck);
              // Popup closed — show manual relay if token not received
              if (!oauthToken && !relayed) {
                document.getElementById('cloud-oauth-relay').hidden = false;
              }
            }
          } catch(e) {
            // Cross-origin — popup still on provider's domain, keep waiting
          }
        }, 500);

        // Show relay fallback after a delay (in case auto-detect doesn't work)
        setTimeout(() => {
          if (!oauthToken && !relayed) {
            document.getElementById('cloud-oauth-relay').hidden = false;
          }
        }, 3000);

        // Poll server for token (rclone outputs it after callback is received)
        let pollCount = 0;
        oauthPollTimer = setInterval(async () => {
          pollCount++;
          if (oauthToken || pollCount > 120) {
            clearInterval(oauthPollTimer);
            oauthPollTimer = null;
            if (!oauthToken && pollCount > 120) {
              oaMsg.textContent = 'Authorization timed out';
              oaMsg.className = 'settings-msg error';
              startBtn.disabled = false;
            }
            return;
          }
          try {
            const pollResp = await fetch(self.davUrl + '_oauth/poll', { credentials: 'same-origin' });
            const pollData = await pollResp.json();
            if (pollData.ok && pollData.token) {
              oauthToken = pollData.token;
              clearInterval(oauthPollTimer);
              oauthPollTimer = null;
              clearInterval(popupCheck);
              if (popup && !popup.closed) popup.close();
              oaMsg.textContent = 'Authorized!';
              oaMsg.className = 'settings-msg success';
              document.getElementById('cloud-oauth-relay').hidden = true;
              document.getElementById('cloud-create-btn').disabled = false;
            } else if (pollData.error && !pollData.pending) {
              oaMsg.textContent = pollData.error;
              oaMsg.className = 'settings-msg error';
              clearInterval(oauthPollTimer);
              oauthPollTimer = null;
              startBtn.disabled = false;
            }
          } catch(e) {}
        }, 3000);

      } catch(e) {
        oaMsg.textContent = 'Connection error';
        oaMsg.className = 'settings-msg error';
        startBtn.disabled = false;
      }
    });

    // OAuth relay button — manual fallback for when auto-detect doesn't work
    document.getElementById('cloud-oauth-relay-btn').addEventListener('click', async () => {
      const relayUrl = document.getElementById('cloud-oauth-relay-url').value.trim();
      const oaMsg = document.getElementById('cloud-oauth-msg');
      if (!relayUrl) return;
      await relayCallbackUrl(relayUrl, oaMsg);
    });

    // Cancel
    document.getElementById('cloud-cancel-btn').addEventListener('click', async () => {
      if (oauthStarted && !mountCreated) {
        var confirmed = await Dialog.confirm('Authorization is in progress. Cancelling will stop the process. Continue?', 'Cancel', true);
        if (!confirmed) return;
        // Kill the rclone authorize process
        fetch(self.davUrl + '_oauth/cancel', { method: 'POST', credentials: 'same-origin' });
      }
      if (oauthPollTimer) clearInterval(oauthPollTimer);
      Dialog.close();
      self.openSettings();
    });

    // Create mount
    document.getElementById('cloud-create-btn').addEventListener('click', async () => {
      const msg = document.getElementById('cloud-create-msg');
      const name = document.getElementById('cloud-mount-name').value.trim().toLowerCase();
      const provider = providerSelect.value;
      const opt = providerSelect.selectedOptions[0];
      const tier = opt ? opt.dataset.tier : '';

      if (!name || !/^[a-z0-9._-]+$/.test(name)) {
        msg.textContent = 'Invalid name (use a-z, 0-9, ., -, _)';
        msg.className = 'settings-msg error';
        return;
      }

      let params = {};
      let rcloneType = provider;

      if (tier === 'oauth') {
        if (!oauthToken) {
          msg.textContent = 'Please authorize first';
          msg.className = 'settings-msg error';
          return;
        }
        params.token = oauthToken;
      } else if (tier === 's3') {
        params.access_key_id = document.getElementById('cloud-s3-key').value.trim();
        params.secret_access_key = document.getElementById('cloud-s3-secret').value.trim();
        params.endpoint = document.getElementById('cloud-s3-endpoint').value.trim();
        params.region = document.getElementById('cloud-s3-region').value.trim();
        if (!params.access_key_id || !params.secret_access_key) {
          msg.textContent = 'Access Key and Secret required';
          msg.className = 'settings-msg error';
          return;
        }
        if (provider !== 's3') params.provider = provider;
        rcloneType = 's3';
      } else if (tier === 'sftp') {
        params.host = document.getElementById('cloud-sftp-host').value.trim();
        params.user = document.getElementById('cloud-sftp-user').value.trim();
        params.port = document.getElementById('cloud-sftp-port').value.trim() || '22';
        var sftpPath = document.getElementById('cloud-sftp-path').value.trim();
        if (sftpPath) params.remotePath = sftpPath;
        if (!params.host || !params.user) {
          msg.textContent = 'Host and username required';
          msg.className = 'settings-msg error';
          return;
        }
        var sftpAuth = document.getElementById('cloud-sftp-auth').value;
        if (sftpAuth === 'key') {
          params.key_pem = document.getElementById('cloud-sftp-key').value;
          if (!params.key_pem.trim()) {
            msg.textContent = 'Private key required';
            msg.className = 'settings-msg error';
            return;
          }
          var keypass = document.getElementById('cloud-sftp-keypass').value;
          if (keypass) params.key_file_pass = keypass;
        } else {
          params.pass = document.getElementById('cloud-sftp-pass').value;
          if (!params.pass) {
            msg.textContent = 'Password required';
            msg.className = 'settings-msg error';
            return;
          }
        }
        var loginPw = await Dialog.miniPrompt('Enter your login password — it is used to encrypt the stored SFTP credentials');
        if (loginPw === null) return;
        params.loginPassword = loginPw;
        rcloneType = 'sftp';
      } else if (tier === 'manual') {
        rcloneType = document.getElementById('cloud-manual-type').value.trim();
        if (!rcloneType) {
          msg.textContent = 'rclone type required';
          msg.className = 'settings-msg error';
          return;
        }
        const lines = document.getElementById('cloud-manual-params').value.trim().split('\n');
        for (const line of lines) {
          const eq = line.indexOf('=');
          if (eq > 0) params[line.substring(0, eq).trim()] = line.substring(eq + 1).trim();
        }
      }

      msg.textContent = 'Creating mount...';
      msg.className = 'settings-msg';

      const readOnly = document.getElementById('cloud-read-only').checked;
      const rootMountEl = document.getElementById('cloud-root-mount');
      const rootMount = rootMountEl ? rootMountEl.checked : false;
      try {
        const resp = await fetch(self.davUrl + '_rclone/create', {
          method: 'POST', credentials: 'same-origin',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ name, type: rcloneType, tier, params, readOnly, rootMount })
        });
        const data = await resp.json();
        if (data.ok) {
          mountCreated = true;
          if (oauthPollTimer) clearInterval(oauthPollTimer);
          const mountMsg = data.mounted
            ? 'Mount "' + name + '" created and active!'
            : 'Mount "' + name + '" created but failed to mount: ' + (data.mountError || 'unknown');
          if (data.mounted) Toast.success(mountMsg);
          else Dialog.alert(mountMsg);
          // Update mount name lists for icons/zones
          if (data.mounted) {
            if (!Auth.mountNames) Auth.mountNames = [];
            if (Auth.mountNames.indexOf(name) === -1) Auth.mountNames.push(name);
            if (readOnly) {
              if (!Auth.readOnlyMounts) Auth.readOnlyMounts = [];
              if (Auth.readOnlyMounts.indexOf(name) === -1) Auth.readOnlyMounts.push(name);
            }
          }
          Dialog.close();
          FileList.reload();
          Tree.init(); Tree.loadRoot();
          self.openSettings();
        } else {
          msg.textContent = data.error || 'Creation failed';
          msg.className = 'settings-msg error';
        }
      } catch(e) {
        msg.textContent = 'Connection error';
        msg.className = 'settings-msg error';
      }
    });
  }
};

function _escHtml(s) {
  var d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
function _escAttr(s) {
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;');
}


/* -----------------------------------------------------------------------
 * Bootstrap
 * ----------------------------------------------------------------------- */

document.addEventListener('DOMContentLoaded', () => {
  App.init();
});
