// =====================================================================
// STREAMOUT (B13) — streamed render output: converter FS -> OPFS,
// forwarded as produced, never accumulated in converter memory.
//
// Why: a render used to write its output (e.g. dance-synced.mp4, up to
// ~1.02 GB for a full-size HEVC input) into the converter's MEMFS, so
// the output accumulated byte-by-byte inside wasm memory — plus the
// faststart relocation pass — until iOS killed the tab mid-mux (the
// device-verified B13 crash: export stage 2 milestones stopped at 60%
// on a 1,067,264,984-byte input). The slice delivery that streams
// FINISHED converter files to OPFS could never run, because the tab
// died before the file was finished.
//
// Mechanism: this block registers a small custom Emscripten
// filesystem (STREAMOPFS) mounted at /out inside the converter
// worker. Before an exec whose args contain an output path of the
// form /out/<name>, the patched EXEC case (see __streamOutBeforeExec)
// opens an OPFS sync access handle for the OPFS file <name> in THIS
// worker and truncates it. The filesystem's stream write op then
// forwards every byte the muxer writes to that handle positionally,
// in slices of at most 16 MB, synchronously as the bytes are produced
// — converter (wasm) memory never holds the output; peak output
// residency is the muxer's own small AVIO buffer, independent of
// output size. The read / llseek / getattr ops are positional through
// the same handle, so ffmpeg's own -movflags +faststart second pass
// (read the moov back, relocate it to the front in place) works
// unchanged against the OPFS file, and the delivered file is a valid
// faststart MP4 (moov before mdat) of exactly the render's duration.
// After the exec, __streamOutAfterExec flushes and closes the handle
// and logs the totals.
//
// The page adopts the OPFS file directly as the render result (no
// post-render read-out at all). If arming the stream fails before the
// exec (OPFS unavailable, handle locked, quota), the exec is refused
// BEFORE the mux starts and the render ends with an honest error —
// a render whose output was planned to stream NEVER falls back to
// accumulating that output in MEMFS (that unbounded fallback is the
// exact B13 death path). Renders only use MEMFS outputs at all when
// the page's own probe found streaming unavailable on this browser
// and never addressed the render to /out in the first place.
//
// Journal proof: [streamout] lines are posted on the converter's LOG
// channel when streaming is armed, at every 16 MB boundary crossed
// while the render proceeds (slices forwarded / total bytes), and at
// close with the final counts — so a diagnostics dump proves the
// output streamed out of the converter instead of residing in it.
// =====================================================================
var __streamOutState = {
  fs: null,
  impl: null,
  mounted: false,
  rootPromise: null,
  entries: {},
  SLICE_BYTES: 16 * 1024 * 1024
};
function __streamOutLog(msg) {
  try { self.postMessage({ type: "LOG", data: { type: "ffmpeg", message: "[streamout] " + msg } }); } catch (_) {}
}
// Full error text for the journal: name + message + errno when the
// value carries one. This matters because an Emscripten ErrnoError's
// message is the literal string "FS error" for EVERY errno — without
// the errno and the operation name, a diagnostics dump cannot say
// what actually failed (the round-1 probe failure was exactly that).
function __streamOutErrText(e) {
  if (e == null) return "unknown error";
  if (typeof e === "string") return e;
  var t = (e.name || "Error") + ": " + (e.message || String(e));
  if (e.errno !== undefined && e.errno !== null) t += " [errno " + e.errno + "]";
  return t;
}
// Used by the worker's top-level message handler (bottom of file) so
// errors posted back to the page keep their errno instead of being
// reduced to "ErrnoError: FS error" by Error#toString.
function __workerErrText(e) { return __streamOutErrText(e); }
function __streamOutRoot() {
  if (!__streamOutState.rootPromise) {
    var p = null;
    try { p = navigator.storage.getDirectory(); } catch (e) { p = Promise.reject(e); }
    __streamOutState.rootPromise = p;
    p.catch(function () { if (__streamOutState.rootPromise === p) __streamOutState.rootPromise = null; });
  }
  return __streamOutState.rootPromise;
}
async function __streamOutPrepare(name) {
  // Each step is named so a failure is rethrown annotated with the
  // operation that raised it (plus the original error's name,
  // message, and errno) — the arming log line in __streamOutBeforeExec
  // then identifies the failing operation exactly.
  var step = "OPFS root (navigator.storage.getDirectory)";
  try {
    var root = await __streamOutRoot();
    __streamOutLog("arming step \"" + step + "\": ok — OPFS root directory handle obtained in the converter worker");
    var prev = __streamOutState.entries[name];
    if (prev && prev.handle) { try { prev.handle.close(); } catch (_) {} }
    step = "getFileHandle(\"" + name + "\", { create: true })";
    var fileHandle = await root.getFileHandle(name, { create: true });
    __streamOutLog("arming step \"" + step + "\": ok — OPFS file handle obtained");
    step = "createSyncAccessHandle() for \"" + name + "\"";
    var handle = await fileHandle.createSyncAccessHandle();
    __streamOutLog("arming step \"" + step + "\": ok — sync access handle open in the converter worker");
    step = "truncate(0) on the sync access handle for \"" + name + "\"";
    // NOT swallowed: if the previous content cannot be cleared, the
    // streamed file could keep a stale tail and corrupt the render —
    // that is an arming failure and refuses the exec like any other.
    handle.truncate(0);
    __streamOutLog("arming step \"" + step + "\": ok — previous content cleared");
    var entry = { name: name, fileHandle: fileHandle, handle: handle, size: 0,
      bytesForwarded: 0, slices: 0, writeCalls: 0, maxSlice: 0,
      readCalls: 0, readBytes: 0, boundary: 0, finalSize: null };
    __streamOutState.entries[name] = entry;
    return entry;
  } catch (e) {
    var err = new Error("arming step \"" + step + "\" failed: " + __streamOutErrText(e));
    err.name = (e && e.name) || "Error";
    if (e && e.errno !== undefined) err.errno = e.errno;
    throw err;
  }
}
function __streamOutBuildFS(FS) {
  var DIR_MODE = 16384 | 511;   // S_IFDIR | 0777
  var FILE_MODE = 32768 | 511;  // S_IFREG | 0777
  function entryOf(node) { return node ? (node.__soEntry || null) : null; }
  function getattr(node) {
    var size = 4096;
    if (!FS.isDir(node.mode)) { var e = entryOf(node); size = e ? e.size : 0; }
    return { dev: 1, ino: node.id, mode: node.mode, nlink: 1, uid: 0, gid: 0, rdev: node.rdev,
      size: size, atime: new Date(node.timestamp), mtime: new Date(node.timestamp),
      ctime: new Date(node.timestamp), blksize: 4096, blocks: Math.ceil(size / 4096) };
  }
  function setattr(node, attr) {
    if (attr.mode !== undefined) node.mode = attr.mode;
    if (attr.timestamp !== undefined) node.timestamp = attr.timestamp;
    if (attr.size !== undefined) {
      var e = entryOf(node);
      if (e) {
        if (e.handle) { try { e.handle.truncate(attr.size); } catch (_) {} }
        e.size = attr.size;
      }
    }
  }
  var dirStreamOps = {
    llseek: function (stream, offset, whence) {
      var position = offset;
      if (whence === 1) position += stream.position;
      else if (whence === 2) position += 4096;
      if (position < 0) throw new FS.ErrnoError(28);
      return position;
    }
  };
  var fileStreamOps = {
    open: function (stream) {
      // (Re)bind the node to the entry prepared for this exec — the
      // node persists across execs while entries are per-render.
      var fresh = __streamOutState.entries[stream.node.name];
      if (fresh) stream.node.__soEntry = fresh;
    },
    close: function (stream) {
      // The sync access handle's lifecycle is owned by
      // __streamOutPrepare / __streamOutAfterExec, not by individual
      // streams: ffmpeg may hold two streams on the same file at once
      // (the mux's write stream and faststart's read-back stream), and
      // both are positional through the one handle.
    },
    read: function (stream, buffer, offset, length, position) {
      var e = entryOf(stream.node);
      if (!e || !e.handle) throw new FS.ErrnoError(5);
      if (position >= e.size) return 0;
      var want = Math.min(length, e.size - position);
      var total = 0;
      while (total < want) {
        var chunkLen = Math.min(__streamOutState.SLICE_BYTES, want - total);
        var tmp = new Uint8Array(chunkLen);
        var n = e.handle.read(tmp, { at: position + total });
        if (n == null || isNaN(n) || n < 0) throw new FS.ErrnoError(5);
        if (n === 0) break;
        buffer.set(tmp.subarray(0, n), offset + total);
        total += n;
        e.readCalls++; e.readBytes += n;
        if (n < chunkLen) break;
      }
      return total;
    },
    write: function (stream, buffer, offset, length, position) {
      var e = entryOf(stream.node);
      if (!e || !e.handle) throw new FS.ErrnoError(5);
      var done = 0;
      while (done < length) {
        var chunkLen = Math.min(__streamOutState.SLICE_BYTES, length - done);
        // Copy this slice out of wasm memory into a fresh buffer, then
        // write it through to OPFS before returning — after this call
        // the converter retains nothing of the output.
        var raw = buffer.subarray ? buffer.subarray(offset + done, offset + done + chunkLen)
                                  : buffer.slice(offset + done, offset + done + chunkLen);
        var src = (typeof Uint8Array !== "undefined" && raw instanceof Uint8Array) ? raw.slice() : Uint8Array.from(raw);
        var w = 0;
        while (w < src.byteLength) {
          var part = (w === 0) ? src : src.subarray(w);
          var n = e.handle.write(part, { at: position + done + w });
          if (n == null || isNaN(n) || n === 0) throw new FS.ErrnoError(5);
          w += n;
        }
        done += chunkLen;
        e.slices++; e.writeCalls++;
        if (chunkLen > e.maxSlice) e.maxSlice = chunkLen;
        e.bytesForwarded += chunkLen;
        var endPos = position + done;
        if (endPos > e.size) e.size = endPos;
        var boundary = Math.floor(e.bytesForwarded / __streamOutState.SLICE_BYTES);
        if (boundary > e.boundary) {
          e.boundary = boundary;
          __streamOutLog(e.name + ": " + e.slices + " slice(s) forwarded · " + e.bytesForwarded + " bytes written to OPFS so far (positional sync-handle slices of at most 16 MB, forwarded as the mux produces them — the output is not accumulating in converter memory)");
        }
      }
      stream.node.timestamp = Date.now();
      return length;
    },
    llseek: function (stream, offset, whence) {
      var e = entryOf(stream.node);
      var position = offset;
      if (whence === 1) position += stream.position;
      else if (whence === 2) position += (e ? e.size : 0);
      if (position < 0) throw new FS.ErrnoError(28);
      return position;
    },
    allocate: function (stream, offset, length) {
      var e = entryOf(stream.node);
      if (!e || !e.handle) throw new FS.ErrnoError(5);
      var endPos = offset + length;
      if (endPos > e.size) { try { e.handle.truncate(endPos); } catch (_) {} e.size = endPos; }
    }
  };
  var fileNodeOps = { getattr: getattr, setattr: setattr };
  var dirNodeOps = {
    getattr: getattr,
    setattr: setattr,
    lookup: function (parent, name) { throw new FS.ErrnoError(44); },
    mknod: function (parent, name, mode, dev) {
      var node = FS.createNode(parent, name, mode, dev);
      if (FS.isDir(mode)) {
        node.node_ops = dirNodeOps; node.stream_ops = dirStreamOps; node.contents = {};
      } else {
        node.node_ops = fileNodeOps; node.stream_ops = fileStreamOps;
        node.__soEntry = __streamOutState.entries[name] || null;
      }
      node.timestamp = Date.now();
      if (parent) { parent.contents[name] = node; parent.timestamp = node.timestamp; }
      return node;
    },
    unlink: function (parent, name) {
      // Drops only the FS node. The OPFS file of the same name is the
      // render result (or a staged leftover) owned by the page's OPFS
      // hygiene and is deliberately NOT removed here.
      delete parent.contents[name];
      parent.timestamp = Date.now();
    },
    rmdir: function (parent, name) { throw new FS.ErrnoError(63); },
    readdir: function (node) {
      var entries = [".", ".."];
      for (var key in node.contents) { if (Object.prototype.hasOwnProperty.call(node.contents, key)) entries.push(key); }
      return entries;
    },
    rename: function (oldNode, newDir, newName) { throw new FS.ErrnoError(63); },
    symlink: function (parent, newname, oldpath) { throw new FS.ErrnoError(63); }
  };
  return {
    mount: function (mount) {
      var rootNode = FS.createNode(null, "/", DIR_MODE, 0);
      rootNode.node_ops = dirNodeOps;
      rootNode.stream_ops = dirStreamOps;
      rootNode.contents = {};
      rootNode.timestamp = Date.now();
      return rootNode;
    },
    node_ops: dirNodeOps,
    stream_ops: fileStreamOps
  };
}
function __streamOutSetup(o) {
  // Every step is journaled with its outcome (and the full error on
  // failure) as it runs — round 2 requirement: a diagnostics dump
  // must identify the failing operation, never a bare "FS error".
  try {
    var FS = o && o.FS;
    if (!FS || typeof FS.mount !== "function" || typeof FS.createNode !== "function") {
      __streamOutLog("setup step \"core FS check\": FAILED — the converter core exposed no usable FS (FS " + (FS ? "present but missing mount/createNode" : "missing") + "); streamed output unavailable");
      return;
    }
    __streamOutState.fs = FS;
    var impl = __streamOutBuildFS(FS);
    __streamOutState.impl = impl;
    __streamOutLog("setup step \"core FS check\": ok — the converter core exposes FS.mount and FS.createNode");
    try {
      FS.filesystems.STREAMOPFS = impl;
      if (FS.filesystems.STREAMOPFS !== impl) throw new Error("FS.filesystems.STREAMOPFS did not retain the registered filesystem object");
      __streamOutLog("setup step \"custom FS registration\": ok — STREAMOPFS registered in FS.filesystems");
    } catch (regErr) {
      __streamOutLog("setup step \"custom FS registration\": FAILED — operation FS.filesystems.STREAMOPFS = impl raised " + __streamOutErrText(regErr) + "; streamed output unavailable");
      return;
    }
    var already = false;
    try { var lp = FS.lookupPath("/out", { follow_mount: false }); already = !!(lp && lp.node && lp.node.mounted); } catch (_) {}
    if (already) {
      __streamOutLog("setup step \"mount STREAMOPFS at /out\": ok — /out was already mounted");
    } else {
      try { FS.mkdir("/out"); __streamOutLog("setup step \"mkdir /out\": ok"); }
      catch (mkErr) { __streamOutLog("setup step \"mkdir /out\": note — operation FS.mkdir(\"/out\") raised " + __streamOutErrText(mkErr) + " (continuing: the directory may already exist)"); }
      try {
        FS.mount(impl, {}, "/out");
        __streamOutLog("setup step \"mount STREAMOPFS at /out\": ok — FS.mount(STREAMOPFS, {}, \"/out\") completed");
      } catch (mountErr) {
        __streamOutState.mounted = false;
        __streamOutLog("setup step \"mount STREAMOPFS at /out\": FAILED — operation FS.mount(STREAMOPFS, {}, \"/out\") raised " + __streamOutErrText(mountErr) + "; streamed output unavailable");
        return;
      }
    }
    __streamOutState.mounted = true;
    try {
      var names = FS.readdir("/out");
      __streamOutLog("setup step \"verify /out directory listing\": ok — FS.readdir(\"/out\") returned " + names.length + " entr" + (names.length === 1 ? "y" : "ies") + " (" + names.join(", ") + ")");
    } catch (listErr) {
      __streamOutState.mounted = false;
      __streamOutLog("setup step \"verify /out directory listing\": FAILED — operation FS.readdir(\"/out\") raised " + __streamOutErrText(listErr) + "; streamed output unavailable");
      return;
    }
    __streamOutLog("streamed-output filesystem mounted at /out in the converter worker (STREAMOPFS): a render addressed to /out/<name> writes through to the OPFS file <name> via a sync access handle in positional slices of at most 16 MB as the bytes are produced — peak converter-memory residency for a render output is the muxer's own buffer, independent of output size");
  } catch (e) {
    __streamOutState.mounted = false;
    __streamOutLog("setup failed (" + __streamOutErrText(e) + ") — streamed output unavailable; renders keep the MEMFS + slice-delivery path for small outputs only");
  }
}
async function __streamOutBeforeExec(o, args) {
  var names = [];
  try {
    if (!__streamOutState.mounted || !args) return null;
    for (var i = 0; i < args.length; i++) {
      var a = args[i];
      if (typeof a === "string" && a.indexOf("/out/") === 0) {
        var nm = a.slice(5);
        if (nm && nm.indexOf("/") === -1 && names.indexOf(nm) === -1) names.push(nm);
      }
    }
  } catch (_) { return null; }
  if (!names.length) return null;
  var prepared = [];
  for (var j = 0; j < names.length; j++) {
    var name = names[j];
    try {
      await __streamOutPrepare(name);
      prepared.push(name);
      __streamOutLog("output streaming armed: /out/" + name + " → OPFS file \"" + name + "\" — sync access handle opened and the file truncated in the converter worker before this exec starts; the render's output will be forwarded to OPFS in positional slices of at most 16 MB as it is produced");
    } catch (prepErr) {
      // Remove a partially-created OPFS file (the file handle may
      // have been created before the sync-handle step failed) so no
      // empty staged output is left behind, then refuse the exec.
      try { var root = await __streamOutRoot(); await root.removeEntry(name); } catch (_) {}
      delete __streamOutState.entries[name];
      __streamOutLog("output streaming could NOT be armed for /out/" + name + " (" + __streamOutErrText(prepErr) + ") — the exec is refused BEFORE the mux starts: a render planned for streamed output never falls back to accumulating its output in converter memory (B13)");
      throw new Error("Streamed saving could not be started for this render (" + ((prepErr && prepErr.message) || prepErr) + "). The render was stopped before encoding so its output could not build up in memory. Close and reopen your browser, then try again.");
    }
  }
  return prepared.length ? { names: prepared } : null;
}
async function __streamOutAfterExec(o, token) {
  if (!token || !token.names) return;
  for (var i = 0; i < token.names.length; i++) {
    var name = token.names[i];
    var e = __streamOutState.entries[name];
    if (!e) continue;
    try {
      if (e.handle) { try { e.handle.flush(); } catch (_) {} try { e.handle.close(); } catch (_) {} e.handle = null; }
    } catch (_) {}
    var finalSize = null;
    try { var f = await e.fileHandle.getFile(); finalSize = f.size; } catch (_) {}
    e.finalSize = finalSize;
    __streamOutLog("output stream closed for " + name + ": " + e.slices + " slice(s) forwarded · " + e.bytesForwarded + " bytes written through the sync handle in " + e.writeCalls + " write call(s) · max slice " + e.maxSlice + " bytes · faststart pass read back " + e.readBytes + " bytes in " + e.readCalls + " read(s) · delivered OPFS file size " + (finalSize == null ? "unknown" : finalSize + " bytes") + " — the output lived in OPFS as it was produced and never resided whole in converter memory");
    delete __streamOutState.entries[name];
  }
}
// =================== end STREAMOUT (B13) ===================

!function(e,t){"object"==typeof exports&&"object"==typeof module?module.exports=t():"function"==typeof define&&define.amd?define([],t):"object"==typeof exports?exports.FFmpegWASM=t():e.FFmpegWASM=t()}(self,(()=>(()=>{"use strict";const e="https://unpkg.com/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.js";var t;!function(e){e.LOAD="LOAD",e.EXEC="EXEC",e.WRITE_FILE="WRITE_FILE",e.READ_FILE="READ_FILE",e.DELETE_FILE="DELETE_FILE",e.RENAME="RENAME",e.CREATE_DIR="CREATE_DIR",e.LIST_DIR="LIST_DIR",e.DELETE_DIR="DELETE_DIR",e.ERROR="ERROR",e.DOWNLOAD="DOWNLOAD",e.PROGRESS="PROGRESS",e.LOG="LOG",e.MOUNT="MOUNT",e.UNMOUNT="UNMOUNT"}(t||(t={}));const r=new Error("unknown message type"),a=new Error("ffmpeg is not loaded, call `await ffmpeg.load()` first"),s=(new Error("called FFmpeg.terminate()"),new Error("failed to import ffmpeg-core.js"));let o;return self.onmessage=async({data:{id:n,type:E,data:c}})=>{const i=[];let p;try{if(E!==t.LOAD&&!o)throw a;switch(E){case t.LOAD:p=await(async({coreURL:r,wasmURL:a,workerURL:n})=>{const E=!o;try{r||(r=e),importScripts(r)}catch{if(r||(r=e.replace("/umd/","/esm/")),self.createFFmpegCore=(await import(r)).default,!self.createFFmpegCore)throw s}const c=r,i=a||r.replace(/.js$/g,".wasm"),p=n||r.replace(/.js$/g,".worker.js");return o=await self.createFFmpegCore({mainScriptUrlOrBlob:`${c}#${btoa(JSON.stringify({wasmURL:i,workerURL:p}))}`}),o.setLogger((e=>self.postMessage({type:t.LOG,data:e}))),o.setProgress((e=>self.postMessage({type:t.PROGRESS,data:e}))),__streamOutSetup(o),E})(c);break;case t.EXEC:p=await(async({args:e,timeout:t=-1})=>{var __so=null;__so=await __streamOutBeforeExec(o,e);o.setTimeout(t),o.exec(...e);const r=o.ret;try{await __streamOutAfterExec(o,__so)}catch(__e){}return o.reset(),r})(c);break;case t.WRITE_FILE:p=(({path:e,data:t})=>(o.FS.writeFile(e,t),!0))(c);break;case t.READ_FILE:p=(({path:e,encoding:t})=>o.FS.readFile(e,{encoding:t}))(c);break;case t.DELETE_FILE:p=(({path:e})=>(o.FS.unlink(e),!0))(c);break;case t.RENAME:p=(({oldPath:e,newPath:t})=>(o.FS.rename(e,t),!0))(c);break;case t.CREATE_DIR:p=(({path:e})=>(o.FS.mkdir(e),!0))(c);break;case t.LIST_DIR:p=(({path:e})=>{const t=o.FS.readdir(e),r=[];for(const a of t){const t=o.FS.stat(`${e}/${a}`),s=o.FS.isDir(t.mode);r.push({name:a,isDir:s})}return r})(c);break;case t.DELETE_DIR:p=(({path:e})=>(o.FS.rmdir(e),!0))(c);break;case t.MOUNT:p=(({fsType:e,options:t,mountPoint:r})=>{const a=e,s=o.FS.filesystems[a];return!!s&&(o.FS.mount(s,t,r),!0)})(c);break;case t.UNMOUNT:p=(({mountPoint:e})=>(o.FS.unmount(e),!0))(c);break;default:throw r}}catch(e){return void self.postMessage({id:n,type:t.ERROR,data:__workerErrText(e)})}p instanceof Uint8Array&&i.push(p.buffer),self.postMessage({id:n,type:E,data:p},i)},{}})()));
//# sourceMappingURL=814.ffmpeg.js.map